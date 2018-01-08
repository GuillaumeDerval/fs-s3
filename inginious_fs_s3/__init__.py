# coding=utf-8
import os
from datetime import datetime
from io import BytesIO

import shutil

import boto3
import zipstream
from botocore.exceptions import ClientError
from cachetools import LRUCache, Cache

from inginious.common.filesystems.provider import FileSystemProvider, NotFoundException

class S3CacheManager(object):
    class CustomLRUCache(LRUCache):
        def __init__(self, maxsize):
            super().__init__(maxsize, getsizeof=self._computeFileSize)

        def __getitem__(self, path, cache_getitem=Cache.__getitem__):
            val = super(S3CacheManager.CustomLRUCache, self).__getitem__(path, cache_getitem)

            # also "up" parent folders
            cur_path = path
            if cur_path.endswith("/"):
                cur_path = cur_path[:-1]
            while cur_path != "":
                cur_path = os.path.split(cur_path)[0]
                if cur_path + "/" in self:
                    self._LRUCache__update(cur_path + "/")

            return val

        def __setitem__(self, path, key, cache_setitem=Cache.__setitem__):
            val = super(S3CacheManager.CustomLRUCache, self).__setitem__(path, key, cache_setitem)

            # also "up" parent folders
            cur_path = path
            if cur_path.endswith("/"):
                cur_path = cur_path[:-1]
            while cur_path != "":
                cur_path = os.path.split(cur_path)[0]
                if cur_path + "/" in self:
                    self._LRUCache__update(cur_path + "/")

            return val

        def __delitem__(self, path, cache_delitem=Cache.__delitem__):
            (timestamp, disk_path, weight) = self[path]
            print("DELETING " + path)
            if not path.endswith("/") or len(os.listdir(disk_path)) == 0:  # if it's a file, or an empty dir
                os.unlink(disk_path)

            # also pop parent folders
            if path.endswith("/"):
                path = path[:-1]
            sub = os.path.split(path)[0]
            if sub != "" and sub + "/" in self:
                del self[sub + "/"]

            # call super
            return super(S3CacheManager.CustomLRUCache, self).__delitem__(path, cache_delitem)

        def _computeFileSize(self, v):
            return v[2]

    def __init__(self, cache_path, max_size):
        """
        Init the cache
        :param cache_path: path on the disk to where to store the cached files 
        :param max_size: maximum size of the cache in bytes. Not that the cache may consume a bit more while downloading files.
        """
        self._cache_path = cache_path
        self._cache_data = S3CacheManager.CustomLRUCache(max_size)
        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)
        os.makedirs(cache_path)

    def get(self, path, cur_timestamp):
        if path in self._cache_data:
            entry = self._cache_data[path]
            if entry[0] >= cur_timestamp:
                return entry[1]
        return None

    def put_file(self, path, cur_timestamp, content):
        disk_path = os.path.join(self._cache_path, path)
        os.makedirs(os.path.split(disk_path)[0], exist_ok=True)
        open(disk_path, 'wb').write(content)
        self._cache_data[path] = (cur_timestamp, disk_path, len(content))

    def put_folder(self, path, cur_timestamp):
        """ THE ASSOCIATED FILES MUST HAVE BEEN ADDED VIA put_file BEFORE. """
        disk_path = os.path.join(self._cache_path, path)
        os.makedirs(os.path.split(disk_path)[0], exist_ok=True)
        self._cache_data[path] = (cur_timestamp, disk_path, 0)

    def invalidate(self, path):
        if path in self._cache_data:
            del self._cache_data[path]  # this will invalidate parents
        else:
            # invalide parent folders
            if path.endswith("/"):
                path = path[:-1]
            sub = os.path.split(path)[0]
            if sub != "":
                self.invalidate(sub + "/")


class S3FSProvider(FileSystemProvider):
    @classmethod
    def get_needed_args(cls):
        """ Returns a list of arguments needed to create a FileSystemProvider. In the form 
            {
                "arg1": (int, False, "description1"),
                "arg2: (str, True, "description2")
            }

            The first part of the tuple is the type, the second indicates if the arg is mandatory
            Only int and str are supported as types.
        """
        return {
            "bucket": (str, True, "S3 bucket to use"),
            "prefix": (str, True, "prefix to be added to files in the bucket"),
            "cachedir": (str, True, "On-disk path to a directory where the cache will be stored"),
            "cachesize": (int, True, "Size of the cache, in Mo."),
            "access_key_id": (str, False, "Access key id"),
            "secret_access_key": (str, False, "Secret access key")
        }

    @classmethod
    def init_from_args(cls, bucket, prefix, cachedir, cachesize, access_key_id, secret_access_key):
        """ Given the args from get_needed_args, creates the FileSystemProvider """
        return S3FSProvider(prefix, boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key), boto3.resource('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key).Bucket(bucket), S3CacheManager(cachedir, cachesize * (10 ** 6)))

    def __init__(self, prefix, s3client, s3bucket, cache_manager: S3CacheManager):
        super().__init__(prefix)
        if not self.prefix.endswith("/"):
            self.prefix = self.prefix + "/"
        if self.prefix == "/":  # no initial /
            self.prefix = ""
        self._client = s3client
        self._bucket = s3bucket
        self._cache = cache_manager

    def from_subfolder(self, subfolder):
        self._checkpath(subfolder)
        return S3FSProvider(self.prefix + subfolder, self._client, self._bucket, self._cache)

    def exists(self, path=None):
        if path is None:
            path = self.prefix
        else:
            path = self.prefix + path

        try:
            self._bucket.Object(path).load()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False

        return True

    def ensure_exists(self):
        if not self.exists():
            self._bucket.put_object(Key=self.prefix)

    def _put_file(self, fullpath, fileobj):
        try:
            self._bucket.upload_fileobj(fileobj, fullpath)
            # update folders timestamps
            parts = fullpath.split("/")[:-1]
            if len(parts) > 0:
                parts[0] = parts[0] + "/"
                for i in range(1, len(parts)):
                    parts[i] = parts[i - 1] + parts[i] + "/"
                for i in range(0, len(parts)):
                    self._bucket.put_object(Key=parts[len(parts) - 1 - i])
        except ClientError:
            raise NotFoundException()

    def put(self, filepath, content):
        self._checkpath(filepath)
        fullpath = self.prefix + filepath

        # ensure fullpath does not end with a /:
        if fullpath.endswith("/"):
            raise NotFoundException()

        if isinstance(content, str):
            content = content.encode("utf-8")

        self._put_file(fullpath, BytesIO(content))

    def get_fd(self, filepath, timestamp=None):
        self._checkpath(filepath)

        fullpath = self.prefix + filepath

        # ensure fullpath does not end with a /:
        if fullpath.endswith("/"):
            raise NotFoundException()

        try:
            obj = self._bucket.Object(fullpath)
            needed_time = timestamp if timestamp is not None else obj.last_modified
            local_path = self._cache.get(fullpath, needed_time)
            if local_path is None:
                print("DOWNLOAD " + fullpath + " FROM S3")
                fd = obj.get()["Body"]
                self._cache.put_file(fullpath, obj.last_modified, fd.read())
                return open(self._cache.get(fullpath, needed_time), 'rb')
            else:
                return open(local_path, 'rb')
        except ClientError:
            raise NotFoundException()

    def get(self, filepath, timestamp=None):
        return self.get_fd(filepath, timestamp).read()

    def _compute_relpath(self, path, base):
        out = os.path.relpath(path, base)
        if path.endswith("/") and not out.endswith("/") and out != "":
            out += "/"
        return out

    def list(self, folders=True, files=True, recursive=False):
        if recursive:
            objects = self._bucket.objects.filter(Prefix=self.prefix, Marker=self.prefix).all()
            objects = [self._compute_relpath(f.key, self.prefix) for f in objects]
        else:
            objects = self._client.list_objects_v2(Bucket=self._bucket.name, Prefix=self.prefix, Delimiter="/").get("CommonPrefixes", [])
            objects = [self._compute_relpath(f["Prefix"], self.prefix) for f in objects]

        out = []
        for f in objects:
            if f.endswith("/") and folders:
                out.append(f)
            if not f.endswith("/") and files:
                out.append(f)
        return out

    def delete(self, filepath=None):
        if filepath is None:
            filepath = ""
        self._checkpath(filepath)

        fullpath = self.prefix + filepath

        if fullpath.endswith("/"):  # folder delete
            subfolder = self.from_subfolder(filepath)
            list = subfolder.list(False, True, True)
            for f in list:
                subfolder.delete(f)

        try:
            self._cache.invalidate(fullpath)
            self._bucket.Object(fullpath).delete()
        except ClientError:
            raise NotFoundException()

    def get_last_modification_time(self, filepath):
        self._checkpath(filepath)

        fullpath = self.prefix + filepath

        # ensure fullpath does not end with a /:
        if fullpath.endswith("/"):
            raise NotFoundException()

        try:
            return self._bucket.Object(fullpath).last_modified
        except ClientError:
            raise NotFoundException()

    def move(self, src, dest):
        self._checkpath(src)
        self._checkpath(dest)

        fullsrc = self.prefix + src
        fulldest = self.prefix + dest

        if fullsrc.endswith("/") or fullsrc == "":  # folder
            if not fulldest.endswith("/"):
                fulldest += "/"

            src_folder = self.from_subfolder(src)
            dest_folder = self.from_subfolder(dest)
            files = self.from_subfolder(src).list(True, True, True)
            for f in files:
                if not f.endswith("/"):  # file
                    dest_folder.put(f, src_folder.get(f))
            src_folder.delete()
        else:
            self.put(dest, self.get(src))
            self.delete(src)

    def copy_to(self, src_disk, dest=None):
        if dest is None:
            dest = ""  # prefix

        if os.path.isdir(src_disk):
            if dest != "" and not dest.endswith("/"):  # dest should be a dir
                dest += "/"
            files = os.listdir(src_disk)
            for f in files:
                self.copy_to(os.path.join(src_disk, f), dest + f)
        else:
            if dest == "" or dest.endswith("/"):  # dest should not be a dir
                raise NotFoundException()
            self._put_file(self.prefix + dest, open(src_disk, 'rb'))

    def copy_from(self, src, dest_disk):
        if src is None:
            src = ""
        self._checkpath(src)
        fullpath = self.prefix + src

        # not a directory
        if fullpath != "" and not fullpath.endswith("/"):
            open(dest_disk, 'wb').write(self.get(src))
            return

        # Goal is to get a full folder from the cache
        # let's first check the timestamp of the folder
        last_modified = datetime.now()
        try:
            self._bucket.Object(fullpath).last_modified
        except:
            pass

        folder = self._cache.get(fullpath, last_modified)
        start_timestamp = datetime.now()
        if folder is not None:  # we have a full folder to directly copy! woohoo!
            self._recursive_overwrite(folder, dest_disk)
        else:  # if that's not the case, we cannot take the assumption that the full directory can sit in the cache; let's download and copy all
            # items, one after the other.
            # But let's try to maintain the directory structure in the cache, to attempt to have the full folder next time!
            objects = self._bucket.objects.filter(Prefix=fullpath, Marker=fullpath).all()
            objects = [(self._compute_relpath(f.key, fullpath), f.last_modified) for f in objects if not f.key.endswith("/")]
            all_folders = set()
            all_folders.add("")
            for f, tmsp in objects:
                dest_file_path = os.path.join(dest_disk, f)
                os.makedirs(os.path.split(dest_file_path)[0], exist_ok=True)
                open(dest_file_path, 'wb').write(self.get(src + f, tmsp))

                while f != "":
                    f = os.path.split(f)[0]
                    all_folders.add(f)

            # update folders in cache
            for f in all_folders:
                if f != "":
                    f += "/"
                self._cache.put_folder(fullpath + f, start_timestamp)

    def _recursive_overwrite(self, src, dest):
        if os.path.isdir(src):
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(src)
            for f in files:
                self._recursive_overwrite(os.path.join(src, f),
                                          os.path.join(dest, f))
        else:
            shutil.copyfile(src, dest)

    def distribute(self, filepath, allow_folders=True):
        self._checkpath(filepath)
        fullpath = self.prefix + filepath
        if fullpath.endswith("/"):
            if not allow_folders:
                return ("invalid", None, None)
            zip = zipstream.ZipFile()
            subfolder = self.from_subfolder(filepath)
            filelist = subfolder.list(False, True, True)
            for f in filelist:
                def generator():
                    complete_filepath = fullpath + f
                    yield self._bucket.Object(complete_filepath).get()["Body"].read()

                zip.write_iter(f, generator())
            return ("local", "application/zip", zip.__iter__())
        else:
            url = self._client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self._bucket.name,
                    'Key': fullpath
                }
            )
            return ("url", None, url)
