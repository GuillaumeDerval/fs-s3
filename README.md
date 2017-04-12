# fs-s3
Filesystem based on S3-compatible APIs

Config:
```
fs:
    module: inginious_fs_s3.S3FSProvider
    cachedir: s3cache
    bucket: inginious-test
    prefix: tasks/
    cachesize: 100
```
