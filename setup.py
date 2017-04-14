#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

install_requires = [
    "zipstream >= 1.1.4",
    "cachetools >= 2.0.0",
    "boto3 >= 1.4.2"
]

# Setup
setup(
    name="inginious_fs_s3",
    version="0.1",
    description="S3 FileSystem Provider",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={'inginious.filesystems': ['s3 = inginious_fs_s3:S3FSProvider']},
    include_package_data=True,
    author="Guillaume Derval",
    author_email="guillaume.derval@uclouvain.be",
    license="Proprietary",
    url="https://github.com/UCL-INGI/INGInious"
)
