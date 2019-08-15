#!/usr/bin/env python

import os

from setuptools import find_packages, setup

install_requires = [
    line.rstrip() for line in open(
        os.path.join(os.path.dirname(__file__), "requirements.txt")
    )
]

scripts = [
            'imaging_db/cli/data_downloader.py',
            'imaging_db/cli/data_uploader.py',
            'imaging_db/cli/query_data.py'
]

setup(name='imagingDB',
      install_requires=install_requires,
      version='0.0.1',
      description='Imaging database and storage',
      url='https://github.com/czbiohub/imagingDB',
      license='MIT',
      packages=find_packages(),
      scripts=scripts,
      zip_safe=False)
