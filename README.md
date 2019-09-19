[![Build Status](https://travis-ci.com/czbiohub/imagingDB.svg?branch=master)](https://travis-ci.com/czbiohub/imagingDB)
[![Code Coverage](https://codecov.io/gh/czbiohub/imagingDB/branch/master/graphs/badge.svg)](https://codecov.io/gh/czbiohub/imagingDB)

# Imaging Database

This is a data management system intended for microscopy images. It consists of two components:
* **File storage**: Image data is stored both in an AWS S3 bucket named 'czbiohub-imaging', as
well as local storage at the Biohub 
(an [IBM ESS](https://www.ibm.com/us-en/marketplace/ibm-elastic-storage-server)) which is assumed to be mounted on the machine 
you're running imagingDB on. The idea is to have fast and cheap access to data with the local storage,
while still having offsite backup taken care of automatically. The S3 portion of the storage
could also be used to access your data when not at/vpn connected to the Biohub or to share data
with collaborators.
The data is synced between local storage and S3 using [AWS DataSync](https://aws.amazon.com/datasync/) on a daily basis.
If repurposing this repo, make sure to change storage access points in the DataStorage class.
* **A database**. We're using an [AWS PostgreSQL RDS](https://aws.amazon.com/rds/postgresql/) 
database, and SQLAchemy for interacting with the database using Python. 

![imagingDB layout](imagingDB_overview.png?raw=true "Title")

A dataset can consist of up to five dimensional image data: x, y, z, channels, timepoints,
and positions (FOVs), as well as associated metadata (global for the entire dataset and local
for each 2D frame).
Each dataset is assumed to have an unique identifier in the form

\<ID>-YYYY-MM-DD-HH-MM-SS-\<XXXX>

where ID is a project ID (e.g. 'ISP' or 'ML'), followed by a timestamp, and the last
section is a four digit serial number.

Below is a visualization of the database schema, generated using 
[eralchemy](https://github.com/Alexis-benoist/eralchemy). 
The arch on the top of the data_set table should connect parent_id with id, and it also doesn't 
show foreign keys (dataset_id to id) clearly, other than that it gives an overview of schema. 

![Database schema](db_schema.png?raw=true "Title")

### data_set
This is the main table in which every dataset gets an entry. It includes the dataset identifier
described above, and you can include strings describing your dataset and the microscope you
used if you so like.

You can choose if you'd like to upload your dataset as an uninspected file, or to split it into
frames. Each frame is a 2D image encoding the x, y information for each of the other indices 
slices (z), channels, timepoints and positions (FOVs). This seemed like the most obvious partition
into smaller files, and the file storage classes allow you to assemble your frames into
up to 5D numpy arrays with the indices you're interested in working with.
Another option that would be interesting in exploring is the
[Zarr](https://zarr.readthedocs.io/en/stable/) storage format and let the user define chunk
sizes, however that's future work. Right now 2D frames are stored in the PNG format since
that provides good lossless compression.

The timestamp part if the dataset identifier gets stored as a separate datetime column, which
allows for quick queries if you're searching for datasets acquired within a certain time window.

If you have datasets that are related to each other, you also have the option to link them using
the parent_id column. An example use case is if you have uploaded an imaging dataset, and you have acquired some
other type of measurements of the same dataset that is stored in a different type of file
(see the bottom of [this notebook](https://github.com/czbiohub/imagingDB/blob/master/notebooks/jsonb_queries.ipynb)
for an example).

### file_global
If a dataset consists of one file, this will be uploaded to storage as is. The storage directory
will be raw_frames/dataset_serial/ and the file will keep its original file name.
A SHA-256 hash is computed during upload and is stored to check for future file corruption.

### frames_global
If choosing to split your dataset into frames, this table will provide you with a global
summary of your dataset such as total number of frames, frame shape and type, as well as
how many (z, channel, time, position) indices there are, directory location of image data in storage.
as well as other metadata found in the header stored in a queryable JSONB column.

### frames
This table has a many to one mapping with frames_global, and it encodes indices and
metadata for individual frames. A SHA-256 hash is also computed during upload and stored
in a column here to ensure that potential data corruption does not go undetected.

## Getting Started

There are three main CLIs, data_uploader, data_downloader and query_data. 

During data upload, you can choose between doing 'file' or 'frames' upload type.
File will just write the existing file as is on S3, whereas 'frames' will
attempt to read the file, separate frames from metadata and upload each
frame individually as a png, and write both global and frame level metadata to 
the database. Files will be written to the 'raw_files' folder
in the storage, and frames will be written to 'raw_frames'.

The data downloader allows you to download complete datasets, or partial datasets if you
specify x, y, z, t, p indices.

The query data tool interfaces the database only, it queries the database and prints out 
datasets that matches specified search criteria.

In addition to the CLIs, you can see examples on how to programatically access your
data in the [Jupyter notebooks](https://github.com/czbiohub/imagingDB/tree/master/notebooks),
e.g. how to query data and assemble full or partial imaging datasets directly in Python.

The data uploader CLI takes the following arguments:

 * **csv:** a csv file containing information for each dataset (one per row), please see the
 [_files_for_upload.csv_](https://github.com/czbiohub/imagingDB/blob/master/files_for_upload.csv) 
 in this repository for required fields and example values.
 The csv file should contain the following columns:
    * _dataset_id:_ The unique dataset identifier (required)
    * _file_name:_ Full path to the file/directory to upload (required)
    * _description:_ Description of the dataset (string, optional)
    * _parent_dataset_id:_ If the dataset is related to another dataset in the database,
    you can specify the unique parent dataset ID and that will create an internal reference
    in the data_set table (unfortunately not very clearly depicted in the database schema). (optional)
    * _positions:_ [list of ints] Positions in file directory that belong to the same
    dataset ID (optional, for ome_tiff uploads only).
* **login:** a json file containing database login credentials (see db_credentials.json)
    * _drivername_
    * _username_
    * _password_
    * _host_
    * _port_
    * _dbname_
* **config:** a json config file containing upload settings (see example in config.json)
    * _upload_type:_ 'frames' if you want to split your dataset into individual 2D frames, or 'file'
    if you want to upload a file uninspected. (required)
    * _frames_format:_ If uploading frames, specify what upload type. For a deep dive
     into how the splitting into metadata and frames work, please see documentation directly
     in the [splitter classes](https://github.com/czbiohub/imagingDB/tree/master/imaging_db/images).
     As well as in their corresponding test files in the tests/ directory and the
     [data_uploader_tests](https://github.com/czbiohub/imagingDB/blob/master/tests/cli/data_uploader_tests.py).
     Options are: 
        * 'ome_tiff': Assumes either a file name or a directory containing one or several 
        [ome-tiff](https://docs.openmicroscopy.org/ome-model/5.6.3/ome-tiff/) files (one for each position).
         Needs MicroManagerMetadata tag for each frame for metadata, see
        [config_ome_tiff.json](https://github.com/czbiohub/imagingDB/blob/master/config_ome_tiff.json)
        for example. For this format you can specify a JSON schema which will tell imagingDB
        which metadata fields to parse (see example in meta_schema below)
        * 'tif_folder': Assumes a directory where each frame is already stored as an individual
        tiff file.
        These get read, get their (MicroManager) metadata extracted and saved in storage.
        This option currently assumes that there is also a metadata.txt file in the directory,
        see [description](https://github.com/czbiohub/imagingDB/blob/master/imaging_db/images/tiffolder_splitter.py#L106)
        See example config [config_tiffolder.json](https://github.com/czbiohub/imagingDB/blob/master/config_tiffolder.json).
        * 'tif_id': Assumes a tiff file with limited metadata (not necessarily MicroManager). 
        Needs ImageDescription tag in first frame page for metadata. 
        See example config in [config_tif_id.json](https://github.com/czbiohub/imagingDB/blob/master/config_tif_id.json).
    * _microscope:_ Which microscope was used for image acquisition (optional, string)
    * _filename_parser:_ If there's metadata information embedded in file name,
    specify which function that can parse file names for you.
    Current options are: 'parse_ml_name' and 'parse_sms_name' (optional)
    They're taylored to specific group that use different naming conventions, see more
    detail in [filename_parsers.py](https://github.com/czbiohub/imagingDB/blob/master/imaging_db/images/filename_parsers.py).
    * _meta_schema:_ If doing a ome_tiff opload, you can specify a metadata 
    schema with required fields. See example in 
    [metadata_schema.json](https://github.com/czbiohub/imagingDB/blob/master/metadata_schema.json)
    (optional for ome_tiff uploads).
* **storage:** 'local' (default) or 's3'. Uploads to local storage will be 
synced to S3 daily. (optional)
* **storage_access:** If using a different storage than defaults, specify here.
Defaults are /Volumes/data_lg/czbiohub-imaging (mount point)
for local storage, and czbiohub-imaging (bucket name) for S3 storage. (optional)
* **nbr_workers:** Number of threads used for image uploads
(default = nbr of processors on machine * 5). (optional)

```buildoutcfg
python imaging_db/cli/data_uploader.py --csv files_for_upload.csv --login db_credentials.json --config config.json
```

The data_downloader CLI takes the following command line inputs: 

* **login:** A JSON file with DB login credentials
* **dest:** A destination directory where the data will be downloaded
* **storage:**  Which storage to get data from: 'local' (default) or 's3'. (optional)
* **storage_access:** If using a different storage access point than defaults, specify here.
Defaults are /Volumes/data_lg/czbiohub-imaging (mount point)
for local storage, and czbiohub-imaging (bucket name) for S3 storage. (optional)
* **id:** a unique dataset identifier
* **metadata:** For files split to frames only. Writes global metadata in json, and
 local metadata for each frame in a csv in the destination directory. (default True)
* **no-metadata:** Downloads image data only.
* **no-download:** Downloads metadata only.
* **download:** For Frames, there's the option of only retrieving the metadata files. (default True)
* **c, channels:** [tuple] Download only specified channel names/indices if tuple contains
 strings/integers (optional)
* **z, slices:** [tuple] Download only specified slice indices (optional)
* **t, times:** [tuple] Download only specified time indices (optional)
* **p, positions:** [tuple] Download only specified position indices (optional)
* **nbr_workers:** [tuple] Number of threads used for data downloads (default = nbr of cpus on machine * 5).

```buildoutcfg
python imaging_db/cli/data_downloader.py --id ID-2018-04-05-00-00-00-0001 --dest /My/local/folder --login db_credentials.json
```

The query_data CLI takes the following command line inputs: 

* **login:** a JSON file containing database login credentials
* **project_id:** First part of dataset_serial containing project ID (e.g. 'ML'). (optional)
* **microscope:** Any string subset from microscope column in data_set table. (optional)
* **start_date:** Format YYYY-MM-DD. Find >= dates in date_time column of data_set table. (optional)
* **end_date:** Format YYYY-MM-DD. Find <= dates in date_time column of data_set table. (optional)
* **description:** Find substring in description column of data_set table. (optional)
```buildoutcfg
python imaging_db/cli/query_data.py --login db_credentials.json --project_id <str> --start_date <start date> --end_date <end date>
```

### Prerequisites

Python requirements can be found in the file **requirements.txt** if you want to do a pip install.
There's also a **conda_environment.yml** file if you'd prefer to create a conda environment.

####  Data Storage Locations
* Local storage IBM ESS: You will have access to local storage if you're at the Biohub or
connected via VPN. imagingDB assumes that you have the data_lg/ instance mounted at 
/Volumes/data_lg/czbiohub-imaging. If mounted elsewhere, you can specify mount point with the 
storage_access parameter.
* The AWS S3 bucket czbiohub-imaging: You will need to have a Biohub AWS account and to configure your AWS CLI with your access 
key ID and secret key using the command
```
aws configure
```
#### Database Location
The database lives in an AWS PostgreSQL RDS. You will need to be added as a user there too, 
and add your username, password and the host in a json file for database access (see db_credentials.json)

Please contact Jenny or Kevin via Slack or email if you want to be added as a user.

## Running imagingDB on a server

The recommended use is running imagingDB inside a docker container. 
Build the supplied Dockerfile.imagingDB, e.g.:
```buildoutcfg
docker build -t imaging_db:python37 -f Dockerfile.imagingDB .
```
Then you will have all the requirements necessary installed. Additionally,
you will need you AWS keys inside your Docker container. If you've stored your
key and secret key in ~/.aws/credentials you can get them as environment variable by running:
```buildoutcfg
AWS_ACCESS_KEY_ID=$(aws --profile default configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws --profile default configure get aws_secret_access_key)
```
Then you can set these variables when you start your Docker container:
```buildoutcfg
docker run -it -p <your port>:8888 -v <your data dir>:/data -v <your repo>:/imagingDB -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY imaging_db:python36  bash 
```
Note: exposing your environment variables like this is not the safest way to 
set up AWS credentials. Future work will include a safer way to do this.

If you want to launch a Jupyter notebook inside your container, you can do so with the following command:
```buildoutcfg
jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --no-browser
```
Then you can access your notebooks in your browser using your server name (e.g. if you're on the biohub internal server named fry):
```buildoutcfg
http://fry:<whatever port you mapped to when starting up docker container>
```
You will need to copy/paste the token generated in your Docker container. 

## Running the tests

Unittests are located in the tests/ directory. To be able to run the database tests, you first need to start the test and dev postgres Docker containers,
which you can do with the command:
```buildoutcfg
make start-local-db
``` 
Then you can run all tests:
```buildoutcfg
nosetests tests/
```
The test and dev databases are mapped to ports 5433 and 5432 respectively, with host localhost and username
'username' and password 'password'.
To stop the Docker containers, run
```buildoutcfg
make stop-local-db
```

## Built With

* [SQLAlchemy](https://www.sqlalchemy.org/) - Python SQL Toolkit and Object Relational Mapper
* [tifffile](https://pypi.org/project/tifffile/) - Read image and metadata from TIFF-like files used in bioimaging
* [Boto 3](https://boto3.readthedocs.io/en/latest/) - AWS SDK for Python

## Authors

* **Jenny Folkesson** - *Original author* - jenny.folkesson@czbiohub.org [GitHub](https://github.com/jennyfolkesson)
* **Kevin Yamauchi** - *Contributor* - kevin.yamauchi@czbiohub.org [GitHub](https://github.com/kevinyamauchi)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
