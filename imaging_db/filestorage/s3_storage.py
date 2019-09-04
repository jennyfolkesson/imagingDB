import boto3
import concurrent.futures
import os

import imaging_db.filestorage.data_storage as data_storage
import imaging_db.utils.image_utils as im_utils


class S3Storage(data_storage.DataStorage):
    """Class for handling data uploads and downloads to S3"""

    def __init__(self,
                 storage_dir,
                 nbr_workers=None,
                 bucket_name=None):
        """
        Initialize S3 client and check that ID doesn't exist already

        :param str storage_dir: Directory name in S3 bucket:
            raw_frames or raw_files / dataset ID
        :param int nbr_workers: Number of workers for uploads/downloads
        :param str bucket_name: S3 bucket name. Default: czbiohub-imaging
        """
        super().__init__(storage_dir,
                         nbr_workers)

        if bucket_name is None:
            self.bucket_name = data_storage.S3_BUCKET_NAME
        else:
            self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def assert_unique_id(self):
        """
        Makes sure folder doesn't already exist on S3

        :raise AssertionError: if folder exists
        """
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=self.storage_dir,
        )
        assert response['KeyCount'] == 0, \
            "Key already exists on S3: {}".format(self.storage_dir)

    def nonexistent_storage_path(self, storage_path):
        """
        Checks that a given path to a file in storage doesn't already exist.

        :param str storage_path: Path in S3 storage
        :return bool: True if file doesn't exist in storage, False otherwise
        """
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=storage_path,
        )
        if response['KeyCount'] == 0:
            return True
        else:
            return False

    def upload_frames(self, file_names, im_stack, file_format=".png"):
        """
        Upload all frames to S3 using threading

        :param list of str file_names: image file names
        :param np.array im_stack: all 2D frames from file converted to stack
        :param str file_format: file format for frames on S3
        """
        # Make sure number of file names matches stack shape
        assert len(file_names) == im_stack.shape[-1], \
            "Number of file names {} doesn't match slices {}".format(
                len(file_names), im_stack.shape[-1])

        serialized_ims = []
        keys = []
        for i, file_name in enumerate(file_names):
            # Create key
            key = "/".join([self.storage_dir, file_name])
            # Make sure image doesn't already exist
            if self.nonexistent_storage_path(storage_path=key):
                # Serialize image
                im_bytes = im_utils.serialize_im(
                    im=im_stack[..., i],
                    file_format=file_format,
                )
                serialized_ims.append(im_bytes)
                keys.append(key)
            else:
                print("Key {} already exists, next.".format(key))

        with concurrent.futures.ThreadPoolExecutor() as ex:
            {ex.submit(self.upload_serialized, key_byte_tuple):
                key_byte_tuple for key_byte_tuple in zip(keys, serialized_ims)}

    def upload_serialized(self, key_byte_tuple):
        """
        Upload serialized image. The tuple is to simplify threading executor
        submission.

        :param tuple key_byte_tuple: Containing key and byte string
        """
        (key, im_bytes) = key_byte_tuple
        # Create new client
        s3_client = boto3.client('s3')
        # Upload slice to S3
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=im_bytes,
        )

    def upload_im(self, im_name, im, file_format='.png'):
        """
        Serialize then upload image to S3 storage after checking that key
        doesn't already exist.

        :param str im_name: File name for image, with extension, no path
        :param np.array im: 2D image
        :param str file_format: File format for serialization
        """
        key = "/".join([self.storage_dir, im_name])
        # Create new client
        s3_client = boto3.client('s3')
        # Make sure image doesn't already exist
        if self.nonexistent_storage_path(storage_path=key):
            im_bytes = im_utils.serialize_im(im, file_format)
            # Upload slice to S3
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=im_bytes,
            )
        else:
            print("Key {} already exists.".format(key))

    def upload_file(self, file_path):
        """
        Upload a single file to S3 without reading its contents

        :param str file_path: Full path to file to be uploaded
        """
        # ID should be unique, make sure it doesn't already exist
        self.assert_unique_id()

        file_name = file_path.split("/")[-1]
        key = "/".join([self.storage_dir, file_name])
        self.s3_client.upload_file(
            file_path,
            self.bucket_name,
            key,
        )

    def get_im(self, file_name):
        """
        Given file name, fetch 2D image (frame)

        :param str file_name: File name of image, with extension, no path
        :return np.array im: 2D image
        """
        s3_client = boto3.client('s3')
        key = "/".join([self.storage_dir, file_name])
        byte_str = s3_client.get_object(
            Bucket=self.bucket_name,
            Key=key,
        )['Body'].read()
        # Construct an array from the bytes and decode image
        return im_utils.deserialize_im(byte_str)

    def download_file(self, file_name, dest_dir):
        """
        Download a single file from S3 without reading its contents.
        A new client is created to make boto3 thread safe
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/\
        resources.html#multithreading-multiprocessing

        :param str file_name: File name
        :param str dest_dir: Destination directory name
        """
        # Create a new client
        s3_client = boto3.client('s3')
        key = "/".join([self.storage_dir, file_name])
        dest_path = os.path.join(dest_dir, file_name)
        s3_client.download_file(
            self.bucket_name,
            key,
            dest_path,
        )
