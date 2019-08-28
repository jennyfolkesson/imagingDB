import boto3
import concurrent.futures
import numpy as np
import os

import imaging_db.filestorage.data_storage as data_storage
import imaging_db.utils.image_utils as im_utils

S3_BUCKET_NAME = "czbiohub-imaging"


class S3Storage(data_storage.DataStorage):
    """Class for handling data uploads and downloads to S3"""

    def __init__(self, dir_name, nbr_workers=None):
        """
        Initialize S3 client and check that ID doesn't exist already

        :param str dir_name: Directory name in S3 bucket
        :param int nbr_workers: Number of workers for uploads/downloads
        """
        super().__init__(dir_name,
                         nbr_workers)

        self.bucket_name = S3_BUCKET_NAME
        self.s3_client = boto3.client('s3')
        self.dir_name = dir_name
        self.nbr_workers = nbr_workers

    def assert_unique_id(self):
        """
        Makes sure folder doesn't already exist on S3

        :raise AssertionError: if folder exists
        """
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name,
                                                  Prefix=self.dir_name)
        assert response['KeyCount'] == 0, \
            "Key already exists on S3: {}".format(self.dir_name)

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
            key = "/".join([self.dir_name, file_name])
            # Make sure image doesn't already exist
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=key,
            )
            if response['KeyCount'] == 0:
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

    def upload_im(self, file_name, im, file_format='.png'):
        """
        Serialize then upload image to S3 storage after checking that key
        doesn't already exist.

        :param str file_name: File name for image
        :param np.array im: 2D image
        :param str file_format: File format for serialization
        """
        key = "/".join([self.dir_name, file_name])
        # Create new client
        s3_client = boto3.client('s3')
        # Make sure image doesn't already exist
        response = s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=key,
        )
        if response['KeyCount'] == 0:
            im_bytes = im_utils.serialize_im(im, file_format)
            # Upload slice to S3
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=im_bytes,
            )
        else:
            print("Key {} already exists, next".format(key))

    def upload_file(self, file_name):
        """
        Upload a single file to S3 without reading its contents

        :param str file_name: full path to file
        """
        # ID should be unique, make sure it doesn't already exist
        self.assert_unique_id()

        file_no_path = file_name.split("/")[-1]
        key = "/".join([self.dir_name, file_no_path])
        self.s3_client.upload_file(
            file_name,
            self.bucket_name,
            key,
        )

    def get_im(self, file_name):
        """
        Given file name, fetch 2D image (frame)

        :param str file_name: slice file name
        :return np.array im: 2D image
        """
        s3_client = boto3.client('s3')
        key = "/".join([self.dir_name, file_name])
        byte_str = s3_client.get_object(
            Bucket=self.bucket_name,
            Key=key,
        )['Body'].read()
        # Construct an array from the bytes and decode image
        return im_utils.deserialize_im(byte_str)

    def get_stack(self, file_names, stack_shape, bit_depth):
        """
        Given file names, fetch images and return image stack.
        This function assumes that the frames in the list are contiguous,
        i.e. the length of the file name is will be the last dimension of
        the image stack.

        :param list of str file_names: Frame file names
        :param tuple stack_shape: Shape of image stack
        :param dtype bit_depth: Bit depth
        :return np.array im_stack: Stack of 2D images
        """
        im_stack = np.zeros(stack_shape, dtype=bit_depth)

        with concurrent.futures.ThreadPoolExecutor(self.nbr_workers) as executor:
            future_result = executor.map(self.get_im, file_names)

        for im_nbr, im in enumerate(future_result):
            im_stack[..., im_nbr] = np.atleast_3d(im)
        return im_stack

    def get_stack_from_meta(self, global_meta, frames_meta):
        """
        Given global metadata, instantiate an image stack. The default order
        of frames is:
        X Y [gray/RGB] Z C T P
        Image height, width, colors (1 or 3), the z depth, channels,
        timepoints, positions

        Retrieve all frames from local metadata and return image stack.
        Ones in stack shape indicates singleton dimensions.
        The stack is then squeezed to remove singleton dimensions, and a string
        is returned to indicate which dimensions are kept and in what order.
        TODO: Add option to customize image order

        :param dict global_meta: Global metadata for dataset
        :param dataframe frames_meta: Local metadata and paths for each file
        :return np.array im_stack: Stack of 2D images
        :return str dim_str: String indicating order of stack dimensions
            Possible values: XYGZCTP
            X=im_height, Y=im_width, G=[gray/RGB] (1 or 3),
            Z=slice_idx, C=channel_idx, T=time_idx, P=pos_idx
        """
        im_stack, unique_ids = self.make_stack_from_meta(global_meta, frames_meta)

        with concurrent.futures.ThreadPoolExecutor(self.nbr_workers) as executor:
            future_result = executor.map(self.get_im, frames_meta['file_name'])

        im_list = list(future_result)
        # Fill the image stack given dimensions
        for im_nbr, row in frames_meta.iterrows():
            im_stack[:, :, :,
                     np.where(unique_ids['slices'] == row.slice_idx)[0][0],
                     np.where(unique_ids['channels'] == row.channel_idx)[0][0],
                     np.where(unique_ids['times'] == row.time_idx)[0][0],
                     np.where(unique_ids['pos'] == row.pos_idx)[0][0],
            ] = np.atleast_3d(im_list[im_nbr])
        # Return squeezed stack and string that indicates dimension order
        im_stack, dim_str = self.squeeze_stack(im_stack)
        return im_stack, dim_str

    def download_files(self, file_names, dest_dir):
        """
        Download files from S3 directory specified in init to a directory
        given list of file names.

        :param list file_names: List of (str) file names
        :param str dest_dir: Destination directory path
        """
        with concurrent.futures.ThreadPoolExecutor(self.nbr_workers) as ex:
            {ex.submit(self.download_file, file_name, dest_dir):
                file_name for file_name in file_names}

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
        key = "/".join([self.dir_name, file_name])
        dest_path = os.path.join(dest_dir, file_name)
        s3_client.download_file(
            self.bucket_name,
            key,
            dest_path,
        )
