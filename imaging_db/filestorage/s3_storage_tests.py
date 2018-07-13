import boto3
import cv2
from moto import mock_s3
import nose.tools
import numpy as np
import numpy.testing
import os
from testfixtures import TempDirectory
import unittest

import imaging_db.filestorage.s3_storage as s3_storage
import imaging_db.utils.image_utils as im_utils


class TestImageValidator(unittest.TestCase):

    def setUp(self):
        """
        Set up
        """
        self.bucket_name = 'czbiohub-imaging'
        self.folder_name = "raw_slices/ISP-2018-06-09-20-00-00-0001"
        # Create temporary directory and write temp image
        self.tempdir = TempDirectory()
        self.temp_path = self.tempdir.path
        # Write temporary image
        self.im = np.zeros((15, 12), dtype=np.uint16)
        self.im[0:5, 3:7] = 5000
        res, im_encoded = cv2.imencode('.png', self.im)
        im_encoded = im_encoded.tostring()
        self.im_name = 'im_0.png'
        self.tempdir.write(self.im_name, im_encoded)
        self.file_path = os.path.join(self.temp_path, self.im_name)
        # Create a grayscale image stack for testing
        self.im_stack = np.ones((10, 15, 2), np.uint16) * 3000
        self.im_stack[0:5, 2:4, 0] = 42
        self.im_stack[3:7, 12:14, 1] = 10000
        self.stack_names = ["im1.png", "im2.png"]

    def tearDown(self):
        """
        Tear down temporary folder and file structure
        """
        TempDirectory.cleanup_all()
        nose.tools.assert_equal(os.path.isdir(self.temp_path), False)

    @mock_s3
    def test_upload_file(self):
        conn = boto3.resource('s3', region_name='us-west-2')
        conn.create_bucket(Bucket=self.bucket_name)
        data_storage = s3_storage.DataStorage(self.folder_name)
        data_storage.upload_file(file_name=self.file_path)
        # Make sure the image uploaded in setUp is unchanged
        key = "/".join([self.folder_name, self.im_name])
        byte_string = conn.Object(self.bucket_name,
                                  key).get()['Body'].read()
        # Construct an array from the bytes and decode image
        im_out = im_utils.deserialize_im(byte_string)
        # Assert that contents are the same
        nose.tools.assert_equal(im_out.dtype, np.uint16)
        numpy.testing.assert_array_equal(im_out, self.im)

    @mock_s3
    def test_upload_data(self):
        # Upload stack
        conn = boto3.resource('s3', region_name='us-west-2')
        conn.create_bucket(Bucket=self.bucket_name)
        stack_folder_name = "raw_slices/ML-2018-05-23-10-00-00-0001"
        data_storage = s3_storage.DataStorage(stack_folder_name)
        data_storage.upload_slices(self.stack_names, self.im_stack)
        # Get images from uploaded stack and validate that the contents are unchanged
        for im_nbr in range(len(self.stack_names)):
            key = "/".join([stack_folder_name, self.stack_names[im_nbr]])
            byte_string = conn.Object(self.bucket_name, key).get()['Body'].read()
            # Construct an array from the bytes and decode image
            im = im_utils.deserialize_im(byte_string)
            # Assert that contents are the same
            nose.tools.assert_equal(im.dtype, np.uint16)
            nose.tools.assert_equal(im.shape, (10, 15))
            numpy.testing.assert_array_equal(im, self.im_stack[..., im_nbr])

    @mock_s3
    def test_upload_color_data(self):
        # Create color image stack
        conn = boto3.resource('s3', region_name='us-west-2')
        conn.create_bucket(Bucket=self.bucket_name)
        im_stack = np.ones((10, 15, 3, 2), np.uint16) * 3000
        im_stack[0:5, 2:4, :, 0] = 42
        im_stack[3:7, 12:14, :, 1] = 10000
        # Expected color image shape
        expected_shape = im_stack[..., 0].shape
        # Mock slice upload
        stack_folder_name = "raw_slices/ML-2018-05-23-10-00-00-0001"
        data_storage = s3_storage.DataStorage(stack_folder_name)
        data_storage.upload_slices(self.stack_names, im_stack)
        # Get images and validate that the contents are unchanged
        for im_nbr in range(len(self.stack_names)):
            key = "/".join([stack_folder_name, self.stack_names[im_nbr]])
            byte_string = conn.Object(self.bucket_name,
                                           key).get()['Body'].read()
            # Construct an array from the bytes and decode image
            im = im_utils.deserialize_im(byte_string)
            # Assert that contents are the same
            nose.tools.assert_equal(im.shape, expected_shape)
            nose.tools.assert_equal(im.dtype, np.uint16)
            numpy.testing.assert_array_equal(im, im_stack[..., im_nbr])

    @mock_s3
    def test_fetch_im(self):
        conn = boto3.resource('s3', region_name='us-west-2')
        conn.create_bucket(Bucket=self.bucket_name)
        folder_name = "raw_slices/ISP-2018-06-09-20-00-00-0001"
        data_storage = s3_storage.DataStorage(folder_name)
        data_storage.upload_file(file_name=self.file_path)
        # Load the temporary image
        im_out = data_storage.fetch_im(file_name=self.im_name)
        # Assert that contents are the same
        nose.tools.assert_equal(im_out.dtype, np.uint16)
        numpy.testing.assert_array_equal(im_out, self.im)

    @mock_s3
    def test_fetch_im_stack(self):
        conn = boto3.resource('s3', region_name='us-west-2')
        conn.create_bucket(Bucket=self.bucket_name)
        stack_folder_name = "raw_slices/ML-2018-05-23-10-00-00-0001"
        data_storage = s3_storage.DataStorage(stack_folder_name)
        data_storage.upload_slices(self.stack_names, self.im_stack)
        # Load image stack in memory
        stack_shape = (10, 15, 1, 2)
        im_out = data_storage.fetch_im_stack(
            self.stack_names,
            stack_shape=stack_shape,
            bit_depth=np.uint16)
        im_out = np.squeeze(im_out)
        nose.tools.assert_equal(self.im_stack.shape, im_out.shape)
        for im_nbr in range(self.im_stack.shape[-1]):
            # Assert that contents are the same
            numpy.testing.assert_array_equal(im_out[..., im_nbr],
                                             self.im_stack[..., im_nbr])

    @mock_s3
    def test_download_file(self):
        conn = boto3.resource('s3', region_name='us-west-2')
        conn.create_bucket(Bucket=self.bucket_name)
        folder_name = "raw_slices/ISP-2018-06-09-20-00-00-0001"
        data_storage = s3_storage.DataStorage(folder_name)
        data_storage.upload_file(file_name=self.file_path)
        # Download the temporary image then read it and validate
        dest_path = os.path.join(self.temp_path, "im_out.png")
        data_storage.download_file(file_name=self.im_name, dest_path=dest_path)
        # Read downloaded file and assert that contents are the same
        im_out = cv2.imread(dest_path, cv2.IMREAD_ANYCOLOR | cv2.IMREAD_ANYDEPTH)
        nose.tools.assert_equal(im_out.dtype, np.uint16)
        numpy.testing.assert_array_equal(im_out, self.im)
