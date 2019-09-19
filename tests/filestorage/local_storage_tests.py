import cv2
import nose.tools
import numpy as np
import numpy.testing
import os
from testfixtures import TempDirectory
import unittest

import imaging_db.filestorage.local_storage as local_storage
import imaging_db.utils.meta_utils as meta_utils
from tests.cli.query_data_tests import captured_output


class TestLocalStorage(unittest.TestCase):

    def setUp(self):
        """
        Set up temporary test directory and mock local storage
        """
        self.storage_dir = "raw_frames/ISP-2005-01-01-01-00-00-0001"
        self.existing_dir = "raw_frames/ISP-2005-05-09-20-00-00-0001"
        # Create temporary directory and write temp image
        self.tempdir = TempDirectory()
        self.temp_path = self.tempdir.path
        # Write temporary image
        self.im = np.zeros((15, 12), dtype=np.uint16)
        self.im[0:5, 3:7] = 5000
        self.im_name = 'im_0.png'
        cv2.imwrite(os.path.join(self.temp_path, self.im_name), self.im)
        self.file_path = os.path.join(self.temp_path, self.im_name)
        # Create a grayscale image stack for testing
        self.im_stack = np.ones((10, 15, 5), np.uint16) * 3000
        self.im_stack[0:5, 2:4, 0] = 42
        for i in range(1, 5):
            self.im_stack[3:7, 12:14, i] = i * 10000
        self.stack_names = ['im1.png', 'im2.png', 'im3.png', 'im4.png', 'im5.png']
        self.nbr_workers = 4
        # Mock file storage
        self.tempdir.makedir('storage_mount_point')
        self.mount_point = os.path.join(self.temp_path, 'storage_mount_point')
        self.tempdir.makedir('storage_mount_point/raw_files')
        self.tempdir.makedir('storage_mount_point/raw_frames')
        # Instantiate class
        self.data_storage = local_storage.LocalStorage(
            storage_dir=self.storage_dir,
            nbr_workers=self.nbr_workers,
            access_point=self.mount_point,
        )
        self.storage_path = os.path.join(
            self.mount_point,
            self.storage_dir,
            self.im_name,
        )
        # Create an already uploaded image
        self.existing_path = os.path.join(
            self.mount_point,
            self.existing_dir,
            self.im_name,
        )
        self.tempdir.makedir(
            os.path.join('storage_mount_point', self.existing_dir),
        )
        self.existing_storage = local_storage.LocalStorage(
            storage_dir=self.existing_dir,
            nbr_workers=self.nbr_workers,
            access_point=self.mount_point,
        )
        cv2.imwrite(self.existing_path, self.im)

    def tearDown(self):
        """
        Tear down temporary folder and files and stop S3 mock
        """
        TempDirectory.cleanup_all()
        nose.tools.assert_equal(os.path.isdir(self.temp_path), False)

    @nose.tools.raises(AssertionError)
    def test_init_bad_mount(self):
        local_storage.LocalStorage(
            storage_dir=self.existing_dir,
            nbr_workers=self.nbr_workers,
            access_point='/not/existing/mount_point',
        )

    @nose.tools.raises(AssertionError)
    def test_init_no_mount(self):
        # This test may fail locally if you have local storage mounted
        local_storage.LocalStorage(
            storage_dir=self.existing_dir,
            nbr_workers=self.nbr_workers,
        )

    def test_assert_unique_id(self):
        self.data_storage.assert_unique_id()

    @nose.tools.raises(AssertionError)
    def test_assert_unique_id_exists(self):
        self.existing_storage.assert_unique_id()

    def test_nonexistent_storage_path(self):
        self.assertTrue(
            self.data_storage.nonexistent_storage_path(
                storage_path=self.storage_path,
            )
        )

    def test_existing_storage_path(self):
        self.assertFalse(
            self.existing_storage.nonexistent_storage_path(
                storage_path=self.existing_path,
            )
        )

    def test_get_storage_path(self):
        storage_path = self.data_storage.get_storage_path(self.im_name)
        self.assertEqual(storage_path, self.storage_path)

    def test_upload_frames(self):
        # Upload image stack
        self.data_storage.upload_frames(
            file_names=self.stack_names,
            im_stack=self.im_stack,
        )
        # Get images from uploaded stack and validate that the contents are unchanged
        for im_nbr in range(len(self.stack_names)):
            storage_path = os.path.join(
                self.mount_point,
                self.storage_dir,
                self.stack_names[im_nbr],
            )
            im = cv2.imread(storage_path, cv2.IMREAD_ANYDEPTH)
            # Assert that contents are the same
            nose.tools.assert_equal(im.dtype, np.uint16)
            nose.tools.assert_equal(im.shape, (10, 15))
            numpy.testing.assert_array_equal(im, self.im_stack[..., im_nbr])

    def test_upload_frames_color(self):
        # Create color image stack
        im_stack = np.ones((10, 15, 3, 2), np.uint16) * 3000
        im_stack[0:5, 2:4, :, 0] = 42
        im_stack[3:7, 12:14, :, 1] = 10000
        # Expected color image shape
        expected_shape = (10, 15, 3)
        rgb_names = ['im_rgb1.png', 'im_rgb2.png']
        self.data_storage.upload_frames(rgb_names, im_stack)
        # Get images and validate that the contents are unchanged
        for im_nbr in range(len(rgb_names)):
            storage_path = os.path.join(
                self.mount_point,
                self.storage_dir,
                rgb_names[im_nbr],
            )
            im = cv2.imread(
                storage_path,
                cv2.IMREAD_ANYDEPTH | cv2.IMREAD_ANYCOLOR,
            )
            # Assert that contents are the same
            nose.tools.assert_equal(im.shape, expected_shape)
            nose.tools.assert_equal(im.dtype, np.uint16)
            numpy.testing.assert_array_equal(im, im_stack[..., im_nbr])

    def test_upload_im_tuple(self):
        self.data_storage.upload_im_tuple(path_im_tuple=(self.storage_path, self.im))
        im = cv2.imread(self.storage_path, cv2.IMREAD_ANYDEPTH)
        numpy.testing.assert_array_equal(im, self.im)

    def test_upload_existing_im_tuple(self):
        self.data_storage.upload_im_tuple(
            path_im_tuple=(self.storage_path, self.im),
        )
        with captured_output() as (out, err):
            self.data_storage.upload_im_tuple(
                path_im_tuple=(self.storage_path, self.im),
            )
        std_output = out.getvalue().strip()
        self.assertEqual(
            std_output,
            "File {} already exists.".format(self.storage_path),
        )

    def test_upload_im(self):
        self.data_storage.upload_im(im_name=self.im_name, im=self.im)
        im = cv2.imread(self.storage_path, cv2.IMREAD_ANYDEPTH)
        numpy.testing.assert_array_equal(im, self.im)

    def test_upload_existing_im(self):
        self.data_storage.upload_im(im_name=self.im_name, im=self.im)
        with captured_output() as (out, err):
            self.data_storage.upload_im(im_name=self.im_name, im=self.im)
        std_output = out.getvalue().strip()
        self.assertEqual(
            std_output,
            "File {} already exists.".format(self.storage_path),
        )

    def test_upload_file(self):
        self.data_storage.upload_file(file_path=self.file_path)
        expected_im = cv2.imread(self.storage_path, cv2.IMREAD_ANYDEPTH)
        # Assert that contents are the same
        nose.tools.assert_equal(expected_im.dtype, np.uint16)
        numpy.testing.assert_array_equal(expected_im, self.im)

    def test_get_im(self):
        # Load the temporary image
        im_out = self.existing_storage.get_im(file_name=self.im_name)
        # Assert that contents are the same
        nose.tools.assert_equal(im_out.dtype, np.uint16)
        numpy.testing.assert_array_equal(im_out, self.im)

    def test_get_stack(self):
        self.data_storage.upload_frames(self.stack_names, self.im_stack)
        # Load image stack in memory
        im_out = self.data_storage.get_stack(
            self.stack_names,
        )
        nose.tools.assert_equal(self.im_stack.shape, im_out.shape)
        for im_nbr in range(self.im_stack.shape[-1]):
            # Assert that contents are the same
            numpy.testing.assert_array_equal(
                im_out[..., im_nbr],
                self.im_stack[..., im_nbr],
            )

    def test_get_stack_with_shape(self):
        self.data_storage.upload_frames(self.stack_names, self.im_stack)
        # Load image stack in memory
        stack_shape = (10, 15, 1, 5)
        im_out = self.data_storage.get_stack_with_shape(
            self.stack_names,
            stack_shape=stack_shape,
            bit_depth=np.uint16,
        )
        im_out = np.squeeze(im_out)
        nose.tools.assert_equal(self.im_stack.shape, im_out.shape)
        for im_nbr in range(self.im_stack.shape[-1]):
            # Assert that contents are the same
            numpy.testing.assert_array_equal(
                im_out[..., im_nbr],
                self.im_stack[..., im_nbr],
            )

    def test_get_stack_with_shape_no_colordim(self):
        self.data_storage.upload_frames(self.stack_names, self.im_stack)
        # Load image stack in memory
        stack_shape = (10, 15, 5)
        im_out = self.data_storage.get_stack_with_shape(
            self.stack_names,
            stack_shape=stack_shape,
            bit_depth=np.uint16,
        )
        nose.tools.assert_equal(self.im_stack.shape, im_out.shape)
        for im_nbr in range(self.im_stack.shape[-1]):
            # Assert that contents are the same
            numpy.testing.assert_array_equal(
                im_out[..., im_nbr],
                self.im_stack[..., im_nbr],
            )

    def test_get_stack_from_meta(self):
        # Upload image stack
        storage_dir = "raw_frames/ML-2005-05-23-10-00-00-0001"
        self.data_storage.upload_frames(self.stack_names, self.im_stack)
        global_meta = {
            "storage_dir": storage_dir,
            "nbr_frames": 5,
            "im_height": 10,
            "im_width": 15,
            "nbr_slices": 5,
            "nbr_channels": 1,
            "im_colors": 1,
            "bit_depth": "uint16",
            "nbr_timepoints": 1,
            "nbr_positions": 1,
        }
        # Download slices 1:4
        frames_meta = meta_utils.make_dataframe(nbr_frames=3)
        for i in range(3):
            sha = meta_utils.gen_sha256(self.im_stack[..., i + 1])
            frames_meta.loc[i] = [0, i + 1, 0, "A", self.stack_names[i + 1], 0, sha]

        im_stack, dim_order = self.data_storage.get_stack_from_meta(
            global_meta=global_meta,
            frames_meta=frames_meta,
        )
        # Stack has X = 10, Y = 15, grayscale, Z = 3, C = 1, T = 1, P = 1
        # so expected stack shape and order should be:
        expected_shape = (10, 15, 3)
        nose.tools.assert_equal(im_stack.shape, expected_shape)
        nose.tools.assert_equal(dim_order, "XYZ")

    def test_download_files(self):
        self.data_storage.upload_frames(self.stack_names, self.im_stack)
        self.data_storage.download_files(
            file_names=self.stack_names,
            dest_dir=self.temp_path,
        )
        # Read downloaded file and assert that contents are the same
        for i, im_name in enumerate(self.stack_names):
            dest_path = os.path.join(self.temp_path, im_name)
            im_out = cv2.imread(dest_path, cv2.IMREAD_ANYDEPTH)
            nose.tools.assert_equal(im_out.dtype, np.uint16)
            numpy.testing.assert_array_equal(im_out, self.im_stack[..., i])

    def test_download_file(self):
        # Download the temporary image then read it and validate
        self.existing_storage.download_file(
            file_name=self.im_name,
            dest_dir=self.temp_path,
        )
        # Read downloaded file and assert that contents are the same
        dest_path = os.path.join(self.temp_path, self.im_name)
        im_out = cv2.imread(dest_path, cv2.IMREAD_ANYCOLOR | cv2.IMREAD_ANYDEPTH)
        nose.tools.assert_equal(im_out.dtype, np.uint16)
        numpy.testing.assert_array_equal(im_out, self.im)
