import concurrent.futures
import cv2
import numpy as np
import os
import shutil

import imaging_db.filestorage.data_storage as data_storage


class LocalStorage(data_storage.DataStorage):
    """Class for handling image and file transfers to local storage"""

    def __init__(self,
                 storage_dir,
                 nbr_workers=None,
                 mount_point=None):
        """
        Local storage is assumed to be mounted at STORAGE_MOUNT_POINT
        unless otherwise specified.

        Main directories for both S3 and local storage are

        raw_frames: For datasets that have been parsed into individual
            2D frames with indices channels, timepoints, slices and positions.
        raw_files: For files that have not been separated into frames + metadata.
            They're copied to storage as is.

        :param str storage_dir: Directory name (dataset ID) in raw_frames or
        raw_files (e.g. raw_frames/ID-YYYY-MM-DD-HH-MM-SS-SSSS)
        :param int nbr_workers: Number of workers for uploads/downloads
        :param str/None mount_point: Path to where local storage is mounted.
            Default /Volumes/data_lg/czbiohub-imaging
        """
        super().__init__(storage_dir,
                         nbr_workers)

        if mount_point is None:
            self.mount_point = data_storage.STORAGE_MOUNT_POINT
        else:
            self.mount_point = mount_point
        assert os.path.exists(self.mount_point),\
            "Make sure local storage is mounted, dir {} doesn't exist".format(
                self.mount_point,
            )

    def assert_unique_id(self):
        """
        Makes sure directory with dataset ID doesn't already exist in storage

        :raise AssertionError: if directory exists
        """
        dir_path = os.path.join(self.mount_point, self.storage_dir)
        assert os.path.exists(dir_path) is False,\
            "ID {} already exists in storage".format(dir_path)

    def upload_frames(self, file_names, im_stack, file_format=".png"):
        """
        Writes all frames to storage using threading or multiprocessing

        :param list file_names: Image file names (str)
        :param np.array im_stack: all 2D frames from file converted to stack
        :param str file_format: file format for frames to be written in storage
        """
        # Make sure number of file names matches stack shape
        assert len(file_names) == im_stack.shape[-1], \
            "Number of file names {} doesn't match slices {}".format(
                len(file_names), im_stack.shape[-1])

        path_im_tuples = []
        for i, file_name in enumerate(file_names):
            path_i = os.path.join(
                self.mount_point,
                self.storage_dir,
                file_name,
            )
            path_im_tuples.append((path_i, im_stack[..., i]))

        with concurrent.futures.ProcessPoolExecutor(self.nbr_workers) as ex:
            res = ex.map(self.upload_im_tuple, path_im_tuples)

    def upload_im_tuple(self, path_im_tuple):
        """
        Save image to storage after checking that the path to file doesn't
        already exist in storage.

        :param tuple file_name: (File name str and image np.array)
        """
        (file_name, im) = path_im_tuple
        cv2.imwrite(file_name, im)

    def upload_im(self, file_name, im, file_format='.png'):
        """
        Save image to storage after checking that the path to file doesn't
        already exist in storage.

        :param str file_name: File name for image
        :param np.array im: 2D image
        :param str file_format: File format for writing image
        """
        cv2.imwrite(file_name, im)

    def upload_file(self, file_name):
        """
        Upload a single file to storage by copying (file is not opened).

        :param str file_name: full path to local file to be moved to storage
        """
        # ID should be unique, make sure it doesn't already exist
        self.assert_unique_id()

        file_no_path = file_name.split("/")[-1]
        save_path = os.path.join(
            self.mount_point,
            self.storage_dir,
            file_no_path,
        )
        shutil.copy(file_name, save_path)

    def get_im(self, file_name):
        """
        Given file name, fetch 2D image (frame) from storage.
        File name consists of raw_files/raw_frames + dataset ID +
        image name (im_c***_z***_t***_p***.png)

        :param str file_name: File name of 2D image (frame)
        :return np.array im: 2D image
        """
        raise NotImplementedError

    def get_stack(self, file_names, stack_shape, bit_depth):
        """
        Given file names and a 3D stack shape, this will fetch corresponding
        images, attempt to fit them into the stack and return image stack.
        This function assumes that the frames in the list are contiguous,
        i.e. the length of the file name will be the last dimension of
        the image stack.

        :param list of str file_names: Frame file names
        :param tuple stack_shape: Shape of image stack
        :param dtype bit_depth: Bit depth
        :return np.array im_stack: Stack of 2D images
        """
        raise NotImplementedError

    def get_stack_from_meta(self, global_meta, frames_meta):
        """
        Given global metadata, instantiate an image stack. The default order
        of frames is:
        X Y [gray/RGB] Z C T P
        X: Image height
        Y: Image width
        G: Grayscale or RGB (1 or 3 dims)
        Z: The slice (z) depth
        C: Channel index
        T: Timepoint index
        P: Position (FOV) index

        Retrieve all frames from local metadata and return image stack.
        Ones in stack shape indicates singleton dimensions.
        The stack is then squeezed to remove singleton dimensions, and a string
        is returned to indicate which dimensions are kept and in what order.

        :param dict global_meta: Global metadata for dataset
        :param dataframe frames_meta: Local metadata and paths for each file
        :return np.array im_stack: Stack of 2D images with dimensions given below
        :return str dim_str: String indicating order of stack dimensions
            Possible values: XYGZCTP
            X=im_height, Y=im_width, G=[gray/RGB] (1 or 3),
            Z=slice_idx, C=channel_idx, T=time_idx, P=pos_idx
        """
        raise NotImplementedError

    def download_files(self, file_names, dest_dir):
        """
        Download files from storage directory specified in init to a
        local directory given list of file names in storage directory.

        :param list file_names: List of (str) file names
        :param str dest_dir: Destination directory path
        """
        raise NotImplementedError

    def download_file(self, file_name, dest_dir):
        """
        Downloads/copies a single file from storage to local destination without
        reading it.

        :param str file_name: File name
        :param str dest_dir: Destination directory name
        """
        raise NotImplementedError
