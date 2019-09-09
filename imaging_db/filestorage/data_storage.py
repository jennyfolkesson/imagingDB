from abc import ABCMeta, abstractmethod
import concurrent.futures
import numpy as np

STORAGE_MOUNT_POINT = '/Volumes/data_lg/czbiohub-imaging/'
S3_BUCKET_NAME = "czbiohub-imaging"


class DataStorage(metaclass=ABCMeta):
    """Abstract class with required functions for all data transfers"""

    def __init__(self, storage_dir, nbr_workers=None, access_point=None):
        """
        Initialize abstract class. Main directories for both S3 and local
        storage are

        raw_frames: For datasets that have been parsed into individual
            2D frames with indices channels, timepoints, slices and positions.
        raw_files: For files that have not been separated into frames + metadata.
            They're copied to storage as is.

        :param str storage_dir: Directory name (dataset ID) in raw_frames or
            raw_files
        :param int nbr_workers: Number of workers for uploads/downloads
        :param str/None access_point: If not using predefined storage locations,
            this parameter refers to mount_point for local storage and
            bucket_name for S3 storage.
        """
        self.storage_dir = storage_dir
        self.nbr_workers = nbr_workers
        self.access_point = access_point

    @staticmethod
    def make_stack_from_meta(global_meta, frames_meta):
        """
        Given global metadata, instantiate an image stack. The default order
        of frames is:
        X Y [gray/RGB] Z C T P
        Image height, width, colors (1 or 3), the z depth, channels,
        timepoints, positions

        :param dict global_meta: Global metadata for dataset
        :param dataframe frames_meta: Local metadata and paths for each file
        :return np.array im_stack: Empty image stack with dimension given by
            metadata
        :return dict unique_ids: Unique indices in frames_meta, order is
            sctp: slices, channels, timepoints, positions
        """
        # Metadata don't have to be indexed starting at 0 or continuous
        unique_ids = {
            'slices': np.unique(frames_meta["slice_idx"]),
            'channels': np.unique(frames_meta["channel_idx"]),
            'times': np.unique(frames_meta["time_idx"]),
            'pos': np.unique(frames_meta["pos_idx"]),
        }
        stack_shape = (
            global_meta["im_height"],
            global_meta["im_width"],
            global_meta["im_colors"],
            len(unique_ids['slices']),
            len(unique_ids['channels']),
            len(unique_ids['times']),
            len(unique_ids['pos']),
        )
        im_stack = np.zeros(stack_shape, global_meta["bit_depth"])
        return im_stack, unique_ids

    @staticmethod
    def squeeze_stack(im_stack):
        """
        Return squeezed stack and string that indicates dimension orders.
        See get_stack_from_meta for possible dimensions.

        :param np.array im_stack: Image stack, potentially with singleton
            dimensions
        :return np.array im_stack: Image stack with singleton dimensions
            removed
        :return str dim_str: String indicating remaining non-singleton
            dimensions in image stack
        """
        dim_order = "XYGZCTP"
        single_dims = np.where(np.asarray(im_stack.shape) == 1)[0]
        dim_str = ''.join(x for x in dim_order
                          if dim_order.index(x) not in single_dims)
        return np.squeeze(im_stack), dim_str

    @abstractmethod
    def assert_unique_id(self):
        """
        Makes sure directory with dataset ID doesn't already exist in storage

        :raise AssertionError: if directory exists
        """
        raise NotImplementedError

    @abstractmethod
    def nonexistent_storage_path(self, storage_path):
        """
        Checks that a given path to a file in storage doesn't already exist.

        :param str storage_path: Path in local or S3 storage
        :return bool: True if file doesn't exist in storage, False otherwise
        """
        raise NotImplementedError

    @abstractmethod
    def upload_frames(self, file_names, im_stack, file_format=".png"):
        """
        Uploads all frames to storage using threading or multiprocessing

        :param list file_names: Image file names (str)
        :param np.array im_stack: all 2D frames from file converted to stack
        :param str file_format: file format for frames to be written in storage
        """
        raise NotImplementedError

    @abstractmethod
    def upload_im(self, im_name, im, file_format='.png'):
        """
        Save image to storage after checking that the path to file doesn't
        already exist in storage.

        :param str im_name: File name for image
        :param np.array im: 2D image
        :param str file_format: File format for writing image
        """
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, file_path):
        """
        Upload a single file to storage by copying (file is not opened).

        :param str file_path: Full path to file to be moved to storage
        """
        raise NotImplementedError

    @abstractmethod
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
        assert len(stack_shape) > 2, "Stack shape must be 3D"
        im_stack = np.zeros(stack_shape, dtype=bit_depth)

        with concurrent.futures.ThreadPoolExecutor(self.nbr_workers) as executor:
            pool_result = executor.map(self.get_im, file_names)

        has_color_dim = True
        if stack_shape[2] > 1:
            has_color_dim = False
        for im_nbr, im in enumerate(pool_result):
            if has_color_dim:
                im = np.atleast_3d(im)
            im_stack[..., im_nbr] = im
        return im_stack

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
        im_stack, unique_ids = self.make_stack_from_meta(global_meta, frames_meta)

        with concurrent.futures.ThreadPoolExecutor(self.nbr_workers) as executor:
            pool_result = executor.map(self.get_im, frames_meta['file_name'])

        im_list = list(pool_result)
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
        Download files from storage directory specified in init to a
        local directory given list of file names in storage directory.

        :param list file_names: List of (str) file names
        :param str dest_dir: Destination directory path
        """
        with concurrent.futures.ThreadPoolExecutor(self.nbr_workers) as ex:
            {ex.submit(self.download_file, file_name, dest_dir):
                 file_name for file_name in file_names}

    @abstractmethod
    def download_file(self, file_name, dest_dir):
        """
        Downloads/copies a single file from storage to local destination without
        reading it.

        :param str file_name: File name
        :param str dest_dir: Destination directory name
        """
        raise NotImplementedError
