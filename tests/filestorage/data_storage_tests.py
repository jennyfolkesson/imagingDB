import itertools
import nose.tools
import numpy as np
import unittest
from unittest.mock import patch

import imaging_db.filestorage.data_storage as data_storage
import imaging_db.utils.meta_utils as meta_utils


class TestDataStorage(unittest.TestCase):
    """
    Abstract classes can be tested with Unittest's mock patch
    """

    @patch.multiple(data_storage.DataStorage, __abstractmethods__=set())
    def setUp(self):
        self.frames_meta = meta_utils.make_dataframe()

        self.channel_ids = [0, 1, 2, 3, 4]
        self.slice_ids = [5, 6, 7]
        self.time_ids = [50]
        self.pos_ids = [3, 14, 26]
        for (c, z, t, p) in itertools.product(self.channel_ids,
                                              self.slice_ids,
                                              self.time_ids,
                                              self.pos_ids):
            meta_row = dict.fromkeys(meta_utils.DF_NAMES)
            meta_row['channel_idx'] = c
            meta_row['slice_idx'] = z
            meta_row['time_idx'] = t
            meta_row['pos_idx'] = p
            meta_row['sha256'] = 'AAAABBBB'
            meta_row['file_name'] = self._get_imname(meta_row)
            self.frames_meta = self.frames_meta.append(meta_row, ignore_index=True)
        print(self.frames_meta)
        self.im_height = 100
        self.im_width = 200
        self.im_colors = 1
        self.global_meta = {
            "storage_dir": 'storage_dir_path',
            "nbr_frames": self.frames_meta.shape[0],
            "im_height": self.im_height,
            "im_width": self.im_width,
            "im_colors": self.im_colors,
            "bit_depth": 'uint16',
            "nbr_slices": len(np.unique(self.frames_meta["slice_idx"])),
            "nbr_channels": len(np.unique(self.frames_meta["channel_idx"])),
            "nbr_timepoints": len(np.unique(self.frames_meta["time_idx"])),
            "nbr_positions": len(np.unique(self.frames_meta["pos_idx"])),
        }
        print(self.global_meta)
        self.storage_inst = data_storage.DataStorage('test_storage', 12)

    def _get_imname(self, meta_row, file_format='.png', int2str_len=3):
        """
        Generate image (frame) name given frame metadata and file format.

        :param dict meta_row: Metadata for frame, must contain frame indices
        :param str file_format: File extension, including '.' (e.g. '.png')
        :param int int2str_len: How many integers will be used for each idx
        :return str imname: Image file name
        """
        return "im_c" + str(meta_row["channel_idx"]).zfill(int2str_len) + \
            "_z" + str(meta_row["slice_idx"]).zfill(int2str_len) + \
            "_t" + str(meta_row["time_idx"]).zfill(int2str_len) + \
            "_p" + str(meta_row["pos_idx"]).zfill(int2str_len) + \
            file_format

    def test__init__(self):
        self.assertEqual(self.storage_inst.storage_dir, 'test_storage')
        self.assertEqual(self.storage_inst.nbr_workers, 12)

    def test_make_stack_from_meta(self):
        im_stack, unique_ids = self.storage_inst.make_stack_from_meta(
            global_meta=self.global_meta,
            frames_meta=self.frames_meta,
        )
        # Shape should be XYGZCTP
        stack_shape = im_stack.shape
        self.assertEqual(stack_shape[0], self.im_height)
        self.assertEqual(stack_shape[1], self.im_width)
        self.assertEqual(stack_shape[2], self.im_colors)
        self.assertEqual(stack_shape[3], len(self.slice_ids))
        self.assertEqual(stack_shape[4], len(self.channel_ids))
        self.assertEqual(stack_shape[5], len(self.time_ids))
        self.assertEqual(stack_shape[6], len(self.pos_ids))
        # dtype should be uint16
        self.assertEqual(im_stack.dtype, 'uint16')
        # Check unique ids from frames_meta
        self.assertListEqual(unique_ids['slices'].tolist(), self.slice_ids)
        self.assertListEqual(unique_ids['channels'].tolist(), self.channel_ids)
        self.assertListEqual(unique_ids['times'].tolist(), self.time_ids)
        self.assertListEqual(unique_ids['pos'].tolist(), self.pos_ids)

    def test_squeeze_stack(self):
        im_stack = np.zeros((100, 200, 1, 10, 20, 1, 30))
        im_stack, dim_str = self.storage_inst.squeeze_stack(im_stack)
        # Singleton dimensions should be removed
        self.assertTupleEqual(im_stack.shape, (100, 200, 10, 20, 30))
        # Singleton dims were G and T, so remaining should be XYZCP
        self.assertEqual(dim_str, 'XYZCP')

    def test_squeeze_stack_no_singleton(self):
        im_stack = np.zeros((100, 200, 3, 10, 20, 40, 30))
        im_stack, dim_str = self.storage_inst.squeeze_stack(im_stack)
        self.assertTupleEqual(im_stack.shape, (100, 200, 3, 10, 20, 40, 30))
        self.assertEqual(dim_str, 'XYGZCTP')

    @nose.tools.raises(NotImplementedError)
    def test_assert_unique_id(self):
        self.storage_inst.assert_unique_id()

    @nose.tools.raises(NotImplementedError)
    def test_upload_frames(self):
        self.storage_inst.upload_frames(
            file_names=['im1.png', 'im2.png'],
            im_stack=np.zeros((3, 4, 5)),
        )

    @nose.tools.raises(NotImplementedError)
    def test_upload_im(self):
        self.storage_inst.upload_im('im1.png', np.zeros((5, 10)))

    @nose.tools.raises(NotImplementedError)
    def test_upload_file(self):
        self.storage_inst.upload_file('file_name.hdf5')

    @nose.tools.raises(NotImplementedError)
    def test_get_im(self):
        self.storage_inst.get_im('file.png')

    @nose.tools.raises(NotImplementedError)
    def test_get_stack(self):
        self.storage_inst.get_stack(['file1.png'], (10, 10, 1), 'uint16')

    @nose.tools.raises(NotImplementedError)
    def get_stack_from_meta(self):
        self.storage_inst.get_stack_from_meta(self.global_meta, self.frames_meta)

    @nose.tools.raises(NotImplementedError)
    def test_download_files(self):
        self.storage_inst.download_files(['f1.png', 'f2.png'], 'local_dest')

    @nose.tools.raises(NotImplementedError)
    def test_download_file(self):
        self.storage_inst.download_file('f1.png', 'local_dest')
