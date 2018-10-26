import nose.tools
import unittest
from unittest.mock import patch

import imaging_db.images.file_splitter as file_splitter


def test_make_dataframe():
    nbr_frames = 3
    test_col_names = ["A", "B"]
    frames_meta = file_splitter.make_dataframe(
        nbr_frames=nbr_frames,
        col_names=test_col_names,
    )
    nose.tools.assert_equal(frames_meta.shape, (nbr_frames, len(test_col_names)))
    nose.tools.assert_equal(test_col_names, list(frames_meta))


def test_make_empty_dataframe():
    expected_names = [
        "channel_idx",
        "slice_idx",
        "time_idx",
        "channel_name",
        "file_name",
        "pos_idx"]
    frames_meta = file_splitter.make_dataframe(nbr_frames=None)
    nose.tools.assert_equal(expected_names, list(frames_meta))
    nose.tools.assert_true(frames_meta.empty)


def test_validate_global_meta():
    global_meta = {
        "s3_dir": "dir_name",
        "nbr_frames": 5,
        "im_height": 256,
        "im_width": 256,
        "im_colors": 1,
        "bit_depth": "uint16",
        "nbr_slices": 6,
        "nbr_channels": 7,
        "nbr_timepoints": 8,
        "nbr_positions": 9,
    }
    file_splitter.validate_global_meta(global_meta)


@nose.tools.raises(AssertionError)
def test_validate_global_meta_invalid():
    global_meta = {
        "s3_dir": "dir_name",
        "nbr_frames": 5,
        "im_height": 256,
        "im_width": 256,
        "im_colors": None,
        "bit_depth": "uint16",
        "nbr_slices": 6,
        "nbr_channels": 7,
        "nbr_timepoints": 8,
        "nbr_positions": 9,
    }
    file_splitter.validate_global_meta(global_meta)


@nose.tools.raises(AssertionError)
def test_validate_global_meta_missing():
    global_meta = {
        "s3_dir": "dir_name",
    }
    file_splitter.validate_global_meta(global_meta)


class TestFileSplitter(unittest.TestCase):
    """
    Abstract classes can be tested with Unittest's mock patch
    """

    @patch.multiple(file_splitter.FileSplitter, __abstractmethods__=set())
    def setUp(self):
        self.test_path = "/datapath/testfile.tif"
        self.test_dir = "raw_frames/ISP-2005-06-09-20-00-00-0001"
        self.mock_inst = file_splitter.FileSplitter(
            data_path=self.test_path,
            s3_dir=self.test_dir,
        )

    def test_init(self):
        nose.tools.assert_equal(self.mock_inst.data_path, self.test_path)
        nose.tools.assert_equal(self.mock_inst.s3_dir, self.test_dir)
        nose.tools.assert_equal(self.mock_inst.int2str_len, 3)
        nose.tools.assert_equal(self.mock_inst.file_format, '.png')

    def test_get_imname(self):
        meta_row = {
            "channel_idx": 6,
            "slice_idx": 13,
            "time_idx": 5,
            "pos_idx": 7,
        }
        im_name = self.mock_inst._get_imname(meta_row=meta_row)
        nose.tools.assert_equal(im_name, 'im_c006_z013_t005_p007.png')
