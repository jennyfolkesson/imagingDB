import itertools

import tests.database.db_basetest as db_basetest
import imaging_db.database.db_operations as db_ops
import imaging_db.utils.meta_utils as meta_utils


class TestDBOperations(db_basetest.DBBaseTest):
    """
    Test the database operations
    """

    def setUp(self):
        super().setUp()

        self.dataset_serial = 'TEST-2005-10-09-20-00-00-0001'
        self.global_meta = {
            "s3_dir": "dir_name",
            "nbr_frames": 6,
            "im_height": 256,
            "im_width": 512,
            "im_colors": 1,
            "bit_depth": "uint16",
            "nbr_slices": 2,
            "nbr_channels": 3,
            "nbr_timepoints": 1,
            "nbr_positions": 1,
        }
        self.global_json_meta = {'status': 'test'}
        self.microscope = 'test_microscope'
        self.description = 'This is a test'
        self.s3_dir = 'testing/TEST-2005-10-09-20-00-00-0001'
        self.sha256 = 'aaabbbccc'

    def tearDown(self):
        super().tearDown()

    def test_connection(self):
        db_ops.test_connection(self.session)

    def test_assert_unique_id(self):
        db_inst = db_ops.DatabaseOperations(
            dataset_serial=self.dataset_serial,
        )
        db_inst.assert_unique_id(self.session)

    def test_insert_frames(self):
        db_inst = db_ops.DatabaseOperations(
            dataset_serial=self.dataset_serial,
        )
        frames_meta = meta_utils.make_dataframe(6)
        frames_json_meta = []
        meta_dict = {'local_key': 'local_value'}
        for i, (c, z) in enumerate(itertools.product(range(3), range(2))):
            im_name = 'im_c{}_z{}_t005_p050.png'.format(c, z)
            frames_meta.loc[i, 'file_name'] = im_name
            frames_meta.loc[i, 'channel_idx'] = c
            frames_meta.loc[i, 'slice_idx'] = z
            frames_meta.loc[i, 'pos_idx'] = 50
            frames_meta.loc[i, 'time_idx'] = 5
            frames_meta.loc[i, 'sha256'] = self.sha256
            frames_json_meta.append(meta_dict)
        db_inst.insert_frames(
            session=self.session,
            description='test frames',
            frames_meta=frames_meta,
            frames_json_meta=frames_json_meta,
            global_meta=self.global_meta,
            global_json_meta=self.global_json_meta,
            microscope=self.microscope,
            parent_dataset=None,
        )
        all_frames = self.session.query(db_ops.Frames) \
            .join(db_ops.FramesGlobal) \
            .join(db_ops.DataSet) \
            .filter(db_ops.DataSet.dataset_serial == self.dataset_serial) \
            .order_by(db_ops.Frames.file_name)
        for i, (c, z) in enumerate(itertools.product(range(3), range(2))):
            im_name = 'im_c{}_z{}_t005_p050.png'.format(c, z)
            self.assertEqual(all_frames[i].file_name, im_name)
            self.assertEqual(all_frames[i].channel_idx, c)
            self.assertEqual(all_frames[i].slice_idx, z)
            self.assertEqual(all_frames[i].time_idx, 5)
            self.assertEqual(all_frames[i].pos_idx, 50)
            self.assertEqual(all_frames[i].sha256, self.sha256)
            self.assertDictEqual(all_frames[i].metadata_json, meta_dict)

    def test_insert_file(self):
        dataset_serial = 'TEST-2005-10-12-20-00-00-0001'
        db_inst = db_ops.DatabaseOperations(
            dataset_serial=dataset_serial,
        )
        db_inst.insert_file(
            session=self.session,
            description=self.description,
            s3_dir=self.s3_dir,
            global_json_meta=self.global_json_meta,
            microscope=self.microscope,
            sha256=self.sha256,
        )
        # Assert insert by query
        datasets = self.session.query(db_ops.DataSet)
        self.assertEqual(datasets.count(), 1)
        dataset = datasets[0]
        self.assertEqual(dataset.id, 1)
        self.assertEqual(dataset.dataset_serial, dataset_serial)
        self.assertEqual(dataset.description, self.description)
        date_time = dataset.date_time
        self.assertEqual(date_time.year, 2005)
        self.assertEqual(date_time.month, 10)
        self.assertEqual(date_time.day, 12)
        self.assertEqual(dataset.microscope, self.microscope)
        self.assertEqual(dataset.description, self.description)
