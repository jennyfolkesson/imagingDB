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

        self.frames_meta = meta_utils.make_dataframe(6)
        self.frames_json_meta = []
        self.meta_dict = {'local_key': 'local_value'}
        for i, (c, z) in enumerate(itertools.product(range(3), range(2))):
            im_name = 'im_c00{}_z00{}_t005_p050.png'.format(c, z)
            self.frames_meta.loc[i, 'file_name'] = im_name
            self.frames_meta.loc[i, 'channel_idx'] = c
            self.frames_meta.loc[i, 'slice_idx'] = z
            self.frames_meta.loc[i, 'pos_idx'] = 50
            self.frames_meta.loc[i, 'time_idx'] = 5
            self.frames_meta.loc[i, 'sha256'] = self.sha256
            self.frames_json_meta.append(self.meta_dict)

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
        db_inst.insert_frames(
            session=self.session,
            description='test frames',
            frames_meta=self.frames_meta,
            frames_json_meta=self.frames_json_meta,
            global_meta=self.global_meta,
            global_json_meta=self.global_json_meta,
            microscope=self.microscope,
            parent_dataset=None,
        )
        # query frames
        all_frames = self.session.query(db_ops.Frames) \
            .join(db_ops.FramesGlobal) \
            .join(db_ops.DataSet) \
            .filter(db_ops.DataSet.dataset_serial == self.dataset_serial) \
            .order_by(db_ops.Frames.file_name)
        for i, (c, z) in enumerate(itertools.product(range(3), range(2))):
            im_name = 'im_c00{}_z00{}_t005_p050.png'.format(c, z)
            self.assertEqual(all_frames[i].file_name, im_name)
            self.assertEqual(all_frames[i].channel_idx, c)
            self.assertEqual(all_frames[i].slice_idx, z)
            self.assertEqual(all_frames[i].time_idx, 5)
            self.assertEqual(all_frames[i].pos_idx, 50)
            self.assertEqual(all_frames[i].sha256, self.sha256)
            self.assertDictEqual(all_frames[i].metadata_json, self.meta_dict)
        # query frames_global
        global_query = self.session.query(db_ops.FramesGlobal) \
            .join(db_ops.DataSet) \
            .filter(db_ops.DataSet.dataset_serial == self.dataset_serial)
        self.assertEqual(global_query[0].s3_dir, self.global_meta['s3_dir'])
        self.assertEqual(global_query[0].nbr_frames, self.global_meta['nbr_frames'])
        self.assertEqual(global_query[0].im_width, self.global_meta['im_width'])
        self.assertEqual(global_query[0].im_height, self.global_meta['im_height'])
        self.assertEqual(global_query[0].nbr_slices, self.global_meta['nbr_slices'])
        self.assertEqual(global_query[0].nbr_channels, self.global_meta['nbr_channels'])
        self.assertEqual(global_query[0].nbr_positions, self.global_meta['nbr_positions'])
        self.assertEqual(global_query[0].nbr_timepoints, self.global_meta['nbr_timepoints'])
        self.assertEqual(global_query[0].im_colors, self.global_meta['im_colors'])
        self.assertEqual(global_query[0].bit_depth, self.global_meta['bit_depth'])

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

    def test_get_filenames(self):
        db_inst = db_ops.DatabaseOperations(
            dataset_serial=self.dataset_serial,
        )
        db_inst.insert_frames(
            session=self.session,
            description='test frames',
            frames_meta=self.frames_meta,
            frames_json_meta=self.frames_json_meta,
            global_meta=self.global_meta,
            global_json_meta=self.global_json_meta,
            microscope=self.microscope,
            parent_dataset=None,
        )
        s3_dir, file_names = db_inst.get_filenames(
            session=self.session,
        )
        self.assertEqual(s3_dir, self.global_meta['s3_dir'])
        for i, (c, z) in enumerate(itertools.product(range(3), range(2))):
            im_name = 'im_c00{}_z00{}_t005_p050.png'.format(c, z)
            self.assertEqual(file_names[i], im_name)
