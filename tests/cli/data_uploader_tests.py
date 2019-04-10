import argparse
import boto3
from moto import mock_s3
import nose.tools
import numpy as np
import os
import pandas as pd
from testfixtures import TempDirectory
import tifffile
import unittest
from unittest.mock import patch

import imaging_db.cli.data_uploader as data_uploader
import tests.database.db_basetest as db_basetest


class TestDataUploader(db_basetest.DBBaseTest):
    """
    Test the data uploader
    """

    def setUp(self):
        super().setUp()
        # Setup mock S3 bucket
        self.mock = mock_s3()
        self.mock.start()
        self.conn = boto3.resource('s3', region_name='us-east-1')
        self.bucket_name = 'czbiohub-imaging'
        self.conn.create_bucket(Bucket=self.bucket_name)
        # Test metadata parameters
        self.nbr_channels = 2
        self.nbr_slices = 3
        # Mock S3 dir
        self.s3_dir = "raw_frames/TEST-2005-06-09-20-00-00-1000"
        # Create temporary directory and write temp image
        self.tempdir = TempDirectory()
        self.temp_path = self.tempdir.path
        # Temporary file with 6 frames, tifffile stores channels first
        self.im = 50 * np.ones((6, 10, 15), dtype=np.uint16)
        self.im[0, :5, 3:12] = 50000
        self.im[2, :5, 3:12] = 40000
        self.im[4, :5, 3:12] = 30000
        # Metadata
        self.description = 'ImageJ=1.52e\nimages=6\nchannels=2\nslices=3\nmax=10411.0'
        # Save test tif file
        self.file_path = os.path.join(self.temp_path, "A1_2_PROTEIN_test.tif")
        tifffile.imsave(
            self.file_path,
            self.im,
            description=self.description,
        )
        upload_csv = pd.DataFrame(
            columns=['dataset_id', 'file_name', 'description'],
        )
        upload_csv = upload_csv.append(
            {'dataset_id': 'TEST-2005-06-09-20-00-00-1000',
             'file_name': self.file_path,
             'description': 'Testing'},
            ignore_index=True,
        )
        print(upload_csv)
        self.csv_path = os.path.join(self.temp_path, "test_upload.csv")
        upload_csv.to_csv(self.csv_path)
        self.credentials_path = os.path.join(
            os.path.dirname(__file__),
            '..',
            '..',
            'db_credentials.json',
        )
        self.config_path = os.path.join(
            os.path.dirname(__file__),
            '..',
            '..',
            'config_tif_id.json',
        )

    def tearDown(self):
        """
        Rollback database session.
        Tear down temporary folder and file structure, stop moto mock
        """
        super().tearDown()
        TempDirectory.cleanup_all()
        self.assertFalse(os.path.isdir(self.temp_path))
        self.mock.stop()

    def test_parse_args(self):
        with patch('argparse._sys.argv',
                   ['python',
                    '--csv', self.csv_path,
                    '--login', 'test_login.json',
                    '--config', 'test_config.json',
                    '--nbr_workers', '5']):
            parsed_args = data_uploader.parse_args()
            self.assertEqual(parsed_args.csv, self.csv_path)
            self.assertEqual(parsed_args.login, 'test_login.json')
            self.assertEqual(parsed_args.config, 'test_config.json')
            self.assertFalse(parsed_args.override)
            self.assertEqual(parsed_args.nbr_workers, 5)

    # # @patch('imaging_db.database.db_operations.session_scope')
    # def test_upload_data(self):
    #     # mock_session().return_value = self.session
    #     # print(self.session)
    #     # print(mock_session)
    #     args = argparse.Namespace(
    #         csv=self.csv_path,
    #         login=self.credentials_path,
    #         config=self.config_path,
    #     )
    #     data_uploader.upload_data_and_update_db(args)
