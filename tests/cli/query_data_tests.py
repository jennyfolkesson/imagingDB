from contextlib import contextmanager
import nose.tools
import os
import sys
from io import StringIO
from unittest.mock import patch

import imaging_db.database.db_operations as db_ops
import imaging_db.cli.query_data as query_data
import tests.database.db_basetest as db_basetest


@contextmanager
def captured_output():
    """
    Context manager that captures stdout and potential errors.
    https://stackoverflow.com/questions/4219717/
    how-to-assert-output-with-nosetest-unittest-in-python

    :return str sys.stdout: Console output
    :return str sys.stderr: Errors
    """
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class TestQueryData(db_basetest.DBBaseTest):
    """
    Test the data query tool
    """
    def setUp(self):
        super().setUp()
        # Database credentials file
        self.credentials_path = os.path.join(
            os.path.dirname(__file__),
            '..',
            '..',
            'db_credentials.json',
        )
        # Add some datasets to session
        self.dataset_ids = [
            'PROJECT-2010-04-01-00-00-00-0001',
            'PROJECT-2010-05-01-00-00-00-0001',
            'PROJECT-2010-06-01-00-00-00-0001',
            'MEOW-2010-05-05-00-00-00-0001',
            'MEOW-2010-06-05-00-00-00-0001',
        ]
        self.descriptions = [
            'First dataset',
            'Second dataset',
            'Third dataset',
            'Very specific description',
            'Not informative description',
        ]
        self.microscopes = [
            'scope1',
            'scope2',
            'scope2',
            'scope2',
            'other microscope',
        ]
        for i in range(len(self.dataset_ids)):
            new_dataset = db_ops.DataSet(
                dataset_serial=self.dataset_ids[i],
                description=self.descriptions[i],
                frames=True,
                microscope=self.microscopes[i],
                parent_id=None,
            )
            self.session.add(new_dataset)

    def tearDown(self):
        """
        Rollback database session.
        """
        super().tearDown()

    def test_parse_args(self):
        with patch('argparse._sys.argv',
                   ['python',
                    '--login', 'test_login.json',
                    '--project_id', 'TEST',
                    '--microscope', 'scope 1',
                    '--start_date', '2010-05-01',
                    '--description', 'This is a test']):
            parsed_args = query_data.parse_args()
            self.assertEqual(parsed_args.login, 'test_login.json')
            self.assertEqual(parsed_args.project_id, 'TEST')
            self.assertEqual(parsed_args.microscope, 'scope 1')
            self.assertEqual(parsed_args.start_date, '2010-05-01')
            self.assertIsNone(parsed_args.end_date)
            self.assertEqual(parsed_args.description, 'This is a test')

    @patch('imaging_db.database.db_operations.session_scope')
    def test_query_data(self, mock_session):
        mock_session.return_value.__enter__.return_value = self.session
        with captured_output() as (out, err):
            query_data.query_data(
                login=self.credentials_path,
                project_id='MEOW',
            )
        std_output = out.getvalue().strip()
        self.assertEqual(
            std_output,
            "Number of datasets matching your query: 2\n" +
            "0 MEOW-2010-05-05-00-00-00-0001\n" +
            "1 MEOW-2010-06-05-00-00-00-0001",
        )

    @patch('imaging_db.database.db_operations.session_scope')
    def test_query_data_dates(self, mock_session):
        mock_session.return_value.__enter__.return_value = self.session
        with captured_output() as (out, err):
            query_data.query_data(
                login=self.credentials_path,
                start_date='2010-05-01',
                end_date='2010-06-15',
            )
        std_output = out.getvalue().strip()
        self.assertEqual(
            std_output,
            "Number of datasets matching your query: 4\n" +
            "0 MEOW-2010-05-05-00-00-00-0001\n" +
            "1 MEOW-2010-06-05-00-00-00-0001\n" +
            "2 PROJECT-2010-05-01-00-00-00-0001\n" +
            "3 PROJECT-2010-06-01-00-00-00-0001",
        )

    @patch('imaging_db.database.db_operations.session_scope')
    def test_query_data_scope_project(self, mock_session):
        mock_session.return_value.__enter__.return_value = self.session
        with captured_output() as (out, err):
            query_data.query_data(
                login=self.credentials_path,
                project_id='MEOW',
                microscope='scope2',
            )
        std_output = out.getvalue().strip()
        self.assertEqual(
            std_output,
            "Number of datasets matching your query: 1\n" +
            "0 MEOW-2010-05-05-00-00-00-0001",
        )

    @patch('imaging_db.database.db_operations.session_scope')
    def test_query_data_description(self, mock_session):
        mock_session.return_value.__enter__.return_value = self.session
        with captured_output() as (out, err):
            query_data.query_data(
                login=self.credentials_path,
                description='Second',
            )
        std_output = out.getvalue().strip()
        self.assertEqual(
            std_output,
            "Number of datasets matching your query: 1\n" +
            "0 PROJECT-2010-05-01-00-00-00-0001",
        )

    @nose.tools.raises(AssertionError)
    @patch('imaging_db.database.db_operations.session_scope')
    def test_query_data_description(self, mock_session):
        mock_session.return_value.__enter__.return_value = self.session
        query_data.query_data(
            login=self.credentials_path,
            start_date='2010-06-01',
            end_date='2010-05-01',
        )
