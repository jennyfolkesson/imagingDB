import nose.tools
import os
from unittest.mock import patch

import imaging_db.utils.db_utils as db_utils
import tests.database.db_basetest as db_basetest


def test_json_to_uri():
    credentials_json = {
        "drivername": "postgres",
        "username": "user",
        "password": "pwd",
        "host": "db_host",
        "port": 666,
        "dbname": "db_name"
    }
    expected_str = "postgres://user:pwd@db_host:666/db_name"
    credentials_str = db_utils.json_to_uri(credentials_json)
    nose.tools.assert_equal(credentials_str, expected_str)


def test_get_connection_str():
    credentials_filename = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            '..',
            '..',
            'db_credentials.json',
        ),
    )
    credentials_str = db_utils.get_connection_str(credentials_filename)
    expected_str = "postgres://imaging_user:imaging_passwd@localhost:5433/imaging_test"
    nose.tools.assert_equal(credentials_str, expected_str)


class TestConnection(db_basetest.DBBaseTest):
    """
    Test the data connection
    """
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    @patch('imaging_db.database.db_operations.session_scope')
    def test_connection(self, mock_session):
        mock_session.return_value.__enter__.return_value = self.session
        connection_str = "postgres://imaging_user:imaging_passwd@localhost:5433/imaging_test"
        db_utils.check_connection(connection_str)
