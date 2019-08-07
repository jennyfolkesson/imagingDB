#!/usr/bin/python

import argparse
import os

import imaging_db.database.db_operations as db_ops
import imaging_db.metadata.json_operations as json_ops
import imaging_db.utils.cli_utils as cli_utils
import imaging_db.utils.db_utils as db_utils


def parse_args():
    """
    Parse command line arguments for CLI

    :return: namespace containing the arguments passed.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--login',
        type=str,
        required=True,
        help="Full path to file containing JSON with DB login credentials",
    )
    # TODO: This is not user friendly. Change it before PR!
    parser.add_argument(
        '--search_str',
        type=str,
        default=None,
        help="Search dict as string",
    )
    return parser.parse_args()


def query_data(login,
               search_dict=None,
               ):
    """
    Provide CLI access to wrappers for common queries.

    :param str login: Full path to json file containing database login
            credentials
    :param dict search_dict: Key/value pairs for dataset fields that should
            be queried over
    """
    # Get database connection URI
    db_connection = db_utils.get_connection_str(login)

    with db_ops.session_scope(db_connection) as session:
        datasets = db_ops.get_datasets(session, search_dict)
        for d in datasets:
            print(d.dataset_serial)


if __name__ == '__main__':
    args = parse_args()
    query_data(
        login=args.login,
        search_dict=args.search_str,
    )
