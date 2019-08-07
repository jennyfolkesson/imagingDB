#!/usr/bin/python

import argparse

import imaging_db.database.db_operations as db_ops
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
    parser.add_argument(
        '--project_id',
        type=str,
        default=None,
        help="Project ID substring (first part of dataset ID)",
    )
    parser.add_argument(
        '--microscope',
        type=str,
        default=None,
        help="Substring of microscope column",
    )
    parser.add_argument(
        '--start_date',
        type=str,
        default=None,
        help="Find >= dates in date_time column",
    )
    parser.add_argument(
        '--end_date',
        type=str,
        default=None,
        help="Find <= dates in date_time column",
    )
    parser.add_argument(
        '--description',
        type=str,
        default=None,
        help="Find substring in description column",
    )
    return parser.parse_args()


def query_data(login,
               project_id=None,
               microscope=None,
               start_date=None,
               end_date=None,
               description=None):
    """
    Provide CLI access to wrappers for common queries.
    Prints the dataset IDs of the datasets returned from the query to the
    standard output device.

    :param str login: Full path to json file containing database login
            credentials
    :param str project_id: First part of dataset_serial containing
            project ID (e.g. ML)
    :param str microscope: Microscope column
    :param str start_date: Find >= dates in date_time column
    :param str end_date: Find <= dates in date_time column
    :param str description: Find substring in description column
    """
    # Get database connection URI
    db_connection = db_utils.get_connection_str(login)

    search_dict = {}
    if project_id is not None:
        search_dict['project_id'] = project_id
    if microscope is not None:
        search_dict['microscope'] = microscope
    if start_date is not None:
        search_dict['start_date'] = start_date
    if end_date is not None:
        search_dict['end_date'] = end_date
    if description is not None:
        search_dict['description'] = description

    with db_ops.session_scope(db_connection) as session:
        datasets = db_ops.get_datasets(session, search_dict)
        print("Number of datasets matching your query: {}".format(len(datasets)))
        for i, d in enumerate(datasets):
            print(i, d.dataset_serial)


if __name__ == '__main__':
    args = parse_args()
    query_data(
        login=args.login,
        project_id=args.project_id,
        microscope=args.microscope,
        start_date=args.start_date,
        end_date=args.end_date,
        description=args.description,
    )
