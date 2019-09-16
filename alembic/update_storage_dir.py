#!/usr/bin/python

import argparse
import os

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
        help="Full path to file containing JSON with DB login credentials",
    )
    return parser.parse_args()


def migrate_db(credentials_filename):
    """
    The mistake I made was to autogenerate a migration without inspection.
    If renaming a column name or table, Alembic will drop then add, removing
    all content.
    Version 8e0d2514fd1f_change_s3_to_storage_dir.py is an example of how
    NOT to do migrations.
    Always inspect your migration script and make sure alter_column or rename_table
    is used for name changes. Lesson learned...
    Updates storage_dir for all files and frames (can be recreated from
    dataset_serial)

    :param credentials_filename: Full path to DB credentials file
    """
    # Edit this depending on where your database credential file is stored
    # This assumes it's stored in dir above imagingDB
    credentials_str = db_utils.get_connection_str(
        credentials_filename=credentials_filename,
    )
    # Get files and update storage dir
    with db_ops.session_scope(credentials_str) as session:
        files = session.query(db_ops.FileGlobal)
        for file in files:
            if file.storage_dir is None:
                new_dir = os.path.join('raw_files', file.data_set.dataset_serial)
                file.storage_dir = new_dir

    # Get frames
    with db_ops.session_scope(credentials_str) as session:
        frames_global = session.query(db_ops.FramesGlobal)
        for frame_global in frames_global:
            if frame_global.storage_dir is None:
                new_dir = os.path.join(
                    'raw_frames',
                    frame_global.data_set.dataset_serial,
                )
                frame_global.storage_dir = new_dir


if __name__ == '__main__':
    args = parse_args()
    migrate_db(args.login)
