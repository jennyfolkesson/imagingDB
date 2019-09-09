#!/usr/bin/python

import argparse
import os

import imaging_db.database.db_operations as db_ops
import imaging_db.metadata.json_operations as json_ops
import imaging_db.utils.aux_utils as aux_utils
import imaging_db.utils.cli_utils as cli_utils
import imaging_db.utils.db_utils as db_utils


def parse_args():
    """
    Parse command line arguments for CLI

    :return: namespace containing the arguments passed.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--id',
        type=str,
        required=True,
        help="Unique dataset identifier",
    )
    parser.add_argument(
        '--login',
        type=str,
        required=True,
        help="Full path to file containing JSON with DB login credentials",
    )
    parser.add_argument(
        '--dest',
        type=str,
        required=True,
        help="Main destination directory, in which a subdir named args.id "
             "\will be created",
    )
    parser.add_argument(
        '--storage',
        type=str,
        default='local',
        choices=['s3', 'local'],
        help="Optional: Specify 'local' (default) or 's3'."
             "Uploads to local storage will be synced to S3 daily and vice versa.",
    )
    parser.add_argument(
        '--storage_access',
        type=str,
        default=None,
        help="If using a different storage than defaults, specify here."
             "Defaults are /Volumes/data_lg/czbiohub-imaging (mount point)"
             "for local storage, czbiohub-imaging (bucket name) for S3 storage."
    )
    parser.add_argument(
        '--nbr_workers',
        type=int,
        default=None,
        help="Number of threads to increase download speed"
    )
    parser.add_argument(
        '-p', '--positions',
        type=int,
        default=None,
        nargs='+',
        help="Tuple containing position indices to download",
    )
    parser.add_argument(
        '-t', '--times',
        type=int,
        default=None,
        nargs='+',
        help="Tuple containing time indices to download",
    )
    parser.add_argument(
        '-c', '--channels',
        type=str,
        default=None,
        nargs='+',
        help="Tuple containing the channel names or indices to download",
    )
    parser.add_argument(
        '-z', '--slices',
        type=int,
        default=None,
        nargs='+',
        help="Tuple containing the z slices to download",
    )
    parser.add_argument(
        '--no-metadata',
        dest='metadata',
        action='store_false',
        help="Don't write csvs with metadata (for datasets split into frames only)",
    )
    parser.set_defaults(metadata=True)
    parser.add_argument(
        '--no-download',
        dest='download',
        action='store_false',
        help="Don't download files (for datasets split into frames only)",
    )
    parser.set_defaults(download=True)
    return parser.parse_args()


def download_data(dataset_serial,
                  login,
                  dest,
                  storage='local',
                  storage_access=None,
                  metadata=True,
                  download=True,
                  nbr_workers=None,
                  positions=None,
                  times=None,
                  channels=None,
                  slices=None):
    """
    Find all files associated with unique project identifier and
    download them to a local directory.

    :param str dataset_serial: Unique dataset identifier
    :param str login: Full path to json file containing database login
                credentials
    :param str dest: Local destination directory name
    :param str storage: 'local' (default) - data will be stored locally and
                synced to S3 the same day. Or 'S3' - data will be uploaded
                directly to S3 then synced with local storage daily.
    :param str/None storage_access: If not using predefined storage locations,
                this parameter refers to mount_point for local storage and
                bucket_name for S3 storage.
    :param bool download: Downloads all files associated with dataset (default)
                If False, will only write csvs with metadata. Only for
                datasets split into frames
    :param bool metadata: Writes metadata (default True)
                global metadata in json, local for each frame in csv
    :param int, None nbr_workers: Number of workers for parallel download
                If None, it defaults to number of machine processors * 5
    :param list, None positions: Positions (FOVs) as integers (default
                None downloads all)
    :param list, None times: Timepoints as integers (default None downloads all)
    :param list, None channels: Channels as integer indices or strings for channel
                names (default None downloads all)
    :param list, None slices: Slice (z) integer indices (Default None downloads all)
    """
    try:
        cli_utils.validate_id(dataset_serial)
    except AssertionError as e:
        raise AssertionError("Invalid ID:", e)

    # Create output directory as a subdirectory in dest named
    # dataset_serial. It stops if the subdirectory already exists to avoid
    # the risk of overwriting existing data
    dest_dir = os.path.join(dest, dataset_serial)
    try:
        os.makedirs(dest_dir, exist_ok=False)
    except FileExistsError as e:
        raise FileExistsError(
            "Folder {} already exists, {}".format(dest_dir, e))

    # Get database connection URI
    db_connection = db_utils.get_connection_str(login)
    db_utils.check_connection(db_connection)

    # Instantiate database class
    db_inst = db_ops.DatabaseOperations(
        dataset_serial=dataset_serial,
    )
    # Import local or S3 storage class
    storage_class = aux_utils.get_storage_class(storage_type=storage)

    if metadata is False:
        # Just download file(s)
        assert download,\
            "You set metadata *and* download to False. You get nothing."
        with db_ops.session_scope(db_connection) as session:
            storage_dir, file_names = db_inst.get_filenames(
                session=session,
            )
    else:
        # Get all the slicing args and recast as tuples
        if positions is not None:
            positions = tuple(positions)
        if times is not None:
            times = tuple(times)
        if channels is not None:
            # If channels can be converted to ints, they're indices
            try:
                channels = [int(c) for c in channels]
            except ValueError:
                # Channels are names, not indices
                channels = channels
            channels = tuple(channels)
        if slices is not None:
            slices = tuple(slices)

        # Get the metadata from the requested frames
        with db_ops.session_scope(db_connection) as session:
            global_meta, frames_meta = db_inst.get_frames_meta(
                session=session,
                positions=positions,
                times=times,
                channels=channels,
                slices=slices,
            )
        # Write global metadata to destination directory
        global_meta_filename = os.path.join(
            dest_dir,
            "global_metadata.json",
        )
        json_ops.write_json_file(
            meta_dict=global_meta,
            json_filename=global_meta_filename,
        )
        # Write info for each frame to destination directory
        local_meta_filename = os.path.join(
            dest_dir,
            "frames_meta.csv",
        )
        frames_meta.to_csv(local_meta_filename, sep=",")
        # Extract folder and file names if we want to download
        storage_dir = global_meta["storage_dir"]
        file_names = frames_meta["file_name"]

    if download:
        if nbr_workers is not None:
            assert nbr_workers > 0,\
                "Nbr of worker must be >0, not {}".format(nbr_workers)
        data_loader = storage_class(
            storage_dir=storage_dir,
            nbr_workers=nbr_workers,
            access_point=storage_access,
        )
        data_loader.download_files(file_names, dest_dir)


if __name__ == '__main__':
    args = parse_args()
    download_data(
        dataset_serial=args.id,
        login=args.login,
        dest=args.dest,
        storage=args.storage,
        storage_access=args.storage_access,
        metadata=args.metadata,
        download=args.download,
        nbr_workers=args.nbr_workers,
        positions=args.positions,
        times=args.times,
        channels=args.channels,
        slices=args.slices,
    )
