from contextlib import contextmanager
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from imaging_db.database.base import Base
from imaging_db.database.dataset import DataSet
from imaging_db.database.file_global import FileGlobal
from imaging_db.database.frames import Frames
from imaging_db.database.frames_global import FramesGlobal
import imaging_db.utils.meta_utils as meta_utils


@contextmanager
def session_scope(credentials_str, echo_sql=False):
    """
    Provide a transactional scope around a series of
    database operations.

    :param str credentials_str: URI for connecting to the database
    :param bool echo_sql: If true will print all generated SQL code
    :return SQLAlchemy session
    """
    # Create SQLAlchemy engine, connect and return session
    engine = create_engine(credentials_str, echo=echo_sql)
    # create a configured "Session" class
    Session = sessionmaker(bind=engine)
    # Generate database schema
    Base.metadata.create_all(engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise Exception(e)
    finally:
        session.close()


def test_connection(session):
    """
    Test that you can connect to the database

    :raise Exception: if you can't log in
    """
    try:
        session.execute('SELECT 1')
    except Exception as e:
        raise ConnectionError("Can't connect to database", e)


def get_datasets(session, search_dict=None):
    """
    Given dataset search criteria such as project ID, start/end date etc,
    query database and return datasets that match the criteria.

    :param session: SQLAlchemy session
    :param dict search_dict: Dictionary with query criteria. Currently available
        search keys are:
        project_id: First part of dataset_serial containing project ID (e.g. ML)
        microscope: Microscope column
        start_date: Find >= dates in date_time column
        end_date: Find <= dates in date_time column
        description: Find substring in description column
    :return list datasets: DataSets that match the query
    """
    datasets = session.query(DataSet) \
        .order_by(DataSet.dataset_serial)
    if 'project_id' in search_dict:
        datasets = datasets.filter(
            DataSet.dataset_serial.contains(search_dict['project_id']),
        )
    if 'microscope' in search_dict:
        datasets = datasets.filter(
            DataSet.microscope.contains(search_dict['microscope']),
        )
    if 'start_date' in search_dict:
        datasets = datasets.filter(
            DataSet.date_time >= search_dict['start_date'],
        )
    if 'end_date' in search_dict:
        datasets = datasets.filter(
            DataSet.date_time <= search_dict['end_date'],
        )
    if 'description' in search_dict:
        datasets = datasets.filter(
            DataSet.description.contains(search_dict['description']),
        )
    return datasets.all()


class DatabaseOperations:
    """
    Class handling standard input and output database operations for a
    specific dataset given an ID.
    """

    def __init__(self, dataset_serial):
        """
        :param dataset_serial: Unique dataset identifier
        """
        self.dataset_serial = dataset_serial

    def assert_unique_id(self, session):
        """
        Make sure dataset is not already in database using assertion.

        :param session: SQLAlchemy session
        """
        # Check if ID already exist
        datasets = session.query(DataSet) \
            .filter(DataSet.dataset_serial == self.dataset_serial).all()
        assert len(datasets) == 0, \
            "Dataset {} already exists in database".format(
                self.dataset_serial,
            )

    def _get_parent(self, session, parent_dataset):
        """
        Find parent key if a parent dataset serial is given.

        :param session: SQLAlchemy session
        :param parent_dataset: parent dataset serial
        :return int parent_key: primary key of parent dataset
        """
        parent_key = None
        if isinstance(parent_dataset, type(None)):
            return parent_key
        if isinstance(parent_dataset, str):
            if parent_dataset.lower() == "none" or len(parent_dataset) == 0:
                parent_dataset = None
        # Check for nan regardless of type
        elif parent_dataset != parent_dataset:
                parent_dataset = None
        if parent_dataset is not None:
            try:
                parent = session.query(DataSet) \
                    .filter(DataSet.dataset_serial == parent_dataset).one()
                parent_key = parent.id
            except Exception as e:
                error_str = "Parent {} not found for data set {}. {}".format(
                    parent_dataset,
                    self.dataset_serial,
                    e,
                )
                raise ValueError(error_str)
        return parent_key

    def insert_frames(self,
                      session,
                      description,
                      frames_meta,
                      frames_json_meta,
                      global_meta,
                      global_json_meta,
                      microscope,
                      parent_dataset=None):
        """
        Insert global and local information from file that has been
        converted to image slices with corresponding metadata

        :param session: SQLAlchemy session
        :param str description: Short description of file
        :param dataframe frames_meta: Dataframe containing mandatory fields for
                each frame
        :param list frames_json_meta: List of json objects with arbitrary
                local metadata
        :param dict global_meta: Required global metadata fields
        :param dict global_json_meta: Arbitrary global metadata that can be
                converted into JSONB format
        :param str microscope: microscope name
        :param str/None parent_dataset: Assign parent if not none
        """
        # Check if ID already exist
        datasets = session.query(DataSet) \
            .filter(DataSet.dataset_serial == self.dataset_serial).all()
        assert len(datasets) == 0, \
            "Dataset {} already exists in database".format(
                self.dataset_serial,
            )
        # If parent dataset identifier is given, find its key and insert it
        parent_key = self._get_parent(session, parent_dataset)
        # Insert dataset ID in the main DataSet table with frames=True
        new_dataset = DataSet(
            dataset_serial=self.dataset_serial,
            description=description,
            frames=True,
            microscope=microscope,
            parent_id=parent_key,
        )
        # Add global frame information
        new_frames_global = FramesGlobal(
            s3_dir=global_meta["storage_dir"],
            nbr_frames=global_meta["nbr_frames"],
            im_width=global_meta["im_width"],
            im_height=global_meta["im_height"],
            nbr_slices=global_meta["nbr_slices"],
            nbr_channels=global_meta["nbr_channels"],
            nbr_timepoints=global_meta["nbr_timepoints"],
            nbr_positions=global_meta["nbr_positions"],
            im_colors=global_meta["im_colors"],
            bit_depth=global_meta["bit_depth"],
            metadata_json=global_json_meta,
            data_set=new_dataset,
        )
        for i in range(frames_meta.shape[0]):
            # Insert all frames here then add them to new frames global
            new_frame = Frames(
                channel_idx=frames_meta.loc[i, "channel_idx"],
                slice_idx=frames_meta.loc[i, "slice_idx"],
                time_idx=frames_meta.loc[i, "time_idx"],
                pos_idx=frames_meta.loc[i, "pos_idx"],
                channel_name=frames_meta.loc[i, "channel_name"],
                file_name=frames_meta.loc[i, "file_name"],
                sha256=frames_meta.loc[i, "sha256"],
                metadata_json=frames_json_meta[i],
                frames_global=new_frames_global,
            )
            session.add(new_frame)

        session.add(new_dataset)
        session.add(new_frames_global)

    def insert_file(self,
                    session,
                    description,
                    storage_dir,
                    file_name,
                    global_json_meta,
                    microscope,
                    sha256,
                    parent_dataset=None):
        """
        Upload file as is without splitting it to frames or extracting metadata

        :param session: SQLAlchemy session
        :param str description: Short description of file
        :param str storage_dir: Directory where data is stored
        :param str file_name: Name of file
        :param global_json_meta: Arbitrary metadata fields for file
        :param str microscope: microscope name
        :param str sha256: sha256 checksum for file
        :param str parent_dataset: Assign parent if not null
        """
        # Check if ID already exist
        datasets = session.query(DataSet) \
            .filter(DataSet.dataset_serial == self.dataset_serial).all()
        assert len(datasets) == 0, \
            "Dataset {} already exists in database".format(
                self.dataset_serial,
            )
        # If parent dataset identifier is given, find its key and insert it
        parent_key = self._get_parent(session, parent_dataset)

        # First insert project ID in the main Project table
        # with frames=False
        new_dataset = DataSet(
            dataset_serial=self.dataset_serial,
            description=description,
            frames=False,
            microscope=microscope,
            parent_id=parent_key
        )
        # Add s3 location
        new_file_global = FileGlobal(
            s3_dir=storage_dir,
            file_name=file_name,
            metadata_json=global_json_meta,
            data_set=new_dataset,
            sha256=sha256,
        )
        session.add(new_dataset)
        session.add(new_file_global)

    def get_filenames(self,
                      session,
                      positions=None,
                      times=None,
                      channels=None,
                      slices=None):
        """
        Get S3 folder name and all file names associated with unique
        project identifier.

        :param session: SQLAlchemy session
        :param [None, tuple(int)] positions: a tuple containing position indices
                to be fetched use None to get all positions.
        :param [None, tuple(int)] times: a tuple containing time indices
                to be fetched use None to get all times.
        :param [None, tuple] channels: a tuple containing channels
                (use channel names (e.g., 'Cy3') or integer indices) to be fetched.
        :param [None, tuple] slices: a tuple containing slice indices to
                be fetched. Use None to get all slices.
        :return str storage_dir: Folder name containing file(s) in storage
        :return list of strs file_names: List of file names for given dataset
        """
        # Check if ID already exist
        dataset = session.query(DataSet) \
            .filter(DataSet.dataset_serial == self.dataset_serial).one()

        if dataset.frames is False:
            # Get file
            file_global = session.query(FileGlobal) \
                .join(DataSet) \
                .filter(DataSet.dataset_serial == dataset.dataset_serial) \
                .one()

            return file_global.s3_dir, [file_global.file_name]
        else:
            # Get frames
            all_frames = session.query(Frames) \
                .join(FramesGlobal) \
                .join(DataSet) \
                .filter(DataSet.dataset_serial == dataset.dataset_serial)

            sliced_frames = self._slice_frames(
                all_frames,
                positions=positions,
                times=times,
                channels=channels,
                slices=slices,
            )
            storage_dir = sliced_frames[0].frames_global.s3_dir

            file_names = []
            for f in sliced_frames:
                file_names.append(f.file_name)

            return storage_dir, file_names

    @staticmethod
    def _slice_frames(frames,
                      positions=None,
                      times=None,
                      channels=None,
                      slices=None):
        """
        Get specific slices of a set of Frames

        :param Query frames: all of the frames to be sliced
        :param [None, tuple(int)] positions: a tuple containing position indices
                to be fetched use None to get all positions.
        :param [None, tuple(int)] times: a tuple containing time indices
                to be fetched use None to get all times.
        :param [None, tuple] channels: a tuple containing channels
                (use channel names (e.g., 'Cy3') or integer indices) to be fetched.
        :param [None, tuple] slices: a tuple containing slice indices to
                be fetched. Use None to get all slices.
        :return Query sliced_frames: query that yields the requested frames
        :raises AssertionError: If illegal indices are given
        :raises ValueError: If an illegal type is given
        :raises AssertionError: If both channels and channel_ids are specified.
        """
        sliced_frames = frames

        # Filter by channel
        if channels is None:
            pass
        elif type(channels) is tuple:
            if np.all([isinstance(c, str) for c in channels]):
                # Query channel names
                sliced_frames = sliced_frames.filter(
                    Frames.channel_name.in_(channels),
                )
            elif np.all([isinstance(c, int) for c in channels]):
                # Query channel indices
                sliced_frames = sliced_frames.filter(
                    Frames.channel_idx.in_(channels),
                )
            else:
                raise ValueError('channels tuple must be either all str or',
                                 'int, not {}'.format(channels))
        else:
            raise ValueError('Invalid channel query')

        # Filter by slice
        if slices is None:
            pass
        elif type(slices) is tuple:
            sliced_frames = sliced_frames.filter(Frames.slice_idx.in_(slices))
        else:
            raise ValueError('Invalid slice query')

        # Filter by time
        if times is None:
            pass
        elif type(times) is tuple:
            sliced_frames = sliced_frames.filter(Frames.time_idx.in_(times))
        else:
            raise ValueError('Invalid time query')

        # Filter by position
        if positions is None:
            pass
        elif type(positions) is tuple:
            sliced_frames = sliced_frames.filter(Frames.pos_idx.in_(positions))
        else:
            raise ValueError('Invalid position query')

        assert sliced_frames.count() > 0,\
            'No frames matched the query'

        return sliced_frames

    @staticmethod
    def _get_meta_from_frames(frames):
        """
        Extract global meta as well as info for each frame given
        a frames query.

        :param list of Frames frames: Frames obtained from dataset query
        :return dict global_meta: Global metadata for dataset
        :return dataframe frames_meta: Metadata for each frame
        """
        # Collect global metadata that can be used to instantiate im_stack
        global_meta = {
            "storage_dir": frames[0].frames_global.s3_dir,
            "nbr_frames": frames[0].frames_global.nbr_frames,
            "im_width": frames[0].frames_global.im_width,
            "im_height": frames[0].frames_global.im_height,
            "nbr_slices": frames[0].frames_global.nbr_slices,
            "nbr_channels": frames[0].frames_global.nbr_channels,
            "im_colors": frames[0].frames_global.im_colors,
            "nbr_timepoints": frames[0].frames_global.nbr_timepoints,
            "nbr_positions": frames[0].frames_global.nbr_positions,
            "bit_depth": frames[0].frames_global.bit_depth,
        }
        meta_utils.validate_global_meta(global_meta)
        # Add global JSON metadata
        global_meta["metadata_json"] = frames[0].frames_global.metadata_json

        # Metadata that will be returned from the DB for each frame
        frames_meta = meta_utils.make_dataframe(
            nbr_frames=frames.count(),
        )
        for i, f in enumerate(frames):
            frames_meta.loc[i] = [
                f.channel_idx,
                f.slice_idx,
                f.time_idx,
                f.channel_name,
                f.file_name,
                f.pos_idx,
                f.sha256,
            ]
        return global_meta, frames_meta

    def get_frames_meta(self,
                        session,
                        positions=None,
                        times=None,
                        channels=None,
                        slices=None):
        """
        Get information for all frames in dataset associated with unique
        project identifier.

        :param session: SQLAlchemy session
        :param [None, tuple] positions: a tuple containing position indices to
                be fetched use None to get all positions.
        :param [None, tuple] times: a tuple containing time indices to be
                fetched use None to get all times.
        :param [None, tuple] channels: a tuple containing channels (use channel
                names e.g., 'Cy3', or integer indices) to befetched. Use None to
                get all channels.
        :param [None, tuple] slices: a tuple containing slice indices
                to be fetched use None to get all channels.
        :return dict global_meta: Global metadata for dataset
        :return dataframe frames_meta: Metadata for each frame
        """
        # Check if ID already exist
        dataset = session.query(DataSet) \
            .filter(DataSet.dataset_serial == self.dataset_serial).one()

        assert dataset.frames is True,\
            "This dataset has not been split into frames." \
            "Set metadata to False if downloading file"

        # Get frames in datset
        all_frames = session.query(Frames) \
            .join(FramesGlobal) \
            .join(DataSet) \
            .filter(DataSet.dataset_serial == dataset.dataset_serial) \
            .order_by(Frames.file_name)

        # Get the specified slices
        sliced_frames = self._slice_frames(
            all_frames,
            positions=positions,
            times=times,
            channels=channels,
            slices=slices,
        )
        # Get global and local metadata
        global_meta, frames_meta = self._get_meta_from_frames(
            sliced_frames)
        return global_meta, frames_meta
