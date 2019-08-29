# coding=utf-8

from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, backref

from imaging_db.database.base import Base


class FramesGlobal(Base):
    """
    Table for files that have been decomposed into 2D slices
    """
    __tablename__ = 'frames_global'

    id = Column(Integer, primary_key=True)
    nbr_frames = Column(Integer)
    im_width = Column(Integer)
    im_height = Column(Integer)
    nbr_slices = Column(Integer)
    nbr_channels = Column(Integer)
    im_colors = Column(Integer)
    nbr_timepoints = Column(Integer)
    nbr_positions = Column(Integer)
    bit_depth = Column(String)
    s3_dir = Column(String)
    # Add potential to throw whatever metadata they want
    # in a JSONB object
    metadata_json = Column(JSONB)
    # Map project id
    dataset_id = Column(Integer, ForeignKey('data_set.id'))
    # Provide one to one mapping with dataset
    data_set = relationship(
        "DataSet",
        backref=backref("frames_global", uselist=False),
    )

    def __init__(self,
                 s3_dir,
                 nbr_frames,
                 im_width,
                 im_height,
                 nbr_slices,
                 nbr_channels,
                 nbr_timepoints,
                 nbr_positions,
                 im_colors,
                 bit_depth,
                 metadata_json,
                 data_set):
        self.s3_dir = s3_dir
        self.nbr_frames = nbr_frames
        self.im_width = im_width
        self.im_height = im_height
        self.nbr_slices = nbr_slices
        self.nbr_channels = nbr_channels
        self.nbr_timepoints = nbr_timepoints
        self.nbr_positions = nbr_positions
        self.im_colors = im_colors
        self.bit_depth = bit_depth
        self.metadata_json = metadata_json
        self.data_set = data_set
