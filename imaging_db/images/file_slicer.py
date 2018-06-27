import numpy as np
import pandas as pd
import pims

import imaging_db.metadata.json_validator as json_validator

MICROMETA_NAMES = ["ChannelIndex",
                   "Slice",
                   "FrameIndex",
                   "Exposure-ms",
                   "XResolution",
                   "YResolution",
                   "ResolutionUnit",
                   "FileName"]


def read_ome_tiff(file_name, file_format=".png", int2str_len=3):
    """
    reads ome.tiff file into memory and separates image files and metadata.

    :param str file_name: full path to file
    :param str file_format: file format for image name
    :param int int2str_len: format file name using ints converted to specific
        string length
    :return np.array im_stack: image stack
    :return pd.DataFrame metadata: associated metadata for each slice
    """
    frames = pims.TiffStack(file_name)
    # Get global metadata
    frame_shape = frames.frame_shape
    global_metadata = {
        "nbr_frames": len(frames),
        "im_width": frame_shape[0],
        "im_height": frame_shape[1],
        "bit_depth": str(frames.pixel_type),
    }
    # Create image stack with image bit depth 16 or 8
    im_stack = np.empty((frame_shape[0],
                         frame_shape[0],
                         global_metadata["nbr_frames"]),
                        dtype=frames.pixel_type)

    # Convert frames to numpy stack and collect metadata
    # Separate structure metadata (with known fields)
    # from unstructured, which goes in last column
    metadata = pd.DataFrame(
        index=range(global_metadata["nbr_frames"]),
        columns=MICROMETA_NAMES)
    # Pandas doesn't really support inserting dicts into dataframes,
    # so micromanager metadata goes into a separate list
    micromanager_meta = []
    for i, im_nbr in enumerate(range(global_metadata["nbr_frames"])):
        frame = frames._tiff[im_nbr]
        im_stack[:, :, im_nbr] = frame.asarray()
        # Starting with just MicroManager meta for now
        # it seems to contain the juciest data
        mm_meta = frame.tags["MicroManagerMetadata"].value
        # Validate metadata
        json_validator.validate_schema(mm_meta, "MICROMETA_SCHEMA")
        # Add structured metadata and Micromanager metadata to dataframe
        micromanager_meta.append(mm_meta)
        # NOTE: Rewrite in a less confusing way
        # But this will probably be in flux for a while...
        # First four names come from micrometa
        for meta_name in MICROMETA_NAMES[:4]:
            metadata.loc[i, meta_name] = mm_meta[meta_name]
        # Last three are from other tags
        # XResolution, YResolution, ResolutionUnit
        metadata.loc[i, "XResolution"] = \
            frame.tags["XResolution"].value[0]
        metadata.loc[i, "YResolution"] = \
            frame.tags["YResolution"].value[0]
        metadata.loc[i, "ResolutionUnit"] = \
            str(frame.tags["ResolutionUnit"].value)
        # Create a file name and add it
        im_name = "im_c" + \
            str(metadata.loc[i, "ChannelIndex"]).zfill(int2str_len) + \
            "_z" + str(metadata.loc[i, "Slice"]).zfill(int2str_len) + \
            "_t" + str(metadata.loc[i, "FrameIndex"]).zfill(int2str_len) + \
            file_format
        metadata.loc[i, "FileName"] = im_name
    return im_stack, metadata, micromanager_meta, global_metadata









