import json
import jsonschema
import nose.tools
import numpy as np
import os
from testfixtures import TempDirectory
import tifffile

import imaging_db.metadata.json_operations as json_ops


def test_validate_schema():
    micrometa_json = {
        "MicroManagerMetadata": {
            "ChannelIndex": 4,
            "Slice": 1,
            "FrameIndex": 0,
            "Exposure-ms": 50,
            "COM1-DataBits": '8',
            "Channel": 'test_channel',
            "PositionIndex": 7
        }
    }
    json_ops.validate_schema(
        micrometa_json,
        schema="MICROMETA_SCHEMA")


@nose.tools.raises(jsonschema.exceptions.ValidationError)
def test_invalid_schema_credentials():
    invalid_json = {
        "drivername": "postgres",
        "username": "user",
        "password": "pwd",
        "host": "db_host",
        "port": "notanumber",
        "dbname": "db_name"
    }
    json_ops.validate_schema(
        invalid_json,
        schema="CREDENTIALS_SCHEMA")


@nose.tools.raises(jsonschema.exceptions.ValidationError)
def test_invalid_schema_micrometa():
    micrometa_json = {
        "MicroManagerMetadata": {
            "ChannelIndex": 4,
            'COM1-DataBits': '8'
        }
    }
    json_ops.validate_schema(
        micrometa_json,
        schema="MICROMETA_SCHEMA")


@nose.tools.raises(KeyError)
def test_validate_not_a_schema():
    json_obj = {
        "drivername": "postgres",
        "username": "user"
    }
    json_ops.validate_schema(
        json_obj,
        schema="NOTA_SCHEMA")


@nose.tools.raises(AssertionError)
def test_validate_bad_schema():
    json_obj = {
        "drivername": "postgres",
        "username": "user"
    }
    json_ops.validate_schema(
        json_obj,
        schema=3)


def test_read_json_file():
    with TempDirectory() as tempdir:
        valid_json = {
            "drivername": "postgres",
            "username": "user",
            "password": "pwd",
            "host": "db_host",
            "port": 666,
            "dbname": "db_name"
        }
        tempdir.write('valid_json_file.json', json.dumps(valid_json).encode())
        json_object = json_ops.read_json_file(
            os.path.join(tempdir.path, "valid_json_file.json"),
            schema_name="CREDENTIALS_SCHEMA")
        nose.tools.assert_equal(json_object, valid_json)


@nose.tools.raises(ValueError)
def test_read_not_a_json_file():
    with TempDirectory() as tempdir:
        invalid_json = {
            "drivername": "postgres",
            "username": "user"
        }
        # Remove last bracket
        invalid_json_str = json.dumps(invalid_json)[:-1]
        tempdir.write('invalid_json_file.json', invalid_json_str.encode())
        json_object = json_ops.read_json_file(
            os.path.join(tempdir.path, "invalid_json_file.json"),
        )


def test_write_json_file():
    with TempDirectory() as tempdir:
        valid_json = {
            "drivername": "postgres",
            "username": "user",
            "password": "pwd",
            "host": "db_host",
            "port": 666,
            "dbname": "db_name"
        }
        json_ops.write_json_file(
            valid_json,
            os.path.join(tempdir.path, 'valid_json_file.json'),
        )
        json_object = json_ops.read_json_file(
            os.path.join(tempdir.path, "valid_json_file.json"),
            schema_name="CREDENTIALS_SCHEMA")
        nose.tools.assert_equal(json_object, valid_json)


@nose.tools.raises(FileNotFoundError)
def test_read_nonexisting_json_file():
    json_ops.read_json_file("not_a_json_file.json")


def test_str2json():
    expected_json = {
        "ChannelIndex": 4,
        'COM1-DataBits': '8'
    }
    json_obj = json_ops.str2json(json.dumps(expected_json))
    nose.tools.assert_dict_equal(json_obj, expected_json)


@nose.tools.raises(ValueError)
def test_str2json_bad_json():
    json_ops.str2json('This is not a json')


def test_get_metadata_from_tags():
    test_schema = {
        "type": "object",
        "properties": {
            "MicroManagerMetadata": {
                "type": "object",
                "properties": {
                    "ChannelIndex": {
                        "type": "integer"
                    },
                    "Slice": {
                        "type": "integer"
                    },
                },
                "required": ["ChannelIndex"]
            }
        },
        "required": ["MicroManagerMetadata"]
    }
    with TempDirectory() as tempdir:
        mmmetadata = json.dumps({
            "ChannelIndex": 10,
            "Slice": 20,
            "FrameIndex": 30,
        })
        # Save test ome tif file
        im_path = os.path.join(tempdir.path, 'test_im.tiff')
        tifffile.imsave(
            im_path,
            np.zeros((10, 15)),
            extratags=[('MicroManagerMetadata', 's', 0, mmmetadata, True)],
        )
        im = tifffile.TiffFile(im_path)
        json_dict, required_dict = json_ops.get_metadata_from_tags(
            page=im.pages[0],
            meta_schema=test_schema,
        )
        expected_dict = {'MicroManagerMetadata':
                             {'ChannelIndex': 10,
                              'Slice': 20,
                              'FrameIndex': 30}
                         }
        nose.tools.assert_dict_equal(json_dict, expected_dict)
        nose.tools.assert_dict_equal(required_dict, {'ChannelIndex': 10})


def test_get_global_json():
    with TempDirectory() as tempdir:
        ijmeta = {
            "Info": json.dumps({"InitialPositionList":
                                [{"Label": "Pos1"}, {"Label": "Pos5"}]}),
        }
        # Save test ome tif file
        im_path = os.path.join(tempdir.path, 'test_im.tiff')
        tifffile.imsave(
            im_path,
            np.zeros((10, 15)),
            ijmetadata=ijmeta,
        )
        im = tifffile.TiffFile(im_path)
        file_name = 'test_im.tiff'
        global_json = json_ops.get_global_json(im.pages[0], file_name)
        expected_json = {
            'file_origin': file_name,
            'IJMetadata': {
                "InitialPositionList": [{"Label": "Pos1"}, {"Label": "Pos5"}]
            }
        }
        nose.tools.assert_dict_equal(global_json, expected_json)


@nose.tools.raises(ValueError)
def test_get_global_json_no_page():
    json_ops.get_global_json([], 'file_name')
