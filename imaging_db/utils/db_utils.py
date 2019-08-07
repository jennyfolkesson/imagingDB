import imaging_db.database.db_operations as db_ops
import imaging_db.metadata.json_operations as json_ops


def json_to_uri(credentials_json):
    """
    Convert JSON object containing database credentials into a string
    formatted for SQLAlchemy's create_engine, e.g.:
    drivername://user:password@host:port/dbname
    It is assumed that the json has already been validated against
    json_ops.CREDENTIALS_SCHEMA

    :param json credentials_json: JSON object containing database credentials
    :return str credentials_str: URI for connecting to the database
    """
    return \
        credentials_json["drivername"] + "://" + \
        credentials_json["username"] + ":" + \
        credentials_json["password"] + "@" + \
        credentials_json["host"] + ":" + \
        str(credentials_json["port"]) + "/" + \
        credentials_json["dbname"]


def get_connection_str(credentials_filename):
    """
    Bundles the JSON read of the login credentials file with
    a conversion to a URI for connecting to the database

    :param credentials_filename: JSON file containing DB credentials
    :return str connection_str: URI for connecting to the DB
    """
    # Read and validate json
    credentials_json = json_ops.read_json_file(
        json_filename=credentials_filename,
        schema_name="CREDENTIALS_SCHEMA")
    # Convert json to string compatible with engine
    return json_to_uri(credentials_json)


def check_connection(db_connection):
    """
    Make sure you can connect to database before anything else.

    :param str db_connection: URI for connecting to the DB
    :raises IOError: If you can't connect to the DB
    """
    try:
        with db_ops.session_scope(db_connection) as session:
            db_ops.test_connection(session)
    except Exception as e:
        raise IOError("Can't connect to DB: {}".format(e))
