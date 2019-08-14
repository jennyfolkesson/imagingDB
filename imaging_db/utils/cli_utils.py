import datetime


def validate_id(id_str):
    """
    Assert that the ID follows the naming convention
    <ID>-YYYY-MM-DD-HH-MM-SS-<SSSS>.
    ID is a 2-3 letter project ID. Currently supported:
    ISP for In Situ Transcriptomics
    ML for ?
    SSSS is a 4 digit serial number

    :param str id_str: ID string
    :param bool check_letters: Check the initial letters in the dataset
        serial (these may vary)
    """
    substrs = id_str.split("-")
    assert len(substrs) == 8, \
        "ID should have format <ID>-YYYY-MM-DD-HH-MM-SS-<SSSS>"
    assert len(substrs[1]) == 4, \
        "Year should consist of 4 letters, not {}".format(substrs[1])
    units = ["Month", "Day", "Hour", "Minute", "Second"]
    for t in range(2, 7):
        assert len(substrs[t]) == 2, \
            "{} should consist of 2 letters, not {}".format(units[t - 2],
                                                            substrs[t])
    assert 1 <= int(substrs[2]) <= 12, \
        "Month should be 1-12, {}".format(substrs[2])
    assert 1 <= int(substrs[3]) <= 31, \
        "Day should be 1-31, {}".format(substrs[3])
    assert 0 <= int(substrs[4]) <= 23, \
        "Hour should be 0-23, {}".format(substrs[4])
    assert 0 <= int(substrs[5]) <= 59, \
        "Minute should be 0-59, {}".format(substrs[5])
    assert 0 <= int(substrs[6]) <= 59, \
        "Second should be 0-59, {}".format(substrs[6])
    # NOTE: Should I also check that time is not from future?
    assert len(substrs[7]) == 4, \
        "Serial number should be 4 digits, {}".format(substrs[7])
    assert 0 <= int(substrs[7]) <= 9999,\
        "Serial number should be 4 integers {}".format(substrs[7])


def validate_date(date_str):
    """
    Make sure string is a valid date.

    :param str date_str: Date in the format YYYY-MM-DD
    :raises ValueError: If string is not a valid date
    """
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def assert_date_order(start_date, end_date):
    """
    Asserts that end date comes after start date.

    :param str start_date: Start date in the format YYYY-MM-DD
    :param str end_date: End date in the format YYYY-MM-DD
    :return bool: True if end date is after start date
    """
    start_datetime = validate_date(start_date)
    end_datetime = validate_date(end_date)
    assert end_datetime > start_datetime,\
        "End date {} must come after start date {}".format(end_date, start_date)
