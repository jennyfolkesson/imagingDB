import nose.tools

import imaging_db.utils.cli_utils as cli_utils


def test_validate_id():
    id_str = "ISP-2018-06-08-15-45-00-0001"
    cli_utils.validate_id(id_str)


@nose.tools.raises(AssertionError)
def test_too_short():
    id_str = "ISP-2018-06-08-15-45-0001"
    cli_utils.validate_id(id_str)


@nose.tools.raises(AssertionError)
def test_invalid_month():
    id_str = "ISP-2018-50-08-15-45-00-0001"
    cli_utils.validate_id(id_str)


@nose.tools.raises(AssertionError)
def test_invalid_year():
    id_str = "ISP-666-12-08-15-45-00-0001"
    cli_utils.validate_id(id_str)


@nose.tools.raises(AssertionError)
def test_invalid_serial():
    id_str = "ISP-666-12-08-15-45-00-A"
    cli_utils.validate_id(id_str)


@nose.tools.raises(AssertionError)
def test_underscore():
    id_str = "ISP_666-12-08-15-45-00-0001"
    cli_utils.validate_id(id_str)


def test_validate_date():
    date = cli_utils.validate_date('2019-05-07')
    nose.tools.assert_equal(date.year, 2019)
    nose.tools.assert_equal(date.month, 5)
    nose.tools.assert_equal(date.day, 7)


@nose.tools.raises(ValueError)
def test_validate_date_nosuchdate():
    cli_utils.validate_date('2019-55-77')


@nose.tools.raises(ValueError)
def test_validate_date_wrong_format():
    cli_utils.validate_date('19-01-01')


def test_assert_date_order():
    cli_utils.assert_date_order('2018-05-17', '2018-06-15')


@nose.tools.raises(AssertionError)
def test_assert_date_order_wrong():
    cli_utils.assert_date_order('2018-06-17', '2018-06-15')
