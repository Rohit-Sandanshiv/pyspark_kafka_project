import pytest
from lib.utils import getSpark_Session, printSparkConfigLoader
from lib.DataLoader import read_accounts, read_parties
from datetime import datetime, date
from lib.transformation import *
from chispa import assert_df_equality

from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row


@pytest.fixture(scope="session")
def spark():
    return getSpark_Session("DEV")


@pytest.fixture(scope='session')
def expected_party_rows():
    return [Row(load_date=date(2022, 8, 2), account_id='6982391060',
                party_id='9823462810', relation_type='F-N', relation_start_date=datetime(2019, 7, 29, 6, 21, 32)),
            Row(load_date=date(2022, 8, 2), account_id='6982391061', party_id='9823462811', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 31, 5, 27, 22)),
            Row(load_date=date(2022, 8, 2), account_id='6982391062', party_id='9823462812', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 25, 15, 50, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391063', party_id='9823462813', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 11, 7, 23, 28)),
            Row(load_date=date(2022, 8, 2), account_id='6982391064', party_id='9823462814', relation_type='F-N',
                relation_start_date=datetime(2019, 6, 6, 14, 18, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391065', party_id='9823462815', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 4, 5, 12, 37)),
            Row(load_date=date(2022, 8, 2), account_id='6982391066', party_id='9823462816', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 15, 10, 39, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462817', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 16, 9, 53, 4)),
            Row(load_date=date(2022, 8, 2), account_id='6982391068', party_id='9823462818', relation_type='F-N',
                relation_start_date=datetime(2017, 11, 27, 1, 20, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462820', relation_type='F-S',
                relation_start_date=datetime(2017, 11, 20, 14, 18, 5)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462821', relation_type='F-S',
                relation_start_date=datetime(2018, 7, 19, 18, 56, 57))]


@pytest.fixture(scope='session')
def expected_account_df(spark):
    schema = StructType([StructField('account_id', StringType()),
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])

    return spark.read.format("json").schema(schema).load("test_data/results/contract_df.json")


def test_SparkVersion(spark):
    assert spark.version == "3.5.4"


def test_config():
    config_dev = printSparkConfigLoader("DEV")
    config_qa = printSparkConfigLoader("QA")
    assert config_dev["spark.app.name"] == "sbdl_dev"
    assert config_qa["spark.app.name"] == "sbdl_qa"


def test_read_accounts(spark):
    df = read_accounts(spark)
    assert df.count() == 9


def test_party_full_details(spark, expected_party_rows):
    actual_party_rows = read_parties(spark).collect()
    assert expected_party_rows == actual_party_rows
    # assert actual_party_rows.count() == 11


def test_account_transformed_df(spark, expected_account_df):
    df = read_accounts(spark)
    actual_accounts_row = get_account_transformed_df(df)
    # assert actual_accounts_row.collect() == expected_account_df.collect()
    assert_df_equality(actual_accounts_row, expected_account_df, ignore_schema = True)