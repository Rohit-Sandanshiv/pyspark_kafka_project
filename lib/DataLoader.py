def get_account_schema():
    schema = """load_date date,active_ind int,account_id string,
        source_sys string,account_start_date timestamp,
        legal_title_1 string,legal_title_2 string,
        tax_id_type string,tax_id string,branch_code string,country string"""
    return schema


def get_party_schema():
    schema = """load_date date,account_id string,party_id string,
    relation_type string,relation_start_date timestamp"""
    return schema


def get_address_schema():
    schema = """load_date date,party_id string,address_line_1 string,
    address_line_2 string,city string,postal_code string,
    country_of_address string,address_start_date date"""
    return schema


def read_accounts(spark):
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_account_schema()) \
        .load("test_data/accounts/")


def read_parties(spark):
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_party_schema()) \
        .load("test_data/parties/")


def read_address(spark):
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_address_schema()) \
        .load("test_data/party_address/")
