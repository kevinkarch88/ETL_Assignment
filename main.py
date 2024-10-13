import json
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import regexp_replace, col, lit, split, to_date, row_number, concat_ws, trim, current_timestamp, when, array
from pyspark.sql.types import StringType, TimestampType, IntegerType, BooleanType, DateType

# setup postgres jar and spark context
postgres_driver_path = "postgresql-42.7.4.jar"
spark = SparkSession.builder.appName("SparkPostgresETL").config("spark.jars", postgres_driver_path).getOrCreate()

# load json mapping file for each of the sources
with open('column_map.json', 'r') as file:
    column_mappings = json.load(file)

# target columns that need to be mapped to
target_columns = [
    'accepts_financial_aid',
    'ages_served',
    'capacity',
    'certificate_expiration_date',
    'city',
    'address1',
    'address2',
    'company',
    'phone',
    'phone2',
    'county',
    'curriculum_type',
    'email',
    'first_name',
    'language',
    'last_name',
    'license_status',
    'license_issued',
    'license_number',
    'license_renewed',
    'license_type',
    'licensee_name',
    'max_age',
    'min_age',
    'operator',
    'provider_id',
    'schedule',
    'state',
    'title',
    'website_address',
    'zip',
    'facility_type',
    'source',
    # added metadata columns
    'etl_load_time',
    'version_number',
    'source_file_name'
]

# types for postgres
column_types = {
    'accepts_financial_aid': BooleanType(),
    'ages_served': StringType(),
    'capacity': IntegerType(),
    'certificate_expiration_date': DateType(),
    'city': StringType(),
    'address1': StringType(),
    'address2': StringType(),
    'company': StringType(),
    'phone': StringType(),
    'phone2': StringType(),
    'county': StringType(),
    'curriculum_type': StringType(),
    'email': StringType(),
    'first_name': StringType(),
    'language': StringType(),
    'last_name': StringType(),
    'license_status': StringType(),
    'license_issued': DateType(),
    'license_number': IntegerType(),
    'license_renewed': DateType(),
    'license_type': StringType(),
    'licensee_name': StringType(),
    'max_age': IntegerType(),
    'min_age': IntegerType(),
    'operator': StringType(),
    'provider_id': StringType(),
    'schedule': StringType(),
    'state': StringType(),
    'title': StringType(),
    'website_address': StringType(),
    'zip': StringType(),
    'facility_type': StringType(),
    'source': StringType(),
    'etl_load_time': TimestampType(),
    'version_number': StringType(),
    'source_file_name': StringType()
}


# strip out non-numbers
def clean_phone(df, phone_col):
    return df.withColumn(
        phone_col,
        regexp_replace(col(phone_col), r'\D', '')
    )


def transform_df(df, mapping, source_name, source_file_name=None):
    # rename columns with mapping
    for src_col, tgt_col in mapping.items():
        if src_col in df.columns:
            df = df.withColumnRenamed(src_col, tgt_col)

    # put null in missing columns
    for col_name in target_columns:
        if col_name not in df.columns:
            data_type = column_types.get(col_name, StringType())
            df = df.withColumn(col_name, lit(None).cast(data_type))

    df = df.withColumn('source', lit(source_name).cast(StringType()))

    # add metadata columns
    df = df.withColumn('etl_load_time', current_timestamp().cast(TimestampType()))
    df = df.withColumn('version_number', lit('1.0').cast(StringType()))
    df = df.withColumn('source_file_name', lit(os.path.basename(source_file_name)).cast(StringType()))
    return df


# special case for 1st file
def combine_age_columns(df, age_columns):
    # combine age columns into 'ages_served' if they are not null
    age_cols = [col(c) for c in age_columns if c in df.columns]
    df = df.withColumn('ages_served', concat_ws(', ', *age_cols))
    # cleanup original age columns
    df = df.drop(*age_columns)
    return df


# special case for 3rd file
def combine_age_columns_source3(df, age_columns):
    age_exprs = []
    for age_col in age_columns:
        if age_col in df.columns:
            age_exprs.append(when(col(age_col) == 'Y', lit(age_col)))

    # age group handling
    df = df.withColumn('ages_array', array(*age_exprs))
    df = df.withColumn('ages_served', concat_ws(', ', col('ages_array')))
    df = df.drop(*age_columns, 'ages_array')
    return df


# main processor
def process_csv_file(file_path, source_name):
    df = spark.read.csv(
        file_path,
        header=True,
        multiLine=True,
        escape='"',
        quote='"',
        inferSchema=False,
        encoding='UTF-8'
    )
    mapping = column_mappings.get(source_name)
    if mapping is None:
        print(f"No mapping found for source_name: {source_name}. Skipping file.")
        return None

    # special case
    if source_name == 'source3':
        # Clean extra quotation marks in 'Operation Name' column
        df = df.withColumn('Operation Name', regexp_replace(col('Operation Name'), r'"', ''))

    df = transform_df(df, mapping, source_name, file_path)
    df = clean_phone(df, 'phone')

    # license name handling
    if 'licensee_name' in df.columns:
        df = df.withColumn('licensee_name', trim(col('licensee_name')))
        df = df.withColumn('name_parts', split(col('licensee_name'), ' '))
        df = df.withColumn('first_name', col('name_parts').getItem(0))
        df = df.withColumn('last_name', when(
            col('name_parts').getItem(1).isNotNull(),
            col('name_parts').getItem(1)
        ).otherwise(lit('')))
        df = df.drop('name_parts')

    # convert date columns to proper date format
    date_columns = ['license_issued', 'certificate_expiration_date']
    date_format = 'M/d/yy'  # Adjusted date format
    for date_col in date_columns:
        if date_col in df.columns:
            df = df.withColumn(date_col, to_date(col(date_col), date_format))

    # cast numeric fields
    if 'capacity' in df.columns:
        df = df.withColumn('capacity', col('capacity').cast(IntegerType()))
    if 'license_number' in df.columns:
        df = df.withColumn('license_number', regexp_replace(col('license_number'), r'\D', '').cast(IntegerType()))

    # handle age columns
    if source_name == 'source2':
        age_columns = ['Ages Accepted 1', 'AA2', 'AA3', 'AA4']
        df = combine_age_columns(df, age_columns)
    elif source_name == 'source3':
        age_columns = ['Infant', 'Toddler', 'Preschool', 'School']
        df = combine_age_columns_source3(df, age_columns)
    return df


# add csv files if needed
def get_csv_files():
    csv_files = {
        'source1': 'csv_files/Technical Exercise Data - source1.csv',
        'source2': 'csv_files/Technical Exercise Data - source2.csv',
        'source3': 'csv_files/Technical Exercise Data - source3.csv'
    }
    return csv_files


# process every csv file
def process_all_files(csv_files):
    dfs = []
    for source_name, file_path in csv_files.items():
        df = process_csv_file(file_path, source_name)
        dfs.append(df)
    return dfs


# put data frames together
def union_all_dataframes(dfs):
    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)
    return df_all


# window function to remove duplicates
def deduplicate_dataframe(df_all):
    window_spec = Window.partitionBy('phone', 'address1').orderBy(col('license_issued').desc())
    df_all = df_all.withColumn('row_num', row_number().over(window_spec)) \
        .filter(col('row_num') == 1).drop('row_num')
    return df_all


# select columns needed
def select_target_columns(df_all, target_columns):
    df_all = df_all.select(target_columns)
    return df_all


# write to the pgdb
def write_to_postgres(df, db_url, db_properties, table_name, mode="append"):
    try:
        df.write.jdbc(url=db_url, table=table_name, mode=mode, properties=db_properties)
        print(f"Data written to PostgreSQL table '{table_name}' successfully.")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        raise


def main():
    csv_files = get_csv_files()
    dfs = process_all_files(csv_files)
    df_all = union_all_dataframes(dfs)
    df_all = deduplicate_dataframe(df_all)
    df_all = select_target_columns(df_all, target_columns)

    # put dataframe into database
    db_url = os.getenv("DB_URL", "jdbc:postgresql://localhost:5433/ETL")
    db_properties = {
        "user": "username",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # write to PostgreSQL
    table_name = "child_care_info"
    if df_all is not None:
        write_to_postgres(df_all, db_url, db_properties, table_name, mode="overwrite")

    spark.stop()


# Call the main function
if __name__ == "__main__":
    main()
