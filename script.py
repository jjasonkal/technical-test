import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError
import json
import sys


# Define function to retrieve secrets from Secrets Manager
def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    return get_secret_value_response['SecretString']


args = getResolvedOptions(sys.argv, ['SECRET_NAME', 'SECRET_REGION', 'REDSHIFT_TMP_DIR', 'DATABASE'])

# Get Redshift credentials from Secrets Manager
redshift_secret_name = args['SECRET_NAME']
redshift_secret_region = args['SECRET_REGION']
redshift_secret = json.loads(get_secret(redshift_secret_name, redshift_secret_region))

# Create SparkContext and GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define connection properties
redshift_username = redshift_secret["username"]
redshift_password = redshift_secret["password"]
redshift_url = f'jdbc:{redshift_secret["engine"]}://{redshift_secret["host"]}:{redshift_secret["port"]}/{redshift_secret["dbname"]}'

# Temporary directory for Redshift
redshift_tmp_dir = args['REDSHIFT_TMP_DIR']

schema_dict = {
    'costs': ' "instance type" VARCHAR(256), "db.r5.4xlarge($)" DOUBLE PRECISION, "db.r6g.xlarge($)" DOUBLE PRECISION, "db.r6g.4xlarge($)" DOUBLE PRECISION, "db.r6g.large($)" DOUBLE PRECISION, "db.r5.xlarge($)" DOUBLE PRECISION, "no instance type($)" DOUBLE PRECISION, "db.r5.large($)" DOUBLE PRECISION, "db.t3.medium($)" DOUBLE PRECISION, "db.t4g.medium($)" DOUBLE PRECISION, "db.t2.medium($)" DOUBLE PRECISION, "total costs($)" DOUBLE PRECISION, "partition_0" VARCHAR(256)',
    'country_table': 'col0 VARCHAR(256), col1 VARCHAR(256), col2 VARCHAR(256), col3 VARCHAR(256), partition_0 VARCHAR(256)',
    'customer_table': 'col0 VARCHAR(256), col1 VARCHAR(256), partition_0 VARCHAR(256)',
    'fx_table': 'date VARCHAR(256), gbpusd DOUBLE PRECISION, eurusd DOUBLE PRECISION, audusd DOUBLE PRECISION, eurgbp DOUBLE PRECISION, euraud DOUBLE PRECISION, gbpaud DOUBLE PRECISION, jpyusd DOUBLE PRECISION, jpyaud DOUBLE PRECISION, jpyeur DOUBLE PRECISION, jpygbp DOUBLE PRECISION, krwusd DOUBLE PRECISION, krwaud DOUBLE PRECISION, krweur DOUBLE PRECISION, krwgbp DOUBLE PRECISION, krwjpy DOUBLE PRECISION, hkdusd DOUBLE PRECISION, hkdaud DOUBLE PRECISION, hkdeur DOUBLE PRECISION, hkdgbp DOUBLE PRECISION, hkdjpy DOUBLE PRECISION, hkdkrw DOUBLE PRECISION, twdusd DOUBLE PRECISION, twdaud DOUBLE PRECISION, twdeur DOUBLE PRECISION, twdgbp DOUBLE PRECISION, twdjpy DOUBLE PRECISION, twdkrw DOUBLE PRECISION, twdhkd DOUBLE PRECISION, partition_0 VARCHAR(256)',
    'salesdata': 'category VARCHAR(256), subcategory VARCHAR(256), product_family VARCHAR(256), key_product VARCHAR(256), sku VARCHAR(256), description VARCHAR(512), grade VARCHAR(256), country_id BIGINT, cost_currency VARCHAR(256), cost_per_device BIGINT, sales_date VARCHAR(256), sold_currency VARCHAR(256), price_sold_per_device BIGINT, status VARCHAR(256), customer_id BIGINT, quantity BIGINT, sales_order_id BIGINT, serial BIGINT, bin_id VARCHAR(256), partition_0 VARCHAR(256)'
}


# Iterate over the dictionary containing table names and schema definitions
for table_name, schema_definition in schema_dict.items():
    # Construct the CREATE TABLE statement dynamically using the schema definition
    create_table_query = f"""
        DROP TABLE IF EXISTS public.{table_name};
        CREATE TABLE IF NOT EXISTS public.{table_name} ({schema_definition});
    """

    # Script generated for node AWS Glue Data Catalog
    aws_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['DATABASE'],
        table_name=table_name,
        transformation_ctx=f"AWSGlueDataCatalog_{table_name}",
    )

    # Write to Amazon Redshift directly
    glueContext.write_dynamic_frame.from_options(
        frame=aws_glue_dynamic_frame,
        connection_type="redshift",
        connection_options={
            "url": redshift_url,
            "user": redshift_username,
            "password": redshift_password,
            "dbtable": f"public.{table_name}",
            "preactions": create_table_query,  # Use the dynamically created CREATE TABLE statement
            "redshiftTmpDir": redshift_tmp_dir,  # Specify the temporary directory
        },
        transformation_ctx=f"AmazonRedshift_{table_name}",
    )

job.commit()

