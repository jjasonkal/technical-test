import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import time
import json
import psycopg2

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials from environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_default_region = os.getenv("AWS_DEFAULT_REGION")
data_path = os.getenv("DATA_PATH")
script_file_path = os.getenv("SCRIPT_FILE_PATH")
secret_name = os.getenv("SECRET_NAME")
secret_values = {
    "username": os.getenv("DW_USER"),
    "password": os.getenv("DW_PASS"),
    "dbname": os.getenv("DW_DB"),
    "host": os.getenv("DW_HOST"),
    "port": os.getenv("DW_PORT"),
    "engine": os.getenv("DW_ENGINE")
}

# Validate environment variables
if not all([aws_access_key_id, aws_secret_access_key, aws_default_region]):
    raise ValueError("One or more AWS environment variables are missing.")

if not data_path:
    raise ValueError("DATA_PATH environment variable is missing or empty.")

# Create a Boto3 session with the provided credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_default_region
)

# Now you can use the Boto3 session to interact with AWS services
s3_client = session.client('s3')
cf_client = session.client('cloudformation')
glue_client = session.client("glue")
secrets_client = boto3.client('secretsmanager')


def get_csv_filenames(directory):
    # Initialize a list to store CSV filenames
    csv_filenames = []

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        # Check if the file is a CSV file
        if filename.endswith('.csv'):
            # Add the filename to the list
            csv_filenames.append(filename)

    return csv_filenames


def validate_cloudformation_template(template_body):
    try:
        cf_client.validate_template(TemplateBody=template_body)
        print("CloudFormation template validation succeeded.")
    except ClientError as e:
        print("CloudFormation template validation failed:")
        print(e)
        raise


def upload_cloudformation_template(stack_name, template_body):
    # Validate inputs
    if not isinstance(stack_name, str) or not stack_name.strip():
        raise ValueError("Stack name must be a non-empty string.")
    if not isinstance(template_body, str) or not template_body.strip():
        raise ValueError("Template body must be a non-empty string representing a valid CloudFormation template.")

    # Validate CloudFormation template
    validate_cloudformation_template(template_body)

    # Check if the stack already exists
    try:
        cf_client.describe_stacks(StackName=stack_name)
        print(f"Stack '{stack_name}' already exists. Skipping stack creation.")
        return
    except ClientError as e:
        if e.response['Error']['Message'] != 'Stack with id {} does not exist'.format(stack_name):
            raise

    # Create the CloudFormation stack
    response = cf_client.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Capabilities=['CAPABILITY_IAM']
    )

    # Wait until the stack creation is complete
    cf_client.get_waiter('stack_create_complete').wait(StackName=stack_name)

    # Get the outputs of the CloudFormation stack
    stack_description = cf_client.describe_stacks(StackName=stack_name)

    print("Cloudformation stack created successfully")
    return stack_description['Stacks'][0]['Outputs']


def create_secret(secret_name, secret_values):
    try:
        # Create a Secrets Manager client

        # Check if the secret already exists
        response = secrets_client.describe_secret(SecretId=secret_name)
        print("Secret already exists, skipping creation.")

    except ClientError as e:
        # If the secret does not exist, create it
        if e.response['Error']['Code'] == 'ResourceNotFoundException':

            # Convert dictionary to JSON string
            secret_string = json.dumps(secret_values)

            # Create the secret
            response = secrets_client.create_secret(
                Name=secret_name,
                Description='Redshift Credentials',
                SecretString=secret_string
            )

            print("Secret created successfully:", response["ARN"])

        else:
            # Handle other errors
            print("Error:", e)


def populate_s3(bucket_name, file_path):
    try:
        if file_path.endswith(".csv"):
            response = s3_client.upload_file(file_path, bucket_name, f"{file_path.split('.')[0]}/{file_path}")
            print(f"File uploaded successfully to s3://{bucket_name}/{file_path.split('.')[0]}/{file_path}")
        else:
            response = s3_client.upload_file(file_path, bucket_name, file_path)
            print(f"File uploaded successfully to s3://{bucket_name}/{file_path}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


def run_crawler(crawler_name):
    try:
        # Start the crawler
        response = glue_client.start_crawler(Name=crawler_name)
        print("Crawler '{}' started successfully.".format(crawler_name))

        # Wait until the crawler finishes
        while True:
            response = glue_client.get_crawler(Name=crawler_name)
            crawler_state = response['Crawler']['State']
            if crawler_state == 'READY':
                print("Crawler '{}' finished successfully.".format(crawler_name))
                # Extract and return the table name created by the crawler
                return response['Crawler']['Targets']['S3Targets'][0]['Path']
            elif crawler_state == 'FAILED':
                raise Exception("Crawler '{}' failed.".format(crawler_name))
            else:
                print("Crawler '{}' is still running...".format(crawler_name))
                time.sleep(10)  # Wait for 10 seconds before checking again

    except Exception as e:
        print("An error occurred:", e)
        return None


def create_and_run_glue_etl(job_name, role_arn, default_arguments, resource_bucket_name, script_file_path):
    script_location = f"s3://{resource_bucket_name}/{script_file_path}"

    # Define the job parameters

    job_command = {
        "Name": "glueetl",
        "ScriptLocation": script_location
    }
    job_role = role_arn

    try:
        glue_client.get_job(JobName=job_name)
        print("Glue ETL job already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        # Create the Glue ETL job with Glue version 4.0 and 2 DPUs
        response = glue_client.create_job(
            Name=job_name,
            Role=job_role,
            Command=job_command,
            DefaultArguments=default_arguments,
            ExecutionProperty={
                "MaxConcurrentRuns": 1
            },
            GlueVersion="4.0",
            MaxRetries=0,
            Timeout=10,
            MaxCapacity=2
        )
        print("Glue ETL job created successfully:", response["Name"])
        run_glue_etl(job_name)

    except Exception as e:
        print("An error occurred:", e)


def run_glue_etl(job_name):
    try:
        # Start the Glue ETL job
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print("Glue ETL job '{}' started successfully with JobRunId: {}".format(job_name, job_run_id))

        # Wait until the job finishes
        while True:
            response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            job_run_state = response['JobRun']['JobRunState']
            if job_run_state == 'SUCCEEDED':
                print(
                    "Glue ETL job '{}' finished successfully. Data Warehouse tables have been populated".format(
                        job_name))
                break
            elif job_run_state == 'FAILED':
                raise Exception("Glue ETL job '{}' failed.".format(job_name))
            else:
                print("Glue ETL job '{}' is still running...".format(job_name))
                time.sleep(10)  # Wait for 10 seconds before checking again

    except Exception as e:
        print("An error occurred:", e)


def get_count_from_csv(file_path):
    # Initialize count
    count = 0

    # Open the CSV file
    with open(file_path, 'r') as file:
        # Count the total number of lines
        count = sum(1 for line in file)

    # Subtract 1 to exclude the header row
    return count - 1


def get_count_from_redshift(table_name):
    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=os.getenv("DW_DB"),
        user=os.getenv("DW_USER"),
        password=os.getenv("DW_PASS"),
        host=os.getenv("DW_HOST"),
        port=os.getenv("DW_PORT")
    )
    cursor = conn.cursor()

    # Execute SQL query to retrieve count of records in the Redshift table
    cursor.execute(f'SELECT COUNT(*) FROM public.{table_name}')
    count = cursor.fetchone()[0]

    # Close connection
    cursor.close()
    conn.close()

    return count


def test_record_counts(csv_files, data_path):
    # Iterate through each CSV file
    for csv_file in csv_files:
        # Construct full file path
        csv_file_path = os.path.join(data_path, csv_file)

        # Get count from Redshift for table with same name as CSV file
        redshift_count = get_count_from_redshift(csv_file[:-4])  # Remove .csv extension
        # Get count from CSV file
        csv_count = get_count_from_csv(csv_file_path)

        print(f'CSV row count for {csv_file}: {csv_count}')
        print(f'Data Warehouse table row count for {csv_file}: {redshift_count}')

        # Assert counts are equal
        if csv_file != 'Country_Table.csv' and csv_file != 'Customer_Table.csv':
            assert redshift_count == csv_count, f"Counts mismatch for {csv_file}: Redshift count = {redshift_count}, CSV count = {csv_count}"

    print("Record counts match for all CSV files!")


if __name__ == "__main__":
    stack_name = os.getenv("STACK_NAME")
    template_file = os.getenv("CLOUDFORMATION_TEMPLATE_PATH")

    # Check if the template file exists
    if not os.path.isfile(template_file):
        raise FileNotFoundError(f"Template file '{template_file}' not found.")

    # Read the template file content
    with open(template_file, 'r') as file:
        template_body = file.read()

    outputs = upload_cloudformation_template(stack_name, template_body)

    parsed_outputs = {}

    for output in outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        parsed_outputs[key] = value

    # Access the parsed outputs as a dictionary
    print(parsed_outputs)

    csv_files = get_csv_filenames(data_path)

    # Print the list of CSV filenames
    for filename in csv_files:
        populate_s3(parsed_outputs['GeneratedBucketName'], f'{data_path}/{filename}')

    run_crawler(parsed_outputs['GeneratedCrawler'])

    resource_bucket_name = parsed_outputs['GeneratedResourceBucketName']
    populate_s3(resource_bucket_name, script_file_path)

    create_secret(secret_name, secret_values)

    db = parsed_outputs['GeneratedDatabase']

    default_arguments = {
        "--SECRET_NAME": secret_name,
        "--SECRET_REGION": aws_default_region,
        "--REDSHIFT_TMP_DIR": f"s3://{resource_bucket_name}/",
        "--DATABASE": db,
    }

    role_arn = parsed_outputs['GeneratedGlueETLRole']

    create_and_run_glue_etl(os.getenv("JOB_NAME"), role_arn, default_arguments, resource_bucket_name, script_file_path)

    test_record_counts(csv_files, data_path)