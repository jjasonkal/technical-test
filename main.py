import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import time

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials from environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_default_region = os.getenv("AWS_DEFAULT_REGION")
data_path = os.getenv("DATA_PATH")

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


def populate_s3(bucket_name, file_path):
    try:
        response = s3_client.upload_file(file_path, bucket_name, f"{file_path.split('.')[0]}/{file_path}")
        print(f"File uploaded successfully to s3://{bucket_name}/{file_path.split('.')[0]}/{file_path}")
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


# Example usage
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
