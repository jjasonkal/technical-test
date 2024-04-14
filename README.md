# Report

## Instructions:
Create .env file in the root folder in which you will store sensitive information and configurations.

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=

DATA_PATH='data'
CLOUDFORMATION_TEMPLATE_PATH='template.yaml'
STACK_NAME='cloudformationstack'
JOB_NAME='glueETLjob'
SCRIPT_FILE_PATH='script.py'

SECRET_NAME='redshift_credentials'
DW_USER=
DW_PASS=
DW_DB='dev'
DW_HOST=
DW_PORT='5439'
DW_ENGINE='redshift'
```

Make sure you have installed all the library dependencies of the main.py script by executing:
```
pip install -r requirements.txt
```

In order to import the data into a data warehouse you have to run the main.py script and this will automatically create a Cloudformation Stack with the required resources. After that boto3 is used to populate the S3 buckets

## Note:
The table schemas are mentioned in the Glue ETL script (script.py)

## Next Steps:
- Clean CSVs to have consistent data and the right header to be used for the column names
- Investigate: The costs.csv file didn’t parse correctly by the glue crawler but for some reason in the redshift table the data are coming for these columns
- Parse schema from glue data catalog dynamically while ingesting CSVs from S3 to Redshift
- Make sure the glue crawler have the correct rules for expected data to convert them into the right data type (dates especially)
- Check the Status of the Glue Crawler's last run to reduce the time the script is waiting for the crawling to be over.
- Validate that the row count that is integrated into the Data Warehouse matches the row count of the file in the Data Lake.
