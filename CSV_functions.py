import boto3
from pprint import pprint
import pandas as pd


# function to connect to the S3 bucket
def connect_to_bucket():
    s3_client = boto3.client("s3")
    return s3_client


# function to retrieve the contents of the bucket for every page
def bucket_contents(bucket_name: str, aws_prefix: str):
    paginator = connect_to_bucket().get_paginator("list_objects_v2")
    contents = paginator.paginate(Bucket=bucket_name, Prefix=aws_prefix)
    return contents


# function to iterate through contents of bucket
def get_all_files(bucket_name: str, aws_prefix: str, file_ending: str ):
    csv_list = []  # list to store CSV files
    for page in bucket_contents(bucket_name, aws_prefix):
        if "Contents" in page:  # checks if "Contents" exists in each object
            for key in page["Contents"]:  # loop through each object's contents to retrieve the Key (filename)
                if file_ending and key["Key"].endswith(file_ending):  # checks to see if files are in csv
                    keystring = key["Key"]  # gets filename of all csv files
                    objectbody = connect_to_bucket().get_object(Bucket=bucket_name, Key=keystring)  # retrieves the body data from each csv
                    readbody = pd.read_csv(objectbody["Body"], header="infer")  # reads each file data as a csv
                    pd.set_option("display.max_columns", None)
                    csv_list.append(readbody)  # appends it to the list of CSVs
    return csv_list


# function to merge all csv files together
def merge_csv(bucket_name: str, aws_prefix: str, file_ending: str ):
    all_csv = pd.concat(get_all_files(bucket_name, aws_prefix, file_ending), ignore_index=True)
    return all_csv


# function to create csv file from the merged list
def convert_to_csv(bucket_name: str, aws_prefix: str, file_ending: str, file_name: str):
    return merge_csv(bucket_name, aws_prefix, file_ending).to_csv(file_name)


# function executes all previous function simultaneously to extract and create a CSV file
def execute_all(bucket_name: str, aws_prefix: str, file_ending: str, file_name: str):
    get_all_files(bucket_name, aws_prefix, file_ending)
    merge_csv(bucket_name, aws_prefix, file_ending)
    return convert_to_csv(bucket_name, aws_prefix, file_ending, file_name)
