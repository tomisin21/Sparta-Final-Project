import boto3
import pandas as pd
from pprint import pprint
import json


bucket = 'data-eng-30-final-project-files'


def download_json_filenames(bucket_name: str, aws_prefix, keyword='json'):
    # This function loads a list of object names within a given aws bucket
    # when provided with a specific key string
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    contents = paginator.paginate(Bucket=bucket_name, Prefix=aws_prefix)
    aws_files = []
    for page in contents:
        if "Contents" in page:
            for key in page["Contents"]:
                if keyword and key["Key"].endswith(keyword):  # checks to see if files are in csv
                    aws_files.append(key["Key"])
    return aws_files


def get_json_object(bucket, file_object):
    # This function returns a json object when provided an aws bucket and json file path name.
    s3_client = boto3.client('s3')
    s3_object = s3_client.get_object(Bucket=bucket, Key=file_object)
    json_object = s3_object['Body']
    json_string = json.load(json_object)
    return json_string


def combine_json_files(json_files):
    new_list = []
    for file in json_files:
        file_object = get_json_object(bucket, file)
        new_list.append(file_object)
    return new_list


def unique_keyword_non_dict_list_data_types(combined_json_file_list, primary_identifier='name'):
    # This function loops through a list of json files and returns a list of unique values
    # of a provided key-value pair (values must be in dict form)
    key_list = []
    for candidate in combined_json_file_list:
        for key in list(candidate.keys()):
            if type(candidate[key]) != dict and type(candidate[key]) != list and key != primary_identifier:
                key_list.append(key)
    distinct_keys = list(set(key_list))
    new_keys = [primary_identifier] + sorted(distinct_keys)
    return new_keys


def unique_keyword_generator_for_dictionaries(combined_json_file_list, keyword, primary_identifier='name'):
    # This function loops through a list of json files and returns a list of unique values
    # of a provided key-value pair (values must be in dict form)
    key_list = []
    for candidate in combined_json_file_list:
        if keyword in list(candidate.keys()):
            for key in list(candidate[keyword].keys()):
                key_list.append(key)
    distinct_keys = list(set(key_list))
    return [primary_identifier] + sorted(distinct_keys)


def unique_keyword_generator_for_lists(combined_json_file_list, keyword, primary_identifier='name'):
    # This function loops through a list of json files and returns a list of unique values
    # of a provided key-value pair (values must be in list form)
    value_list = []
    for candidate in combined_json_file_list:
        if keyword in list(candidate.keys()):
            for key in candidate[keyword]:
                value_list.append(key)
    distinct_values = list(set(value_list))
    return [primary_identifier] + sorted(distinct_values)


def convert_non_list_dict_values_to_df(combined_json_file_list, df_headers):
    # This loops through a list of dictionaries, appends a specified key value pair
    # (values must be in list form) to a list then aggregates it all to a dataframe.
    data = []
    for candidate in combined_json_file_list:
        index_list = []
        for key in df_headers:
            if type(candidate[key]) != dict and type(candidate[key]) != list:
                index_list.append(candidate[key])
        data.append(index_list)

    df = pd.DataFrame(data, columns=df_headers)
    return df


def convert_dictionary_values_to_df(combined_json_file_list, keyword, df_headers, primary_identifier='name'):
    # This loops through a list of dictionaries, appends a specified key value pair
    # (values must be in dict form) to a list then aggregates it all to a dataframe.
    data = []
    for candidate in combined_json_file_list:
        index_list = []
        index_list.insert(0, candidate[primary_identifier])
        index = 1
        for subject in df_headers[1:]:
            if keyword in candidate:
                if subject in candidate[keyword].keys():
                    index_list.insert(index, candidate[keyword][subject])
                else:
                    index_list.insert(index, None)
            else:
                index_list.insert(index, None)
            index += 1
        data.append(index_list)
    df = pd.DataFrame(data, columns=df_headers)
    return df


def convert_list_values_to_df(combined_json_file_list, keyword, df_headers, primary_identifier='name'):
    # This loops through a list of dictionaries ,appends a specified key value pair
    # (values must be in list form) to a list then aggregates it all to a dataframe.
    data = []
    for candidate in combined_json_file_list:
        index_list = []
        index_list.insert(0, candidate[primary_identifier])
        index = 1
        for subject in df_headers[1:]:
            if subject in candidate[keyword]:
                index_list.insert(index, True)
            else:
                index_list.insert(index, False)
            index += 1
        data.append(index_list)
    df = pd.DataFrame(data, columns=df_headers)
    return df


def generate_csv_file(df, filename):
    # Converts a given dataframe to CSV format
    df.to_csv(f'{filename}.csv', header=True, mode='w')
    print(f'{filename}.csv has been generated')
