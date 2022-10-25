import boto3
import pandas as pd
import sqlite3

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket_name = "data-eng-30-final-project-files"


# Access the Talent txt files
AWS_BUCKET_PREFIX = 'Talent/Sparta'
bucket_contents = s3_client.list_objects(
    Bucket=bucket_name, Prefix=AWS_BUCKET_PREFIX)


def get_text_files(s3_client, bucket_name, bucket_contents):
    df = pd.DataFrame()  # to call the DataFrame constructor on list

    # Accessing the file from the Bucket using loop
    for file in bucket_contents['Contents']:
        file_name = file["Key"]

        # Download the file
        s3_client.download_file(
            Filename="Sparta_day.txt",
            Bucket=bucket_name,
            Key=file_name)
        df = merge_text_files(df)  # Merging all files in one CSV files
    return df


def merge_text_files(df):  # Merging all files into a raw CSV files
    try:
        data = pd.read_csv(  # Converting file CSV file
            "Sparta_day.txt",
            sep=";",
            header=None)
        data['date'] = data[0][0]  # Extract the date column
        data['academy'] = data[0][1]  # Extract the Academy Column
        data.drop([0, 1], inplace=True)  # Clearing up date and Academy lines

        df = pd.concat([df, data])  # Append file to CSV file
    except:
        pass
    return df  # returning the raw Data Frame


def get_raw_df(s3_client, bucket_name, bucket_contents):  # Getting Raw Data Frame from combining all the text files
    df = get_text_files(s3_client, bucket_name, bucket_contents)  #  getting all the text files
    return df  # Returning the Raw Data Frame


def clean_df(df):  # Cleaning the Data frame
    date_academy_data = pd.DataFrame()  # Creating temporary data frame,
    date_academy_data["Date"] = df["date"]  # Getting date column
    date_academy_data["Academy"] = df["academy"]  # Getting Academy column
    name_test_data = df[0].str.split(" -  ", expand=True)  # Separate the name Column and split it to another column
    del df[0]  # Drop the unnecessary column
    name_test_data[["test1", "test2"]  # Getting the two test result column
                   ] = name_test_data[1].str.split(",", expand=True)  # Splitting the Test result column
    del df['date']  # delete unnecessary column for date
    del df['academy']  # delete unnecessary column for academy
    del name_test_data[1]  # deleting extra columns, except Name and the two test result column

    main = pd.concat([name_test_data, date_academy_data], axis=1)
    # Merging name test result and date and academy column into one data frame
    main.columns = ['Candidate Name', 'Psychometrics Result', 'Presentation Result', 'Date',
                    'Academy Location']  # Give header to the main data frame
    # Give The header name
    main['Psychometrics Result'] = main['Psychometrics Result'].str.replace('Psychometrics: ', '')
    main['Presentation Result'] = main['Presentation Result'].str.replace('Presentation: ', '')
    main['Academy Location'] = main['Academy Location'].str.replace(' Academy', '')

    main = main.reset_index()  # Reset The index number in an ascending
    del main['index']  # Delete the Index Table due to Inconsistency
    return main  # Return clean Data frame


def upload(main):
    main.to_csv("cleaned.csv")
    # s3_client.upload_file (  # Download the file one by one
    #     Filename="latest_update.csv", Bucket=bucket_name, Key="Cleaned.csv")


df = get_raw_df(s3_client, bucket_name, bucket_contents)  # Getting the row data frame

main = clean_df(df)  # Clean row data frame

main.to_csv('candidate_day_information.csv', header=True, mode='w')
# upload(main)  # Uploading data frame

##############################################################################################
# sqliteConnection = sqlite3.connect('project.db')
# conn = sqlite3.connect('candidates.db')
#
# c = sqliteConnection.cursor()

# data.to_sql('candidatesss', sqliteConnection, if_exists='append', index = False)
# c = conn.cursor()

# main.to_sql('text_files', conn, if_exists='append', index = False)