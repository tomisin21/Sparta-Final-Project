import boto3
import pandas as pd
from io import StringIO



def download_aws_filenames(bucket: str = 'data-eng-30-final-project-files', prefix: str = 'Academy/'):
    # This function loads a list of object names within a given aws bucket
    # when provided with a specific key string
    s3_client = boto3.client('s3')
    bucket_contents = s3_client.list_objects(Bucket=bucket)
    aws_files = []
    for object in bucket_contents['Contents']:
        if prefix in object['Key']:
            aws_files.append(object['Key'])

    return aws_files


def generate_tech_workstream_name(file_name):
    first_split_string = file_name.replace('Academy/', '')
    split_string = first_split_string.split('_')
    file_string = f'{split_string[0]} {split_string[1]}'
    return file_string


def load_panda_df(file_object, bucket: str = 'data-eng-30-final-project-files'):
    # This function returns a pandas dataframe when provided an aws bucket and csv object.
    s3_client = boto3.client('s3')
    s3_object = s3_client.get_object(Bucket=bucket, Key=file_object)
    csv_object = s3_object['Body']
    csv_string = csv_object.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df


def create_workstream_df(file):
    df = load_panda_df(file)
    index = len(df.index)
    workstream = []
    workstream_name = generate_tech_workstream_name(file)
    for i in range(index):
        workstream.append(workstream_name)
    df.insert(1, 'workstream_name', workstream, True)

    nan_rows_df = df[df['Analytic_W8'].isnull()]
    discontinued_students = nan_rows_df['name'].tolist()
    student_status = []
    for student in df['name']:
        if student not in discontinued_students:
            student_status.append('Completed Training')
        else:
            student_status.append('Discontinued From Course')

    df.insert(1, 'status', student_status, True)
    return df


def create_df_list(csv_file_objects):
    df_list = []
    for file in csv_file_objects:
        df = create_workstream_df(file)
        df_list.append(df)
    return df_list


def combine_dfs(df_list):
    # This function concatenates all the dataframes provided within the argument into a consolidated dataframe
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df



def upload_file_to_aws(file_name, bucket_name, path='Data30/Tomi'):
    # This function uploads a given csv file into a specified AWS bucket and path
    s3_client = boto3.client('s3')
    s3_client.upload_file(
        Filename=file_name,
        Bucket=bucket_name,
        Key=f'{path}/{file_name}')
    print(f'Uploaded file {file_name}')

