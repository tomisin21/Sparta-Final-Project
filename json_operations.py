from json_functions import *
import sqlite3


bucket = 'data-eng-30-final-project-files'
aws_prefix = 'Talent/'

# 1 Generate a list of all the json files we will be working with
json_files = download_json_filenames(bucket, aws_prefix)

# 2. Combine all the json objects into a consolidated list
combined_json_list = combine_json_files(json_files)


# 3. Generate sets of unique values for the tech_self_score, strengths and weaknesses values.

# Convert them into a text file to save time and resources.
candidate_information_headers = unique_keyword_non_dict_list_data_types(combined_json_list)
sparta_subjects_headers = unique_keyword_generator_for_dictionaries(combined_json_list, 'tech_self_score')
sparta_strengths_headers = unique_keyword_generator_for_lists(combined_json_list, 'strengths')
sparta_weaknesses_headers = unique_keyword_generator_for_lists(combined_json_list, 'weaknesses')


# 4. Generate dataframes
candidate_information_df = convert_non_list_dict_values_to_df(combined_json_list, candidate_information_headers)
candidate_tech_scores_df = convert_dictionary_values_to_df(combined_json_list, 'tech_self_score', sparta_subjects_headers)
candidate_strengths_df = convert_list_values_to_df(combined_json_list, 'strengths', sparta_strengths_headers)
candidate_weaknesses_df = convert_list_values_to_df(combined_json_list, 'weaknesses', sparta_weaknesses_headers)

##############################################################################################

# 5. Generate CSV files
# candidate_information_df.to_csv('candidate_information.csv', header=True, mode='w')
# candidate_tech_scores_df.to_csv('candidate_test_scores.csv', header=True, mode='w')
# candidate_strengths_df.to_csv('candidate_strengths.csv', header=True, mode='w')
# candidate_weaknesses_df.to_csv('candidate_weaknesses.csv', header=True, mode='w')


sqliteConnection = sqlite3.connect('project.db')
conn = sqlite3.connect('candidates.db')

c = sqliteConnection.cursor()

# data.to_sql('candidatesss', sqliteConnection, if_exists='append', index = False)
# c = conn.cursor()


candidate_information_df.to_sql('candidate_information', conn, if_exists='append', index = False)
candidate_tech_scores_df.to_sql('candidate_tech_scores', conn, if_exists='append', index = False)
candidate_strengths_df.to_sql('candidate_strengths', conn, if_exists='append', index = False)
candidate_weaknesses_df.to_sql('candidate_weaknesses', conn, if_exists='append', index = False)



# 5. Generate CSV files
candidate_information_df.to_csv('candidate_information.csv', header=True, mode='w')
candidate_tech_scores_df.to_csv('candidate_test_scores.csv', header=True, mode='w')
candidate_strengths_df.to_csv('candidate_strengths.csv', header=True, mode='w')
candidate_weaknesses_df.to_csv('candidate_weaknesses.csv', header=True, mode='w')