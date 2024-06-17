import boto3
import glob
import os
import json

"""
INSERT AWS CREDENTIALS HERE
"""
SQS_PROFILE = 
SQS_SESSION = 
QUEUE_NAME = 

S3_PROFILE = 
S3_SESSION = 
BUCKET_NAME = 
    

json_path = glob.glob('params.json')[0] # Find params.json file in root directory

# Read the JSON file
with open(json_path, 'r') as file:
    params = json.load(file)


# Set parameters
MAX_WORDS_PER_PAGE = params["max_words_per_page"]
NUM_THREADS = params["num_threads"]
MAX_PAGES = params["max_pages"]
SITE_TIME_LIMIT = params["site_time_limit"]
INITIAL_SCAN_TIME_LIMIT = params["initial_scan_time_limit"]
NEW_LINKS_PER_PAGE = params["new_links_per_page"]
ROOT_DIRECTORY = params["root_directory"]
USER_AGENTS = params["user_agents"]
