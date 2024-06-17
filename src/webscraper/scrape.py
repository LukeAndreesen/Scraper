import json
#import smart_queue
#import processor
import sys
import boto3
import os
import csv
import datetime
from . import config
import time
import logging
import requests
import subprocess

from .processor import Processor
from .smart_queue import SmartQueue

logging.basicConfig(filename='warn.log', level=logging.WARN)

SQS_PROFILE = config.SQS_PROFILE
SQS_SESSION = config.SQS_SESSION
QUEUE_NAME = config.QUEUE_NAME

S3_PROFILE = config.S3_PROFILE
S3_SESSION = config.S3_SESSION
BUCKET_NAME = config.BUCKET_NAME

RETRY_CODES = config.RETRY_CODES

def get_message():
    """
    Take message (link) from SQS queue and return it
    """
    sqs = SQS_SESSION.resource('sqs')
    sqs_queue = sqs.get_queue_by_name(QueueName = QUEUE_NAME)

    for message in sqs_queue.receive_messages(MessageAttributeNames=['All']):
        return message


def get_metadata(domain, metadata):
    """
    Load existing metadata for given link from S3
    """
    current_datetime = datetime.datetime.now()
    formatted_date = current_datetime.strftime("%Y-%m-%d")
    s3 = S3_SESSION.resource('s3')
    try:
        # Object found in success directory
        object = s3.Object(BUCKET_NAME, ('metadata/success' + domain + '.json'))
        data = object.get()
        json_string = json.loads(json.loads(data['Body'].read().decode('utf-8')))
        json_string[formatted_date] = metadata
        return (json_string, 'success')
    except:
        try:
        # Object found in low_count directory
            object = s3.Object(BUCKET_NAME, ('metadata/failure' + domain + '.json'))
            data = object.get()
            json_string = json.loads(json.loads(data['Body'].read().decode('utf-8')))
            json_string[formatted_date] = metadata
            return (json_string, 'fail')
        except:
            try:
            # Object found in failure directory
                object = s3.Object(BUCKET_NAME, ('metadata/low_count' + domain + '.json'))
                data = object.get()
                json_string = json.loads(json.loads(data['Body'].read().decode('utf-8')))
                json_string[formatted_date] = metadata
                return (json_string, 'low_count')
            except:
                json_string = {formatted_date: metadata}
                return (json_string, None)


def update_success_rate(status):
    """
    Update scraper success rate data in S3
    """
    num_success = 0
    num_fail = 0
    num_bad = 0

    if status == "success":
        num_success = 1
    elif status == "fail":
        num_fail = 1
    else:
        num_bad = 1

    s3 = S3_SESSION.resource('s3')
    object = s3.Object(BUCKET_NAME, ('success_rate/success-fail.json'))

    s3_resource = S3_SESSION.resource('s3')
    bucket = s3_resource.Bucket(BUCKET_NAME)

    data = object.get()
    json_string = json.loads(json.loads(data['Body'].read().decode('utf-8')))
    success = int(json_string["success"]) + num_success
    fail = int(json_string["fail"]) + num_fail
    bad = int(json_string["bad_site"]) + num_bad
    rate = (success/(success + fail)) * 100
    rate_string = str(rate) + "%"
    new_data = {
        "success": success,
        "fail": fail,
        "bad_site": bad,
        "rate": rate_string
    }
    json_data = json.dumps(new_data, indent = 2)
    bucket.put_object(Body = json.dumps(json_data), Key = ('success_rate/success-fail.json'))


def retry(code):
    """
    Determine if link should be retried
    """
    if (int(code) >= 500) or (int(code) in RETRY_CODES):
      return True
    return False


def main(sqs_name, number_links, test_mode=False, test_url=None):
    """
    Main process. Run scraper on given number of links from SQS, as
    specified by standard input
    """
    # Create loggers for standard output and error
    start_time = time.time()
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H:%M:%S")
    formatted_date = current_datetime.strftime("%Y-%m-%d")
    
    #Initialize local storage directories
    subdirectory_name = "scraper_results"
    directory_path = os.path.join(os.getcwd(), subdirectory_name, formatted_date)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        os.makedirs(os.path.join(directory_path, "results"))
        os.makedirs(os.path.join(directory_path, "processes"))
        os.makedirs(os.path.join(directory_path, "errors"))
        os.makedirs(os.path.join(directory_path, "cpu_ram"))
    process_file = os.path.join(directory_path, "processes", f"{formatted_datetime}.txt")
    error_file = os.path.join(directory_path, "errors", f"{formatted_datetime}.txt")
    master_file = os.path.join(os.getcwd(), subdirectory_name, "master.csv")
    
    # Data tracking
    word_counts = {}
    check_files = set()
    num_success = 0
    num_fail = 0
    num_bad = 0
    num_timeouts = 0

    # Initalize AWS Services
    sqs = SQS_SESSION.resource('sqs')
    sqs_queue = sqs.get_queue_by_name(QueueName = QUEUE_NAME)
    s3 = S3_SESSION.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)


    try:
        # Open logging files
        with open(process_file, 'w') as log_file:
            with open(error_file, 'w') as error_file:
                link_count = 0
                # Scrape until specified number of links scanned
                while(link_count < number_links):
                    url = ''
                    message = None
                    status = "success"
                    # Fetch and clean link
                    try:
                        if test_mode:
                            url = test_url
                        else:
                            message = get_message()
                            #url = 'http://' + message.message_attributes['domain']['StringValue']
                            url = 'https://www.clicktime.com/'
                        try:    
                            response = requests.get(url)
                            code = response.status_code
                        # Mark as SQS Error
                        except Exception as e:
                            code = "-1"
                        
                        # Local master file of all links scanned
                        f = open(master_file, "w")
                        f.write(url + ',')
                        f.close
                        link_start_time = time.time() # Begin timer
                        link_count += 1
                        try:
                            # Initialize scrape of root url 
                            queue = SmartQueue(url)

                            queue.run_all() # Scrape entire site
                            # Update statuses after scrape
                            if queue.timeout:
                                status = "timeout"
                            word_counts[queue.root] = queue.total_words
                            if queue.total_words < 500:
                                check_files.add(queue.root)
                            
                            # Load site text 
                            p = Processor(queue) 
                            link_end_time = time.time()
                            link_duration = link_end_time - link_start_time
                            text = p.get_text()
                            
                            # Main data JSON
                            data = {
                                    "domain": queue.root,
                                    "date": formatted_datetime,
                                    "duration": link_duration,
                                    "count": queue.total_words, 
                                    "html": text
                            }
                            # Error JSON
                            error_data = {
                                        "url": url,
                                        "date": formatted_datetime,
                                        "duration": link_duration,
                                        "response_code": code
                            }
                            json_data = json.dumps(data, indent = 2)
# Case 1: No words found 
                            if queue.total_words == 0:
                                status = "fail"
                                json_error_data = json.dumps(error_data, indent = 2)
                                if (int(code) < 200) or (int(code) > 299):
                                    num_bad += 1 # bad link
                                    update_success_rate("bad")
                                else:
                                    num_fail += 1
                                    update_success_rate("fail")
                            
    # Case 1a: Code is worth retrying
                                if retry(code):
                                  bucket.put_object(Body = json.dumps(json_error_data), Key = ('flagged_links/redirects/' + queue.home_domain + 'json'))
    # Case 1b: Code is not worth retrying
                                else:
                                  bucket.put_object(Body = json.dumps(json_error_data), Key = ('flagged_links/error_links/' + queue.home_domain + '.json'))
# Case 2: < 500 words found 
                            elif queue.total_words < 500:
                                if (queue.total_words < 200):
                                    num_fail += 1
                                    update_success_rate("fail")
                                else:
                                    num_success += 1
                                    update_success_rate("success")
                                status = "low_count"
                                bucket.put_object(Body = json.dumps(json_data), Key = ('flagged_links/low_count/' + queue.home_domain + '.json'))
                            elif status == "timeout":
                                num_fail += 1
                                update_success_rate("fail")
                                bucket.put_object(Body = json.dumps(json_error_data), Key = ('flagged_links/timeout/' + queue.home_domain + '.json'))
                            else:
# Case 3: Successful link       
                                num_success += 1
                                update_success_rate("success")
                                bucket.put_object(Body = json.dumps(json_data), Key = ('html_data/' + queue.home_domain + '.json'))
                            if not test_mode:
                                message.delete()
                            
                            
                            metadata = {
                                    "domain": queue.root,
                                    "response_code": code,
                                    "word_count": queue.total_words,
                                    "duration": link_duration,
                                    "status": status
                            }
# Upload metadata           
                            if not test_mode:
                                json_string, current_bucket = get_metadata(queue.home_domain, metadata)
    # Add metadata to metadata/fail, remove from previous bucket if necessary

                                # Update appropriate S3 directories with error and metadata
                                if status == "fail":
                                    bucket.put_object(Body = json.dumps(json_string), Key = (f'metadata/fail/{queue.home_domain}.json'))
                                    if (current_bucket != "fail") and (current_bucket is not None):
                                        s3.Object(BUCKET_NAME, (f'metadata/{current_bucket}/{queue.home_domain}.json')).delete()
                                if status == "low_count":
                                    bucket.put_object(Body = json.dumps(json_string), Key = (f'metadata/low_count/{queue.home_domain}.json'))
                                    if (current_bucket != "low_count") and (current_bucket is not None):
                                        s3.Object(BUCKET_NAME, (f'metadata/{current_bucket}/{queue.home_domain}.json')).delete()
                                if status == "timeout":
                                    num_timeouts += 1
                                    bucket.put_object(Body = json.dumps(json_string), Key = (f'metadata/fail/{queue.home_domain}.json'))
                                    if (current_bucket != "fail") and (current_bucket is not None):
                                        s3.Object(BUCKET_NAME, (f'metadata/{current_bucket}/{queue.home_domain}.json')).delete()
                                else:
                                    bucket.put_object(Body = json.dumps(json_string), Key = (f'metadata/success/{queue.home_domain}.json'))
                                    if (current_bucket != "success") and (current_bucket is not None):
                                        s3.Object(BUCKET_NAME, (f'metadata/{current_bucket}/{queue.home_domain}.json')).delete()
                                try:
                                    # Site complete, kill all lingering processes
                                    subprocess.run(["killall", "chrome"], check=True)
                                    print("scrape.py inner exception: Chrome processes terminated successfully.")
                                except Exception as e:
                                    print("No chrome processes to kill (line 266)")
                            
                        # Error in Smart_Queue
                        except Exception as scraper_error:
                            print(scraper_error)
                            error_file.write(str(scraper_error))
                            check_files.add(url)
                            error_data = {
                                    "url": url,
                                    "date": formatted_datetime
                            }

                            # Log error in S3
                            json_error_data = json.dumps(error_data, indent = 2)
                            bucket.put_object(Body = json.dumps(json_error_data), Key = ('flagged_links/error_links/' + queue.home_domain + '.json'))
                            if not test_mode:
                                message.delete() # remove bad link from queue, but save it in bucket
                            try:
                                subprocess.run(["killall", "chrome"], check=True)
                                print("scrape.py inner exception: Chrome processes terminated successfully.")
                            except Exception as e:
                                print("No chrome processes to kill (line 281)")
                            print("scrape.py encountered error (1):", scraper_error)
                    
                    # Handle SQS error
                    except Exception as sqs_error:
                        logging.error(f"An error with sqs occurred at {formatted_datetime}: {str(sqs_error)}\n\n\n\n\n", exc_info=True)
                        error_data = {
                                "url": url,
                                "date": formatted_datetime
                        }
                        # procedures here to kill the process, recover server
                        json_error_data = json.dumps(error_data, indent = 2)
                        bucket.put_object(Body = json.dumps(json_error_data), Key = ('flagged_links/error_links/' + queue.home_domain + '.json'))
                        if (len(url) > 0) and (message is not None) and (not test_mode):
                            message.delete()
                        try:
                            subprocess.run(["killall", "chrome"], check=True)
                            print("scrape.py inner exception: Chrome processes terminated successfully.")
                        except Exception as e:
                            print("No chrome processes to kill (line 298)")

            
            # Update process data
            end_time = time.time()
            elapsed_time = end_time - start_time

            rate = (num_success/(num_success + num_fail)) * 100
            rate_string = str(rate) + "%"

            # Local data tracking
            with open(process_file, 'w') as p:
                p.write(f"Script completed in {elapsed_time:.2f} seconds for {link_count} links\n")
                p.write(f"Average time per link: {(elapsed_time/link_count):.2f} seconds\n")
                p.write("Success " + str(num_success) + "\n")
                p.write("Fail " + str(num_fail) + "\n")
                p.write("Bad Links " + str(num_bad) + "\n")
                p.write("Rate: " + str(rate) + "\n")
                p.write(f"Number of timeouts: {num_timeouts}")
                p.write("Links to check:\n")
                if len(check_files) == 0:
                    p.write((f"No files to check\n"))
                else:
                    for filename in check_files:
                        p.write(f"{filename}\n")
                p.write("Word counts: \n'")
              
                for key, value in word_counts.items():
                    p.write(f"{key}, {value}")
            
            # Send process data to S3
            time_data = {
                "num_links": link_count,
                "total_duration": elapsed_time,
                "avg_link_duration": (elapsed_time/link_count)
            }
            num_threads = config.NUM_THREADS
            bucket.put_object(Body = json.dumps(time_data), Key = (f'thread_data/{num_threads}thread/duration/{formatted_datetime}.json'))

            if not test_mode:
                # Output process data to terminal
                print(f"Script completed in {elapsed_time:.2f} seconds for {link_count} links")
                print(f"Average time per link: {(elapsed_time/link_count):.2f} seconds")
                print("Success", num_success)
                print("Fail", num_fail)
                print("Bad Links", num_bad)
                print("Rate:", rate)
                print(f"Number of timeouts: {num_timeouts}")
                print(f"Links to check:")
                if len(check_files) == 0:
                    print(f"No files to check")
                else:
                    for filename in check_files:
                        print(f"{filename}")
                print(f"\n Word Counts")
                for key, value in word_counts.items():
                    print(f"{key}, {value}")
            
    # Error occurred before scraping began
    except Exception as e:
        logging.error(f"An error occurred at {formatted_datetime}: {str(e)}", exc_info=True)
        subprocess.run(["killall", "chrome"], check=True)

    if test_mode:
        return word_counts[queue.root], elapsed_time

# Fetch command line arguments        
if __name__ == "__main__":
    number_links = sys.argv[1]
    main('html-update-queue', int(number_links))
