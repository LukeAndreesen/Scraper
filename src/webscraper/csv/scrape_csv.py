import json
import processor
import sys
import boto3
import os
import csv
import datetime
import time
import logging

from ..processor import Processor
from smart_queue_csv import SmartQueue
"""
logging.basicConfig(filename='warn.log', level=logging.WARN)
SQS_PROFILE = 'SQS-consume-html-queue-550716682872'
SQS_SESSION = boto3.Session(profile_name = SQS_PROFILE)
QUEUE_NAME = 'html-update-queue'

S3_PROFILE = 'S3-website-crawler-550716682872'
S3_SESSION = boto3.Session(profile_name = S3_PROFILE)
BUCKET_NAME = 'website-crawler'
"""

"""
def get_message():

    sqs = SQS_SESSION.resource('sqs')
    sqs_queue = sqs.get_queue_by_name(QueueName = QUEUE_NAME)
    #wait_time = 10
   # max_number= 10  # max is 10

    for message in sqs_queue.receive_messages(MessageAttributeNames=['All']):
        return message
"""
def main(sqs_name):#, number_links):
    # Create loggers for standard output and error
    start_time = time.time()
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H:%M:%S")
    formatted_date = current_datetime.strftime("%Y-%m-%d")

    subdirectory_name = "scraper_results"
    directory_path = os.path.join(os.getcwd(), subdirectory_name, formatted_date)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        os.makedirs(os.path.join(directory_path, "results"))
        os.makedirs(os.path.join(directory_path, "processes"))
        os.makedirs(os.path.join(directory_path, "errors"))

 #   stdout_logger = logging.getLogger('stdout')
    
    
#    results_file = os.path.join(directory_path, "results", f"{formatted_datetime}.txt")
    process_file = os.path.join(directory_path, "processes", f"{formatted_datetime}.txt")
    error_file = os.path.join(directory_path, "errors", f"{formatted_datetime}.txt")
    master_file = os.path.join(os.getcwd(), subdirectory_name, "master.csv")
    

    word_counts = {}
    check_files = []

   # sqs = SQS_SESSION.resource('sqs')
    #sqs_queue = sqs.get_queue_by_name(QueueName = QUEUE_NAME)

    try:
        with open(process_file, 'w') as log_file:
            with open(error_file, 'w') as error_file:
       #         sys.stdout = log_file
        #        sys.stderr = error_file
             #   reader = csv.reader(sys.stdin)
                link_count = 0
             #   while(True):
                #while(link_count < number_links):
                reader = csv.reader(sys.stdin)
                for row in reader:
                    link_count += 1
                    url = ''
     #               message = None
                    try:
      #                  message = get_message()
       #                 url = 'http://' + message.message_attributes['domain']['StringValue']
                        #print(url)
                        f = open(master_file, "w")
                        f.write(url)
                        f.write(',')
                        f.close

                        link_count += 1
                        #print(url)
                        try:
                # for row in reader:
                            #link_count += 1
                        #3    queue = SmartQueue(url)
                            queue = SmartQueue(row[0])
                           # results_file = os.path.join(directory_path, "results", queue.result_file)
                            queue.run_all()
                            word_counts[queue.root] = queue.total_words
                            if queue.total_words < 500:
                                check_files.append(queue.root)
                            p = Processor(queue)#, results_file)
                            text = p.get_text()
                            data = {
                                    "domain": queue.root,
                                    "date": formatted_datetime,
                                    "count": queue.total_words, #this will need to be updated
                                    "html": text
                                    }
                            json_data = json.dumps(data, indent = 2)
                          #  with open(results_file, 'w') as file:
                           #     file.write(json_data)
                            if queue.total_words == 0:
                                error_data = {
                                        "url": url,
                                        "date": formatted_datetime
                                        }
                                json_error_data = json.dumps(error_data, indent = 2)
                                bucket.put_object(Body = json.dumps(json_error_data), Key = ('boost_payments/error_links/' + queue.home_domain + '.json'))
                          #  elif queue.total_words < 500:
                           #     bucket.put_object(Body = json.dumps(json_data), Key = ('flagged_links/low_count/' + queue.home_domain + '.json'))
                            else:
                                bucket.put_object(Body = json.dumps(json_data), Key = ('boost_payments/' + queue.home_domain + '.json'))
                      #      message.delete()
                        except Exception as scraper_error:
                            error_file.write(str(scraper_error))
                            check_files.append(url)
                            error_data = {
                                        "url": url,
                                        "date": formatted_datetime
                                        }
                            json_error_data = json.dumps(error_data, indent = 2)
                            bucket.put_object(Body = json.dumps(json_error_data), Key = ('boost_payments/error_links/' + url + '.json'))
                       #     message.delete() # remove bad link from queue, but save it in bucket
                    except Exception as sqs_error:
                        logging.error(f"An error with sqs occurred at {formatted_datetime}: {str(sqs_error)}\n\n\n\n\n", exc_info=True)
                        # NOTE - ERROR DATA MAY BE EMPTY URL IF WE FAIL AT GET MESSAGE
                        error_data = {
                                "url": url,
                                "date": formatted_datetime
                                }
                        json_error_data = json.dumps(error_data, indent = 2)
                        bucket.put_object(Body = json.dumps(json_error_data), Key = ('boost_payments/error_links/' + queue.home_domain + '.json'))
                       # if (len(url) > 0) and (message is not None):
                        #    message.delete()


            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Script completed in {elapsed_time:.2f} seconds for {link_count} links")
            print(f"Average time per link: {(elapsed_time/link_count):.2f} seconds")
            print(f"Links to check:")
            if len(check_files) == 0:
                print(f"None")
            else:
                for filename in check_files:
                    print(f"{filename}")
            print(f"\n Word Counts")
            for key, value in word_counts.items():
                print(f"{key}, {value}")
    except Exception as e:
        logging.error(f"An error occurred at {formatted_datetime}: {str(e)}", exc_info=True)
        
if __name__ == "__main__":
#    number_links = sys.argv[1]
    main('html-update-queue')#, int(number_links))
