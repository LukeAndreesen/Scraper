import smart_queue
import processor
import sys
import os
import csv
import datetime
import time
import logging

from processor import Processor
from smart_queue import SmartQueue

logging.basicConfig(filename='warn.log', level=logging.WARN)


def main():
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

    stdout_logger = logging.getLogger('stdout')
    
    
    results_file = os.path.join(directory_path, "results", f"{formatted_datetime}.txt")
    process_file = os.path.join(directory_path, "processes", f"{formatted_datetime}.txt")
    error_file = os.path.join(directory_path, "errors", f"{formatted_datetime}.txt")
    

    word_counts = {}
    check_files = []

    try:
        with open(process_file, 'w') as log_file:
            with open(error_file, 'w') as error_file:
                sys.stdout = log_file
            #  sys.stderr = error_file
                reader = csv.reader(sys.stdin)
                link_count = 0
                for row in reader:
                    link_count += 1
                    queue = SmartQueue(row[0])
                    queue.run_all()
                    word_counts[queue.root] = queue.total_words
                    if queue.total_words < 500:
                        check_files.append(queue.root)
                    p = Processor(queue, results_file)
                    p.print_text()
                    print("\n")
                    print("\n")

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
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
        
if __name__ == "__main__":
    main()