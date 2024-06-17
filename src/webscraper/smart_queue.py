import selenium
import time
import random
import tldextract
import nltk
import threading
from . import utils as ut
import os
import multiprocessing as mp
import tracemalloc
import subprocess
import datetime
import signal
import logging
import boto3
from . import config
from contextlib import contextmanager

###
from .minheap import MinHeap
from .thread import ScannerThread
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


# CONFIGURE AWS PROFILE USING config.py #
S3_PROFILE = config.S3_PROFILE
S3_SESSION = config.S3_SESSION
BUCKET_NAME = config.BUCKET_NAME
S3_RESOURCE = S3_SESSION.resource('s3')
BUCKET = S3_RESOURCE.Bucket(BUCKET_NAME)

# CONFIGURE LOGGING TO TIMEOUT.LOG
logging.basicConfig(filename='timeout.log', level=logging.WARN)


class TimeoutException(Exception):
    """
    Class used to create a timeout signal below
    """
    pass

@contextmanager
def time_limit(seconds):
    """
    Enforces a time limit with a signal interrupt. Used to raise "TimeoutException"
    if process exceeds a time threshold. Used to limit time spent on each link/site

    Inputs:
    seconds (int): Timeout threshold in seconds

    Raises: TimeoutExcecption

    Returns: None
    """
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        

class SmartQueue:
    """
    SmartQueue class is used to scan a given website. By initializing an instance 
    of the class with a root url, the class will automatically deploy threads
    with ChromeDrivers to scan the site by adding all new links found for each page
    in the site. 

    The class uses a MinHeap FIFO queue structure to order and prioritize new pages to 
    visit. Threads access the queue using a mutex locking system on the queue

    SmartQueue tracks CPU/RAM performance data while scanning, and uploads this data
    continually to S3 bucket. The class enforces a timeout limit using  TimeoutException 
    created above to ensure scraper does not spend too much time on site

    Inputs:
    root (str): A url to a webpage (preferably home page) to begin scanning from. 
    (optional) num_threads (int): Number of threads to be deployed on site
    (optional) max_links (int): Maximum number of pages to visit within site
    """
    
    def __init__(self, root, num_threads=config.NUM_THREADS, max_links=config.MAX_PAGES):
        # Setup
        self.queue = MinHeap(max_links + 20) # MinHeap class acts as queue
        self.root = root # Root url
        self.redirect = False # True if root url redirects to new url
        self.home_domain = '' # Root url, or redirected root url
        self.max_links = max_links # Limit number of links to be visited
        self.visited_links = [] # All links visited by queue
        self.original_link_count = 0 # Number of links found on root url
        self.original_links = [] # Links found on root URL
        self.new_link_count = 0 # Number of additional links found through scan
        self.num_threads = num_threads # Specified number of threads
        self.scanner_threads = self.generate_threads() # ScannerThread objects
        self.queue_lock = threading.Lock() # Lock for accesing links from queue
        self.english = True # Site is in English if True
        self.complete = False # True when site scan has een completed 
        self.timeout_limit = config.SITE_TIME_LIMIT # Time limit to scan entire site (sec)

        # Data collection
        self.total_words = 0 
        self.pages_visited = 0
        self.text = []
        self.timeout = False # True if website exceeds timeout limit
        self.start_time = None # Time at which scanning starts

        # Creating files/paths to store site data temporarily before push to S3
        self.result_file =  f"{ut.extract_link_domain(root)}.json"
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H:%M:%S")
        formatted_date = current_datetime.strftime("%Y-%m-%d")
        subdirectory_name = "scraper_results"
        directory_path = os.path.join(os.getcwd(), subdirectory_name, formatted_date)
        self.error_file = os.path.join(directory_path, "errors", f"{formatted_datetime}.txt")
        self.cpu_file = os.path.join(directory_path, "cpu_ram", f"{formatted_datetime}.csv")
        self.formatted_datetime = formatted_datetime


    def run_all(self):
        """
        Run all essential functions. Using the root url, scan through entire 
        site by recursively adding links from each visited page, then visit
        each of these links. Links inserted into queue by subdirectory 
        depth, and limited by specified maximum link visit count.

        Inputs: None
        Returns: None
        Updates: All data through called functions
        Calls: generate_links(), populate_queue(), visit_all_links()
        """
        self.start_time = time.time()
        try:
            # Outputs for debugging/progress tracking
            print(f"Starting new link: {self.root}")
            command = "free | grep Mem | awk '{print $3/$2 * 100.0}'"
            output = subprocess.check_output(command, shell=True)
            print(f"Initial memory check for {self.root} (%RAM):")
            print(output.decode('utf-8'))

            links = self.generate_links() # Generate all links from root url
            if self.timeout:
                # Scan has timed out - kill lingering processes and return
                subprocess.run(["killall", "chrome"], check=True)
                print("smart_queue: Chrome processes terminated successfully.")
                return

            # If no links generated, end, investigate further
            if len(self.original_links) == 0: 
                print("No links found, please investigate")
                print(f"Final memory check for {self.root} (%RAM):")
                print(output.decode('utf-8'))
                return
                
            self.populate_queue(links) # Add root links to queue
            self.visit_all_links() # Visit root links, find new ones, continue recursively through site

            # Outputs for debugging/progress tracking
            print(f"Final memory check for {self.root} (%RAM):")
            print(output.decode('utf-8'))


            # Send performance data to S3
            with open(self.cpu_file, 'rb') as f:
                csv_data = f.read()
            # Upload the CSV
            bucket_key = 'thread_data/' + str(self.num_threads) + 'thread/cpu_ram/' + self.formatted_datetime + '.csv'
            BUCKET.put_object(Body = csv_data, Key = (bucket_key))

        # Exception occurred
        except Exception as e:
            print("smart_queue: exception in run_all()")
            with open(self.error_file, "w") as f:
                f.write(str(e) + "\n")


    def generate_links(self):
        """
        Generate links given a root url to begin site scraping. Checks if 
        site is in English, checks for redirects.

        Inputs: None
        Returns: 
        links (List) - all links found on root url

        Updates: 
        self.home_domain, self.redirect, self.original_links, 
        self.original_link_count

        Calls: 
        utils.create_driver(), english_site(), utils.extract_link_domain(),
        utils.clean_link()
        """

        links = [] # List to store links
        try:
            with(time_limit(config.INITIAL_SCAN_TIME_LIMIT)):
                links.append(self.root)
                driver, header_index = ut.create_driver() # Create a driver for linux operation
                driver.get(self.root) # Visit root url

                # Ensure site is English
                if not self.english_site(driver):
                    self.english = False
                    print("Non-english site detected")
                    home_domain = ut.extract_link_domain(driver.current_url)
                    self.home_domain = home_domain
                    return
                
                # Begin scanning, or find errors
                try:
                    # Create list of all link elements, and search for redirects
                    results = driver.find_elements(By.TAG_NAME, "a") 
                    home_domain = ut.extract_link_domain(driver.current_url)
                    root_domain = ut.extract_link_domain(self.root)
                    # Check for redirect - root url domain != driver domain
                    if home_domain != root_domain:
                        self.redirect = True
                    self.home_domain = home_domain # Update home_domain attribute

                    # Iterate through all result elements and extract href link
                    for element in results:
                        link = element.get_attribute("href") # Get link from element
                        link_domain = ut.extract_link_domain(link) # Get domain from link
                        if ut.passes_link_conditions(link) and (self.home_domain == link_domain):
                            # Is a valid, non-external link
                          #  print("new link found")
                            links.append(ut.clean_link(link)) # Update links
                # Process any errors
                except Exception as e:
                    print("error in smart_queue (line 179)", e)
                    with open(self.error_file, "w") as f:
                        f.write(str(e) + "\n")
        except TimeoutException:
            print("The function timed out!")
            logging.error("The function timed out! - link generation")
            self.timeout = True
                
        result = list(set(links)) # Remove duplicates from list
        self.original_link_count = len(result)
        self.original_links = result
        if len(result) > self.max_links:
            result = result[:self.max_links]
        ut.delete_pdf_files(config.ROOT_DIRECTORY)
        return result # Return cleaned links 
    

    def populate_queue(self, links):
        """
        Populate queue with links found on root url. Inserts links into 
        minheap with priority 1 - root links prioritized first.

        Inputs: 
        links(List) - a list of links found using generate_links()

        Returns: None
        Updates: queue
        Calls: None
        """

        queue = self.queue
        priority = 1
        for link in links:
            if link == self.root:
                queue.insert(0, link)
            else:
                queue.insert(priority, link)
                
                priority += 1
        self.queue = queue
    

    def visit_all_links(self):
        """
        Scan all links; main function for class. Send all scanner threads to 
        queue, which take links 1 by 1 and scans these. 

        Inputs: None
        Returns: None
        Updates: None (calls run_thread)
        Calls: run_thread
        """
        start_time = self.start_time

        try:
            with(time_limit(config.SITE_TIME_LIMIT)):
                for scanner_thread in self.scanner_threads:
                    scanner_thread.thread = threading.Thread(target=self.run_thread, args=(scanner_thread,))
                    scanner_thread.thread.start()
        # The following commented-out section uses # for each line.
                while time.time() - start_time < self.timeout_limit:
                    if self.complete:
                        break
                    time.sleep(1)  # Sleep to prevent a busy loop
        # # If timeout is reached, signal threads to stop
                if time.time() - start_time >= self.timeout_limit:
                    self.timeout = True
                    for scanner_thread in self.scanner_threads:
                        scanner_thread.driver.quit()
                        logging.error("timeout - via innner logic in thread")
                        return
        except TimeoutException:
            print("The function timed out! - TimeoutException in thread \n")
            logging.error("The function timed out! - TimeoutException in thread \n")
            self.timeout = True

        for scanner_thread in self.scanner_threads:
            scanner_thread.thread.join()
            scanner_thread.driver.quit()

        command = "free | grep Mem | awk '{print $3/$2 * 100.0}'"
        output = subprocess.check_output(command, shell=True)

    def run_thread(self, scanner_thread):
        """
        Scan a single link. A single link is removed from the queue and given
        to a scanner thread, which then processes the text and all links 
        within a given link. The queue is updated with new links and data.

        Locking system ensures only one link taken at a time

        Inputs: 
        scanner_thread (ScannerThread object)

        Returns: None

        Updates: 
        self.queue, self.visited_links, self.text, self.pages_visited

        Calls: 
        SmartQueue.is_empty(), SmartQueue.remove_next(), 
        continue_link_gathering(), process_new_links()
        """

        while True:
            new_links = []
            text = []

            with self.queue_lock: # Acquire lock to prevent duplicate link access
                if self.queue.is_empty():
                    self.complete = True
                    break # No more links to visit; end loop
                _, link = self.queue.remove_next()  # Unpack link from queue
                # Scan page with a scanning thread, unpack resulting text and links
                try:
                    text, new_links = scanner_thread.scan_page(link, self.continue_link_gathering(), self.cpu_file)
                except Exception as e:
                    print("error in smart queue")
                    print(e)
                    with open(self.error_file, "w") as f:
                        f.write(str(e) + "\n")
            # Outside of loop, release lock and process results of link
            if len(new_links) != 0: 
                # Process newly acquired links and add them to queue
                self.process_new_links(new_links) 
            
            # Update data
            self.text.append(text) # Add new text to array
            self.total_words += len(text) # Update word count
            self.pages_visited += 1 # Update page visit count
            self.visited_links.append(link) # Update visited links array


    def process_new_links(self, new_links):
        """
        Given a list of links from a scanned page, add these to the queue.
        Filters out all visited links, links already in queue and external links

        Inputs: 
        new_links (list) - links gathered from a page

        Returns: None
        Updates: Queue

        Calls: 
        utils.extract_link_domain(), utils.link_in_queue(), SmartQueue.insert()
        """

        home_domain = self.home_domain # Home domain to check for external links
        # Below comments can be used to limit number of new links gathered per page
        count = 0 ## simple limit of new links per page 
        for link in new_links:
            if count > config.NEW_LINKS_PER_PAGE:
                break
            link_domain = ut.extract_link_domain(link)
            if (home_domain == link_domain) and (link not in self.queue.visited) \
            and not (ut.link_in_queue(link, self.queue)):
                # Is a valid, non-external link not in queue or visited
                directory_rank = ut.num_slashes(link) # Prioritize link by subdirectory (number of slashes)
                priority = self.queue.size() + 1 + (100 * directory_rank) # Score by directory rank
                self.queue.insert(priority, link) # Insert link into queue with score
                count += 1 # Used to limit number of links
        self.new_link_count += count


    def continue_link_gathering(self):
        """
        Simple boolean function to determine if more links should be gathered.
        Passed to threads to inidicate when to stop acquiring new links.

        Stop gathering links when number of visited links + links in queue 
        is equal to max links to be visited

        Inputs: None
        Returns: boolean (True if more links should be acquired)
        Updates: None
        Calls: SmartQueue.size()
        """

        return self.original_link_count + self.new_link_count < self.max_links 
    

    def generate_threads(self):
        """
        Generate scanner threads, specified by SmartQueue's number of threads

        Inputs: None
        Returns: threds(list of ScannerThread objects)
        Updates: None
        Calls: None
        """

        threads = []
        for n in range(self.num_threads):
            threads.append(ScannerThread())
        return threads
    

    def english_site(self, driver):
        """
        Check if site is English

        Inputs: driver (WebDriver with inputted url)
        Returns: boolean - True if site is English
        Updates: None
        Calls: None
        """
        result = True
        try:
            text = driver.find_element(By.XPATH, "html/body").text
            result = ut.is_english(text)
        except Exception as e:
             with open(self.error_file, "w") as f:
                f.write(str(e) + "\n")

        return result
