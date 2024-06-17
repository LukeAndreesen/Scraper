import selenium
import time
import random
import tldextract
import nltk
import threading
import utils as ut
import os
import multiprocessing as mp
import tracemalloc
import subprocess
import datetime



###
from ..minheap import MinHeap
from thread_csv import ScannerThread
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

class SmartQueue:

    def __init__(self, root, num_threads=4, max_links=40):
        # Setup
        self.queue = MinHeap(max_links + 20) # MihHeap class acts as queue
        self.root = root # Root url
        self.redirect = False # True if root url redirects to new url
        self.home_domain = '' # Root url, or redirected root url
        self.max_links = max_links # Limit number of links to be visited
        self.visited_links = [] # All links visited by queue
        self.original_link_count = 0 # Number of links found on root url
        self.original_links = [] # Links found on root URL
        self.new_link_count = 0
        self.num_threads = num_threads # Specified number of threads
        self.scanner_threads = self.generate_threads() # ScannerThread objects
        self.queue_lock = threading.Lock() # Lock for accesing links from queue
        self.english = True

        # Data collection
        self.total_words = 0
        self.pages_visited = 0
        self.text = []
        # Filenames to be used by processor
        self.result_file =  f"{ut.extract_link_domain(root)}.json"
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H:%M:%S")
        formatted_date = current_datetime.strftime("%Y-%m-%d")
        subdirectory_name = "scraper_results"
        directory_path = os.path.join(os.getcwd(), subdirectory_name, formatted_date)
        self.error_file = os.path.join(directory_path, "errors", f"{formatted_datetime}.txt")
     #   self.errors_file = f"errors/fatal/errors_{ut.extract_link_domain(root)}.txt"

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
        try:
            print(f"Starting new link: {self.root}")
            command = "free | grep Mem | awk '{print $3/$2 * 100.0}'"
            output = subprocess.check_output(command, shell=True)
            print(f"Initial memory check for {self.root} (%RAM):")
            print(output.decode('utf-8'))


            #tracemalloc.start()


            links = self.generate_links() # Generate all links from root url

            # If no links generated, end, investigate further
            if len(self.original_links) == 0: 
                print("No links found, please investigate")
                print(f"Final memory check for {self.root} (%RAM):")
                print(output.decode('utf-8'))
                return

            self.populate_queue(links) # Add root links to queue
            self.visit_all_links() # Visit root links, find new ones, continue recursively through site
            print(f"Final memory check for {self.root} (%RAM):")
            print(output.decode('utf-8'))
        except Exception as e:
            with open(self.error_file, "w") as f:
                f.write(str(e) + "\n")
       # print("memory usage for this link")
      #  print(tracemalloc.get_traced_memory())
      #  tracemalloc.stop()


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
        links.append(self.root)
        driver = ut.create_driver() # Create a driver for linux operation
        driver.get(self.root) # Visit root url

        # Ensure site is English
        if not self.english_site(driver):
            self.english = False
            print("Non-english site detected")
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
               #     print(ut.clean_link(link))
                    links.append(ut.clean_link(link)) # Update links
        # Process any errors
        except Exception as e:
            with open(self.error_file, "w") as f:
                f.write(str(e) + "\n")
        
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

        for scanner_thread in self.scanner_threads:
            scanner_thread.thread = threading.Thread(target=self.run_thread, args=(scanner_thread,))
            scanner_thread.thread.start()

        for scanner_thread in self.scanner_threads:
            scanner_thread.thread.join()
            scanner_thread.driver.quit()
          #  scanner_thread.driver.close()
            #scanner_thread.driver = None

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
            with self.queue_lock: # Acquire lock to prevent duplicate link access
                if self.queue.is_empty():
                    break # No more links to visit; end loop
                _, link = self.queue.remove_next()  # Unpack link from queue
                # Scan page with a scanning thread, unpack resulting text and links
                try:
                    text, links = scanner_thread.scan_page(link, self.continue_link_gathering())
                   # print(text)
                  #  print(links)
                except Exception as e:
                    with open(self.error_file, "w") as f:
                        print("error exception in run_thread of smart_queue.py")
                        f.write(str(e) + "\n")
            # Outside of loop, release lock and process results of link
            if len(links) != 0: 
                # Process newly acquired links and add them to queue
                self.process_new_links(links) 
            
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
            if count > 10:
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
