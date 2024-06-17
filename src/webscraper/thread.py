import time
import os
import sys
import selenium
from . import utils as ut
import datetime
import csv
from . import config

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait


MAX_WORDS = config.MAX_WORDS_PER_PAGE # Max number of words to read from a page 

class ScannerThread:

    def __init__(self):
        self.driver, _ = ut.create_driver() # Thread has own Linux-capable driver
        self.total_word_count = 0 # Word count for specific thread
        self.word_counts = [] # Word count split by each visited link
        self.thread = None # ScannerThread assigned Python thread object 


    def scan_page(self, link, continue_link_gathering, cpu_file):
        """
        Scan the contents of a given link. Called by SmartQueue, which 
        distributes links from queue 1 by 1 to ScannerThread objects.

        Inputs: 
        link(string) - A url; continue_link_gathering(boolean) - 
        True if more links should be gathered, False if visited link limit hit

        Returns: 
        (text, links) (tuple) - A tuple containing list of page text 
        split by spaces, and all links from the page

        Updates:
        self.total_word_count, self.word_counts

        Calls:
        utils.passes_link_conditions(), utils.clean_link()
        """
        current_datetime = datetime.datetime.now()
        formatted_date = current_datetime.strftime("%Y-%m-%d")
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H:%M:%S")

        subdirectory_name = "scraper_results"
        directory_path = os.path.join(os.getcwd(), subdirectory_name, formatted_date)
       
        
        ram = ut.ram_check(link)
        cpu = ut.cpu_check(link)

        with open(cpu_file, 'a', newline='') as csvfile:
            # Create a CSV writer object
            writer = csv.writer(csvfile)
            writer.writerow([ram, cpu, link])

        # Returns text on page as list of single-word strings
        driver = self.driver
        driver.get(link) # Visit url
        # Get text from url, limit by word count
        text = ""
        try:
            text = driver.find_element(By.XPATH, "html/body").text 
        except Exception as e:
            print("error in thread.py : driver.find_element")
        text = text.split()
        if len(text) > MAX_WORDS:
            text = text[:MAX_WORDS]

        # Gather links from url, until halted by continue_link_gathering condition
        links = []
        if continue_link_gathering:
            # Find all links, filter out visited and external links
            link_elements = []
            try:
                link_elements = driver.find_elements(By.TAG_NAME, "a")
            except Exception as e:
                print("thread.py: errror in find_elements (link)")
            # Iterate through all link elements and find href link
            for element in link_elements:
                try:
                    link = element.get_attribute("href")
                    link_domain = ut.extract_link_domain(link)
                    if ut.passes_link_conditions(link):
                        links.append(ut.clean_link(link))
                # Handle exceptions
                except Exception as e:
                    with open("link_errors.txt", 'w') as f:
                        f.write(str(e))
                    
        # Split text string into list of single words
        self.total_word_count += len(text) # Update total word count
        self.word_counts.append(len(text)) # Update page word count
        
        ut.delete_pdf_files(config.ROOT_DIRECTORY)
        # Return tuple of list of words and list of links (set to remove duplicates)
        return (text, list(set(links)))
