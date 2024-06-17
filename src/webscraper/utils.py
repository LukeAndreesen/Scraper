import tldextract
import langdetect
import logging
import subprocess
import os
import requests
import random
from . import config

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from urllib.parse import urlparse

USER_AGENTS = config.USER_AGENTS
logging.basicConfig(filename='memory.log', level=logging.INFO)

def split_list(links, n):
    """
    Divide a list of links into n intervals (lists)
    """
    interval = int(len(links)/n)
    result = []
    for cutoff in range(0, len(links), interval):
        if (cutoff + interval) > len(links):
            result.append(links[cutoff:])
        else:
            result.append(links[cutoff: (cutoff + interval)])
    return result

def is_link(href_str):
    """
    Determine if an href found while scanning is a link
    """
    if (href_str != None and "http" in href_str):
        return True
    return False

def extract_link_domain(url):
    """
    Extra domain from a url using tldextract
    """
    return tldextract.extract(url).registered_domain.lower()

def create_driver():
    """
    Create a headless ChromeDriver for selenium to operate on
    """
    service = Service()
    options = webdriver.ChromeOptions()
    random_index = random.randint(0, len(USER_AGENTS) - 1)
    random_user_agent = USER_AGENTS[random_index]
    options.add_argument(f"--user-agent={random_user_agent}")
    options.add_argument('--headless') # Do not open visual window
    
    # Atttempt to restrict unwanted downloads
    prefs = {
    "download.open_pdf_in_system_reader": False,
    "download.prompt_for_download": True,
    "plugins.always_open_pdf_externally": False
    }
    options.add_experimental_option("prefs", prefs)

    # Configure driver with above options
    driver = webdriver.Chrome(service = service, options = options)
    return driver, random_index

def switch_header(driver, current_index):
    driver.quit()
    service = Service()
    options = webdriver.ChromeOptions()
    # Get a random user agent that is not the current
    indices = list(range(0, len(USER_AGENTS)))
    indices.remove(exclude)  # Removes the number you don't want
    random_index = random.choice(numbers) 
    random_user_agent = USER_AGENTS[random_index]
    options.add_argument(f"--user-agent={random_user_agent}")
    options.add_argument('--headless') # Do not open visual window
    
    # Atttempt to restrict unwanted downloads
    prefs = {
    "download.open_pdf_in_system_reader": False,
    "download.prompt_for_download": True,
    "plugins.always_open_pdf_externally": False
    }
    options.add_experimental_option("prefs", prefs)

    # Configure driver with above options
    driver = webdriver.Chrome(service = service, options = options)
    return (driver, user_agent_index)


def is_number(str):
    """
    Determine if string contains a number while handling exception
    """
    try:
        float(str)
        return True
    except ValueError:
        return False
    
def link_in_queue(link_to_check, queue):
    """
    Check if a link is currently in the MinHeap queue
    """
    queued_links = list(queue.indices.keys())
    for link in queued_links:
        if link_to_check == link:
            return True
    return False
        
def remove_anchor(link):
    """
    Remove anchor tag from links. Anchor tags take you to specific part of page.
    Anchor tags can make two links to the same page appear different, so we must 
    remove them.
    """    
    if "#" in link:
        link = link.split("#")[0]
    return link

def is_not_download(link):
    """
    Ensure that a link is not actually a file download. Check the path to ensure
    no file extension beside .html (ie. 'pdf', 'zip, etc.) is contained in link.
    Further check by requesting link headers and checking content type and 
    disposition.
    """ 
    parse = urlparse(link)
    # Check if the path has a file extension that is not .html
    if '.' in parse.path and '.html' not in parse.path:
        return False

    # Download not always evident by path: request headers to check further
    try:
        response = requests.get(link)
        headers = response.headers
        content_disposition = headers.get('Content-Disposition', '')
        content_type = headers.get('Content-Type', '')
        if "attachment" in content_disposition or "octet-stream" in content_type:
            return False
    except Exception as e:
        pass
    return True

def is_not_login(link):
    """
    Check if a page is a login page, which we will ignore.
    """
    if "login" in link:
        return False
    return True

def passes_link_conditions(link):
    """
    Check that a link can be added to queue and scanned by ensuring
    it is a valid link, is not a download, and is not a login page
    """
    return is_link(link) and is_not_download(link) and is_not_login(link)

def remove_trailing_slash(path):
    """
    Remove slash at end of path
    """
    if len(path) > 1:
        if path[-1] == "/":
        # print(path)
            path = path[:-1]
    return path

def clean_link(link):
    """
    Clean link in order to check for duplicates by removing anchor tag and 
    trailing slash
    """
    link = remove_anchor(link)
    link = remove_trailing_slash(link)
    return link

def num_slashes(url):
    """
    Count the number of slashes in path to determine how 'deep' into a site
    a given link is for prioritization purposes. Cleans link before checking.
    """
    parse = urlparse(url)
    path = parse.path
    cleaned_path = remove_trailing_slash(path)
    return cleaned_path.count("/")

def is_english(text_sample):
    """
    Determines if text sample is English
    """
    return langdetect.detect(text_sample) == "en" 

def cpu_check(url):
    """
    Check the current CPU usage using subprocess module, print the usage info
    to terminal and log, and return usage as float
    """
    command = "mpstat 1 1 | awk '/Average:/ {print 100 - $12}'"
    output = subprocess.check_output(command, shell=True)
    print(f"Current CPU usage for {url} (%CPU):")
    print(output.decode('utf-8').strip())
    logging.info(f"CPU: {output.decode('utf-8').strip()}")
    return float(output.decode('utf-8').strip())

def ram_check(url):
    """
    Check the current RAM usage using subprocess module, print the usage info
    to terminal and log, and return usage as float
    """
    command = "free | grep Mem | awk '{print $3/$2 * 100.0}'"
    output = subprocess.check_output(command, shell=True)
    print(f"Current RAM usage for {url} (%RAM):")
    print(output.decode('utf-8'))
    logging.info(f"RAM: {output.decode('utf-8')}")
    return float(output.decode('utf-8').strip())

def delete_pdf_files(directory_path):
    """
    Delete all files that were unintentionally downloaded from a site.
    We attempt to ensure no files are downloaded with other methods, but 
    in the case where file is downloaded, we delete it immediately to ensure
    no build up of files
    None
    """
    # Ensure the directory path ends with a separator
    if not directory_path.endswith(os.path.sep):
        directory_path += os.path.sep

    try:
        # List all files in the directory
        files = os.listdir(directory_path)

        # Iterate through the files and delete those with .pdf extension
        for file_name in files:
            if not ((file_name.endswith(".py")) or (file_name.endswith(".log")) or (file_name.endswith(".txt")) or (file_name.endswith(".csv")) or (file_name.endswith(".json")) or  (file_name.endswith(".sh")) or ('.' not in file_name) or (file_name.startswith('.')) or (file_name.endswith(".md"))):
                file_path = os.path.join(directory_path, file_name)
                os.remove(file_path)
                print(f"Deleted: {file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
