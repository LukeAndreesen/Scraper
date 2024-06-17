# Site-scraping Web Crawler
Scrape contents from a list of sites
### Contents:
* [Getting Started](#getting-started)
* [Running the Scraper](#running-the-scraper)
* [Parameters](#parameters)
* [File Overview](#file-overview)
## Getting Started
### Please complete all of the following steps to set up your webscraper prior to running
Run the following command to install Python3, venv virtual environment, and all required libraries:
```sh
./config.sh
```
If permission is denied, run the following command from the command line:
```sh
chmod +x *.sh
```
Then retry the previous step.

Next, update `params.json` with desired parameters. **Important:** Must update `root_directory` parameter. Find parameter descriptions [here](#parameters).
```sh
vim params.json
```
Next, connect your AWS and S3 credentials by adding them to the `config.py` in the `src` direectory.
Insert your credentials into their appropriate positions in the file.
```sh
vim src/webscraper/config.py
```

## Running the Scraper
After completing all steps in [Getting Started](#getting-started), the scraper can be run in 3 modes: default, csv, and debug.
* Default: Scans sites from AWS SQS
* CSV: Scan sites from csv file (`urls.csv`)
* Debug: Prints additional information to console for debugging purposes
### Default mode
Add links to AWS SQS, and run the following command:
```sh
./run/run.sh
```
### CSV mode
First, populate `urls.csv` with links. Then run the following command:
```sh
./run/run_csv.sh
```
### Debug mode
Add links to AWS SQS, and run the following command:
```sh
./run/run_debug.sh
```

## Testing
To run the testing suite, simply run the following command:
```sh
python -m pytest
```
### Addings Tests
Test are stored in `tests/test_.py`. To add a test, run the scraper on a given link 5-6 times, and gather the word count and 
duration for each run. Add these to a test using the template in the file.
## Parameters
* `root_directory` (string): path to root directory of project
* `max_words_per_page` (integer): maximum words to scan from each page on site.
* `num_threads` (integer): number of threads to deploy on site. 1 to 4 threads recommended.
* `max_pages` (integer): maximum number of pages to visit on site.
* `site_time_limit` (integer): time limit (seconds) to spend on a single site
* `initial_scan_time_limit` (integer): time limit (seconds) to spend on scanning root page of site. Changes to limit 20 is not recommended.
* `new_links_per_page` (integer): maximum number of new links to gather from each page

## File Overview
The file structure for this project is shown below: 
``` 
.
└── web-crawler
    ├── src
    │   └── webscraper
    │       ├── config.py
    │       ├── scrape.py
    │       ├── smart_queue.py
    │       ├── thread.py
    │       ├── minheap.py
    │       ├── processor.py
    │       ├── utils.py
    │       ├── csv
    │       │   ├── scrape_csv.py
    │       │   ├── smart_queue_csv.py
    │       │   └── thread_csv.py
    │       └── debug
    │           ├── debug_scrape.py
    │           ├── debug_smart_queue_csv.py
    │           ├── debug_thread_csv.py
    │           ├── debug_utils.py
    │           └── debug_minheap.py
    ├── run
    │   ├── run.sh
    │   ├── run_csv.sh
    │   └── run_debug.sh
    ├── README.md
    ├── config.sh
    ├── requirements.txt
    └── params.json
```

## File Breakdown
* `scrape.py` - scrapes sites from SQS by calling smart_queue on each site
* `smart_queue.py` - scrapes a single site
* `thread.py` - scrapes a single page of site and returns contents to queue
* `minheap.py` - priority queue implementation used by smart_queue
* `processor.py` - process text data gathered by smart_queue
* `utils.py` - useful functions called by multiple files
* `config.py` - configures parameters using `params.json`