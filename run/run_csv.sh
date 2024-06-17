#!/bin/bash

# Activate the virtual environment
source ..env/bin/activate

# Run the Python script 'main.py' with input from 'urls.csv'
python3 src/webscraper/scrape_csv.py < urls.csv
