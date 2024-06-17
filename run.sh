#!/bin/bash

# Activate the virtual environment
source env/bin/activate


if [ $# -ne 1 ]; then
    echo "Usage: $0 <number_of_links>"
    exit 1
fi

# Get the argument
number_of_links=$1

# Run your Python script with the argument
python3 src/webscraper/scrape.py "$number_of_links"
# Run the Python script 'main.py' with input from 'urls.csv'
#python3 src/webscraper/scrape.py

