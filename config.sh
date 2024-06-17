#!/bin/bash

# Update the Linux server
sudo yum update

# Install Python 3
sudo yum install python3

# Install pip for Python 3
sudo yum install python3-pip

# Install virtualenv using pip
pip3 install virtualenv

# Install chrome
sudo curl https://intoli.com/install-google-chrome.sh | bash
sudo mv /usr/bin/google-chrome-stable /usr/bin/google-chrome

# Create a virtual environment named 'env'
virtualenv env

# Activate the virtual environment
source env/bin/activate

# Install Python libraries from requirements.txt
pip install -r requirements.txt

# Create CSV to store URLS
touch urls.csv

# Deactivate the virtual environment
deactivate

# Script complete
echo "Setup is complete."

