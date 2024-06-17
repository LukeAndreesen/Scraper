import pytest
import os
import sys 
import statistics

from src.webscraper.scrape import main

def test_scrape_1():
    # BASELINE DATA FOR 2 THREADS
    baseline_counts = [17448, 19655, 20925, 19245, 21572, 20771]
    baseline_durations = [298.44, 340.95, 317.77, 300.7, 313.53, 281.8]
    
    count_deviation = statistics.stdev(baseline_counts)
    count_average = statistics.mean(baseline_counts)
    duration_deviation = statistics.stdev(baseline_durations)
    duration_average = statistics.mean(baseline_durations)
    link = 'https://www.binance.com/en'

    word_count, duration = main('html-update-queue', 1, True, link)

    assert abs(word_count - count_average) <= 2 * count_deviation
    assert abs(duration - duration_average) <= 2 * duration_deviation


def test_scrape_2():
    # BASELINE DATA FOR 2 THREADS
    baseline_counts = [16964,16956,16894,16845,16806]
    baseline_durations = [330.62,365.98,278.41,248.02,248.49]
    
    count_deviation = statistics.stdev(baseline_counts)
    count_average = statistics.mean(baseline_counts)
    duration_deviation = statistics.stdev(baseline_durations)
    duration_average = statistics.mean(baseline_durations)
    link = 'https://360payments.com/'

    word_count, duration = main('html-update-queue', 1, True, link)

    assert abs(word_count - count_average) <= 2 * count_deviation
    assert abs(duration - duration_average) <= 2 * duration_deviation

def test_scrape_3():
    # BASELINE DATA FOR 2 THREADS
    baseline_counts = [25077,26336,26060,26420,26095]
    baseline_durations = [185.13,181.45,164.62,187.6,194.19]
    
    count_deviation = statistics.stdev(baseline_counts)
    count_average = statistics.mean(baseline_counts)
    duration_deviation = statistics.stdev(baseline_durations)
    duration_average = statistics.mean(baseline_durations)
    link = 'https://www.cognizant.com/us/en'

    word_count, duration = main('html-update-queue', 1, True, link)

    assert abs(word_count - count_average) <= 2 * count_deviation
    assert abs(duration - duration_average) <= 2 * duration_deviation



def test_scrape_2():
 #  # link = 'https://armadalabs.com/'
 #   mean_word_count = 
 #   word_count, duration = scrape.main('html-update-queue', 1, True, link)
 #   assert word_count 

def test_scrape_3():
 #   link = 'https://www.censia.com/'
 #   mean_word_count = 
  #  word_count, duration = scrape.main('html-update-queue', 1, True, link)
    assert word_count 

def test_answer():
    assert func(3) == 5

"""