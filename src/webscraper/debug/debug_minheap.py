import sys
import utils as ut
import nltk
# from nltk.corpus import stopwords

#STOP_WORDS = set(stopwords.words('english'))

class Processor:
    def __init__(self, queue):#,# results_file):
        self.queue = queue
     #   self.results_file = results_file
        self.stop_word_count = 0
    
    def print_text(self):
        queue = self.queue
        with open(self.results_file, 'w') as file:
            if self.queue.english == False:
                file.write("Non english site detected")
            for page in queue.text:
                for word in page:
                   # if word not in STOP_WORDS:
                    file.write(word + " ")
                   # else:
                    #    self.stop_word_count += 1
        file.close()
        
    def get_text(self):
        result = ''
        queue = self.queue
        if self.queue.english == False:
            result += "Non english site detected"
        else:
            for page in queue.text:
                for word in page:
                   # if word not in STOP_WORDS:
                    result += word
                    result += " "
                   # else:
                    #    self.stop_word_count += 1
        return result

    def print_text_without_numbers(self, filename):
        queue = self.queue
        with open(filename, 'w') as file:
            for page in queue.text:
                for word in page:
                    if not ut.is_number(word):
                        file.write(word + " ")

    
    
    def run_processes(self):
        self.update_word_count()
    
    def update_word_count(self):
        raise NotImplementedError
    
    def update_page_count(self):
        raise NotImplementedError
    
    def clean_file(filename):
        # strip puctuation, strip flllers, merge stem words
        raise NotImplementedError
         
    def strip_punctuation(filename):
        raise NotImplementedError
    
    def strip_filler_words(filename):
        raise NotImplementedError
    
    def replace_stem_words(filename):
        raise NotImplementedError
        
    def add_result_file(self, filename):
        self.result_file_names.append(filename)
