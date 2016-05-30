# CS123
# Campaign Donation MRJob Project

# This file runs entity resolution on the full 5.2 million donations, mapping
# company aliases to their Fortune 500 authoritative name for further analyis.

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
import csv
from fuzzywuzzy import fuzz
import json

# Determines the threshold at which two strings are considered similiar
SIMILARITY_THRESHOLD = 70

# Column indices for slicing
ID = 0
AMOUNT = 8
CONTRIBUTOR_NAME = 10
SEAT = 36
K = 50
DONOR_TYPE = 12
ORGANIZATION = 21

class fortune_json_builder(MRJob):
    '''
    Creates a JSON file mapping Fortune 500 company names to their aliases
    within the dataset. 
    '''
    OUTPUT_PROTOCOL = JSONValueProtocol
    
    def fields(self, line):
        '''
        Reads and strips line.
        '''
        try:
            # Reads line & eliminates weird characters, skips header
            rdr = csv.reader([line])
            columns = rdr.next()
            for i in range(len(columns)):
                columns[i] = columns[i].strip("'\"\/\\").upper()
            if columns[0] != 'id':
                organization = columns[ORGANIZATION]
            else:
                organization = None
        except: 
            organization = None
        
        return organization
    
    def similarity_score(self, string1, string2):
        '''
        Performs an analysis to determine the similarity of two strings.
        '''
        return fuzz.token_set_ratio(string1, string2)
    
    def configure_options(self):
        '''
        Makes csv containing Fortune 500 companies available to mapper.
        
        Use: Add --ancillary='FILE-PATH' as an option when running MRJob.
        '''
        super(fortune_json_builder, self).configure_options()
        self.add_file_option('--ancillary')
    
    def mapper_init(self):
        '''
        Gets Fortune 500 list to use for filtering/entity resolution.
        '''
        self.companies = []
        with open(self.options.ancillary, 'rU') as f:
            reader = csv.reader(f, dialect=csv.excel_tab)
            for row in reader: 
                if len(row) > 0: 
                    self.companies.append(row[0])
    
    def mapper(self, _, line):
        '''
        For each line in file, checks if organization name matches any of the
        companies within the companies dictionary using a similarity metric.
        '''
        organization = self.fields(line)
        for c in self.companies: 
            f500_score = self.similarity_score(c, organization)
            if f500_score > SIMILARITY_THRESHOLD: 
                yield c, organization
                
    def reducer_init(self):
        '''
        Initializes return value.
        '''
        self.rv = {}
    
    def reducer(self, company, alias):
        '''
        Adds each {Instance: AUTHNAME} to return value.
        '''
        for name in alias: 
            self.rv[name] = company
        
    def reducer_final(self):
        '''
        Yields final JSON.
        '''
        yield None, self.rv
        
if __name__ == '__main__':
    fortune_json_builder.run()
        
        
        
        
        
        
