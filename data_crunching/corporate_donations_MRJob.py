# CS123
# Campaign Donation MRJob Project

# This file takes a JSON of resolved entities of the form {instance:
# authoritative_name}, reads the full dataset, then extracts relevant 
# information for each authoritative name and prints to csv format.

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
import csv
import json

# Column indices for slicing
ID = 0
AMOUNT = 8
DATE = 9
CONTRIBUTOR_NAME = 10
CONTRIBUTOR_TYPE = 12
CONTRIBUTOR_EMPLOYER = 14
ORGANIZATION = 21
PARENT_ORGANIZATION = 23
RECIPIENT_NAME = 25
CANDIDATE_PARTY = 27
SEAT = 36
RESULT = 41

K = 50

class build_corporate_donations(MRJob):
    '''
    This job builds a dataset for corporate donations.
    '''
    OUTPUT_PROTOCOL = ReprValueProtocol

    def configure_options(self):
        '''
        Makes a file available to mapper.
        
        Use: Add --ancillary='FILE-PATH' as an option when running MRJob.
        '''
        super(fortune_json_builder, self).configure_options()
        self.add_file_option('--ancillary')
    
    def fields(self, line):
        '''
        Reads and strips line.
        '''
        yield_value = False
        try:
            # Reads line & eliminates weird characters, skips header
            rdr = csv.reader([line])
            columns = rdr.next()
            for i in range(len(columns)):
                columns[i] = columns[i].strip("'\"\/").upper()
            if columns[0] != 'id':
                name
                organization = columns[ORGANIZATION]
                parent = columns[PARENT_ORGANIZATION]
                recipient = columns[RECIPIENT_NAME]
                party = columns[CANDIDATE_PARTY]
                date = columns[DATE]
                amount = columns[AMOUNT]
                seat = columns[SEAT]
                result = columns[RESULT]
            else:
                organization = None
                parent = None
                recipient = None
                party = None
                date = None
                amount = None
                seat = None
                result = None
            if organization.lower() != parent.lower() and parent in \
             entity_dictionary:
                organization = entity_dictionary.get(parent)
                yield_value = True
            elif organization in entity_dictionary: 
                organization = entity_dictionary.get(organization)
                yield_value = True
            recipient = entity_dictionary.get(recipient, None)
            
        except: 
            organization = None
            recipient = None
            party = None
            date = None
            amount = None
            seat = None
            result = None
        
        if yield_value:
            return organization, recipient, party, date, amount, seat, result
    
    def mapper_init(self):
        '''
        Holds entity dictionary in memory for authoritative name lookup.
        '''
        with open(self.options.ancillary, 'rU') as f:
            entity_dictionary = json.load(f)
    
    def mapper(self, _, line):
        '''
        Reads line and concats strings. 
        '''
        organization, recipient, party, date, amount, seat, result = \
         self.fields(line)
        year = date.split('-')[0]
        month = date.split('-')[1]
        key = ','.join([organization, recipient, party, seat, result, month, 
        year])
        
        yield key, amount
        
    def combiner(self, corporation, amount):
        '''
        Combines at each node to optimize.
        '''
        yield key, sum(amount)
        
    def reducer(self, key, amount):
        '''
        '''
        total = sum(amount)
        rv = ','.join([key, total])
        yield None, rv
        
        
