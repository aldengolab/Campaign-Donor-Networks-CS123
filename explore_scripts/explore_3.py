#!/usr/bin/env python

from mrjob.job import MRJob
import csv

class Donor_Pairs(MRJob):
    '''
    Lists all donor pairs from file.
    '''
    def mapper(self, _, line):
        '''
        Grabs contributor and recipient from line.
        '''
        try:
            # Use reader to account for commas within quotation marks
            # This code is broken, not sure why
            rdr = csv.reader([line])
            columns = rdr.next()
            for i in range(len(columns)):
                columns[i] = columns[i].strip("'\"\/").lower()
            # columns = line.split(',')
            if columns[0] != 'id':
                contributor_name = columns[10]
                recipient_id = columns[25]
                if contributor_name != '':
                    yield contributor_name, recipient_id
        except IndexError as e:
            yield '', ''

    def combiner(self, contributor_name, recipient_id):
        '''
        For each contributor, find unique recipients.
        '''
        recipients = set([])
        for r in recipient_id:
            recipients.add(r)
        for r in recipients:
            yield contributor_name, r

    def reducer(self, contributor_name, recipient_id):
        '''
        Find final unique recipients list.
        '''
        recipients = set([])
        for r in recipient_id:
            recipients.add(r)
        for r in recipients:
            yield contributor_name, r

if __name__ == '__main__':
    Donor_Pairs.run()
