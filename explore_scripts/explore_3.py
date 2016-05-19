'''
3. Pull all donor/election pairs
'''

from mrjob.job import MRJob
import csv

YEAR_START = 1998
YEAR_END = 2001

class donor_pairs(MRJob):

    def mapper(self, _, line):
        '''
        Grabs contributor and recipient from line.
        '''
        # Use reader to account for commas within quotation marks
        rdr = csv.reader([line])
        columns = rdr.next()
        if columns[0] != 'id' and int(columns[2]) >= YEAR_START and\
         int(columns[2]) <= YEAR_END:
            contributor_name = columns[10]
            recipient_id = columns[25]
            yield contributor_name, recipient_id

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
    donor_pairs.run()
