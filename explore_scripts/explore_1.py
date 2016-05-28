'''
1. Sum donations by donor over time
'''
from mrjob.job import MRJob
import heapq
import csv


ID = 0
AMOUNT = 8
CONTRIBUTOR_NAME = 10
SEAT = 36
K = 50
DONOR_TYPE = 12


class total_donations(MRJob):
    def fields(self,line):
        '''Contructs Fields from CSV File'''
        try:
            rdr = csv.reader([line])
            columns = rdr.next()
            for i in range(len(columns)):
                columns[i] = columns[i].strip("'\"\/").lower()
            #ignore header
            if columns[0] != 'id':
                amount = float(columns[AMOUNT])
                donor = columns[CONTRIBUTOR_NAME].lower()
                donor_type = columns[DONOR_TYPE]
            if columns[0] == 'id':
                amount = None
                donor = None
                donor_type = None
        except Exception as e:
            amount = None
            donor = None
            donor_type =  None

        return donor, amount, donor_type

    def mapper(self, _, line):
        '''Yields the Name on Each line  and the value 1'''
        donor, amount, donor_type = self.fields(line)
        if donor and amount:
            yield (donor_type,donor),amount

    def combiner(self, donor, amount):
        ''' Combines names '''
        yield donor, sum(amount)

    def reducer_init(self):
        ''' initializes a heap to keep track of the top K results '''
        h =  [(0," ")] * K
        heapq.heapify(h)
        self.h = h



    def reducer(self, donor, amount):
        ''' replaces min val of the heap if the amount number is greater than the min value '''
        if donor[0] == 'c':
            donor = str(donor[1])
            amount = sum(amount)
            replacement = (amount, donor)
            if replacement > min(self.h):
                if donor:
                    heapq.heapreplace(self.h,replacement)



    def reducer_final(self):
        ''' reverse sorts the heap and yields the tuple containing the number of visits and names of the Top K '''
        top = list(self.h)
        top.sort(reverse = True)
        for i in top:
            yield i



if __name__ == '__main__':
    total_donations.run()