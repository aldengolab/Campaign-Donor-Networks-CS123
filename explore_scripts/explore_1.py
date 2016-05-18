'''
1. Sum donations by donor over time
'''
from mrjob.job import MRJob
import heapq


AMOUNT = 8
CONTRIBUTOR_NAME = 10
K = 50


class total_donations(MRJob):
    def fields(self,line):
        '''Contructs Fields from CSV File'''
        fields = line.split(",")
        #ignore header
        try:
            amount = float(fields[AMOUNT])
            donor = fields[CONTRIBUTOR_NAME]
        except:
            amount = None
            donor = None
   
        # if  fields[AMOUNT] == 'amount':
        #     amount = None
        # else:
        #     amount = float(fields[AMOUNT])
        # if fields[CONTRIBUTOR_NAME] == "contributor_name":
        #     donor = None
        # else:
        #     donor = fields[CONTRIBUTOR_NAME]

        return donor, amount

    def mapper(self, _, line):
        '''Yields the Name on Each line  and the value 1'''
        donor, amount = self.fields(line)
        if donor and amount:
            yield donor, amount

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
        donor = str(donor)
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