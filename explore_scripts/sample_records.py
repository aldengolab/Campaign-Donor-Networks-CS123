
from mrjob.job import MRJob
from mrjob.protocol import ReprValueProtocol
import random


class sample_records(MRJob):
    '''
    Sample records to N from full sample.
    '''
    OUTPUT_PROTOCOL = ReprValueProtocol
    
    def mapper(self, _, line):
        fields = line.split()
        if  fields[0] == 'id':
            yield line,  None
        rand = random.randrange(1,100)
        if rand == 1:
            yield None, line

if __name__ == '__main__':
    sample_records.run()
