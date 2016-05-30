
from mrjob.job import MRJob
from mrjob.protocol import ReprValueProtocol
import random
import string


class sample_records(MRJob):
    '''
    Sample records to N from full sample.
    '''
    OUTPUT_PROTOCOL = ReprValueProtocol
    
    def mapper(self, _, line):
        line = filter(lambda x: x in string.printable, line)
        fields = line.split()
        if  fields[0] == 'id':
            yield None, line
        else:
            rand = random.randrange(1,100)
            if rand == 1:
                yield None, line

if __name__ == '__main__':
    sample_records.run()
