#!/usr/bin/env python

from mrjob.job import MRJob
import math

N = 500000.0

class sample_records(MRJob):
    '''
    Sample records to N from full sample.
    '''
    def mapper(self, _, line):
        rand = random.uniform(0,1)
        if rand <= (1.0/N):
            yield line, ''
