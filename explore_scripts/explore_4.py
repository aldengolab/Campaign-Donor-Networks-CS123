#!/usr/bin/env python

from mrjob.job import MRJob
import csv

class Match_Donors_as_Recipients(MRJob):
    '''
    Given a list of donor/recipient pairs, matches the donors to recipients
    and appends.
    '''
    def mapper(self, _, line):
        '''
