# Fixes JSON output from MR to a single JSON file

import json
import csv
from jsonmerge import merge 
import sys
import pprint

rv = {}
rv['filler']  = 'Replace at end'


def transform_file(filename):
    rv = {}
    rv['filler']  = 'Replace at end'
    with open(filename, 'rU') as f:
        rdr = csv.reader(f, delimiter = "\n")
        for j in rdr:
            rv = merge(rv,j)
    print rv[0]
    



if __name__ == "__main__":
    '''takes a json file containing multiple jsons as the input'''
    assert '.json' in sys.argv[1]
    transform_file(sys.argv[1])
