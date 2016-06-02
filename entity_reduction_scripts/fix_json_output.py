# Fixes JSON output from MR to a single JSON file

import json
import csv
from jsonmerge import merge 
import sys


def transform_file(filename):
    jsons = []
    #found at:
    num_lines = sum(1 for line in open(filename))

    i = 0
    f = open(filename, 'rU')
    while i < num_lines:
        line = f.next()
        jsons.append(line)
        i+=1
    f.close()
    
    first = json.loads(jsons[0])
    second = json.loads(jsons[1])
    rv = merge(first, second)
    for i in range(len(jsons)):
        if i != 0:
            rv = merge(rv, json.loads(jsons[i]))
            
    with open('output.json', 'w') as f: 
        json.dump(rv, f)

    


if __name__ == "__main__":
    '''takes a json file containing multiple jsons as the input'''
    assert '.json' in sys.argv[1]
    transform_file(sys.argv[1])
