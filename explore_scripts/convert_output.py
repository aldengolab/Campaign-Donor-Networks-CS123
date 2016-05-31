# CS123
# Campaign Donation MRJob Project

# This file converst the output from F500_json_MR.py into a properly
# formatted json file.

import json
import sys

def transform(filename, new_filename):
    '''
    Takes a file with jsons in each line and outputs a single json.
    '''
    rv = {}
    with open(filename, 'rU') as f:
        content = f.readlines()
    for x in content: 
        line = dict(json.loads(x))
        for k in line:
            rv[k] = line[k]
    with open(new_filename, 'w') as fp:
        json.dump(rv, fp, indent = 4)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        transform(sys.argv[1], sys.argv[2])
    else: 
        print "USAGE: convert_output.py <FILENAME> <OUTPUT_PATH>"
