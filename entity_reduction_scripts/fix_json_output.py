# Fixes JSON output from MR to a single JSON file

import json

def transform_file(filename, output_filename):
    '''
    '''
    f = open(filename, 'rU')
    w = open(output_filename, 'w')
    for i in range(20000000):
        line = json.loads(f.next())
        w.append()
        
    f.close()
    w.close()
        
