# This file eliminates encoding issues in files with non ASCII characters.

import string

def fix_encoding(filename, new_filename):
    with open(filename, 'rU') as f:
        content = f.readlines()
        
    for i in range(len(content)): 
        content[i] = filter(lambda x: x in string.printable, content[i])

    with open(new_filename, 'w') as f: 
        f.writelines(content)
