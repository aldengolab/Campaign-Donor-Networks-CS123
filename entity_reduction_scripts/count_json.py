'''
This file takes in a json containg the canonical names and aliases and computes summary statistics 
'''
import sys
import json
import re
import os
import heapq


K = 10
def find_aliases(aliases, value):
    rv = []
    for key, val in aliases.items():
        if val  == value:
            rv.append(key)
    return rv
  


def run(filename):
    '''create statistics  on the json alias file'''
    with open(filename, 'rU') as f:
        aliases = json.load(f)

    count = {}
    vals = aliases.values()

    for val in vals:
        if val in count:
            count[val] = count[val] +1
        else:
            count[val] = 1

    h = [(0, " ")] * K
    heapq.heapify(h)
    for key, val in count.items():
        replacement = (val, key) 
        if replacement > min(h):
            heapq.heapreplace(h,replacement)
    print "There are", len(count.keys()),  "unique businesses represented in the json \n"
    print "There are", len(aliases),  "aliases in the json \n"
    print "The top 10 companies with the most aliases are: \n"
    h = list(h)
    h.sort(reverse=True)
    for i in h:
        print i[1], "had " , i[0], " aliases \n"
    print "Here are some the aliases \n"
    att = find_aliases(aliases,'AT&T')
    coke = find_aliases(aliases,'Coca-Cola Enterprises, Inc.')
    sachs = find_aliases(aliases,'The Goldman Sachs Group, Inc.')
    print(att, "\n")
    print(coke, "\n")
    print(sachs, "\n")







if __name__ == "__main__":
    assert '.json' in sys.argv[1]
    run(sys.argv[1])