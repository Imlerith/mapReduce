# -*- coding: utf-8 -*-
"""
Created on Sat Jun 11 20:58:17 2016

@author: nasekin
"""
#import os
#os.chdir('C:/Documents/Python Scripts/assignment3')

import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # key: document identifier
    # value: document contents
    key   = 1
    value = [str(record[0]),str(record[1])]
    mr.emit_intermediate(key,value)
    

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    rslt = []
    for v in list_of_values:       
        rslt.append(v)
        rslt.append(list(reversed(v)))

    for line in rslt:
        if rslt.count(line) < 2:
            mr.emit(tuple(line))

# =============================
if __name__ == '__main__':
  inputdata = open('friends.json')
  mr.execute(inputdata, mapper, reducer)

#count = 0  
#for line in inputdata:
#    print line
#    count += 1
#
#lst = ['a','b','c','d','d']
#for line in lst:
#    if lst.count(line) < 2:
#        print line
    
