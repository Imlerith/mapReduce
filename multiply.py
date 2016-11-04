# -*- coding: utf-8 -*-
"""
Created on Sun Jun 12 02:58:39 2016

@author: nasekin
"""
#
#import os
#os.chdir('C:/Documents/Python Scripts/assignment3')

import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # key: document identifier
    # value: document contents
    for i in range(5):
        if str(record[0]) == 'a':
            key = (int(record[1]),i)
            value = (str(record[0]),int(record[1]),int(record[2]),int(record[3]))
            mr.emit_intermediate(key,value)
        elif str(record[0]) == 'b':
            key = (i,int(record[2]))
            value = (str(record[0]),int(record[1]),int(record[2]),int(record[3]))
            mr.emit_intermediate(key,value)
    
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    for ii in range(5):
        for kk in range(5):
            
            vl = 0
            for v1 in list_of_values: 

                for v2 in list_of_values:

                    if v1[0] == 'a' and v2[0] == 'b' and v1[2] == v2[1]  and v1[1] == ii and v2[2] == kk:
                        prod = v1[3]*v2[3]
                        vl += prod
            if vl != 0:
                mr.emit((key[0],key[1],vl))


# =============================
if __name__ == '__main__':
  inputdata = open('matrix.json')
  mr.execute(inputdata, mapper, reducer)

 
#for line in inputdata:
#    print line

#
#lst = ['a','b','c','d','d']
#for line in lst:
#    if lst.count(line) < 2:
#        print line