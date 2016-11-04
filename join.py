# -*- coding: utf-8 -*-
"""
Created on Sun May 29 03:08:27 2016

@author: nasekin
"""


import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # key: document identifier
    # value: document contents
    key   = str(record[1])
    value = map(str,record)
    mr.emit_intermediate(key,value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    v = []
    for v1 in list_of_values:
        for v2 in list_of_values:
            if v1[1] == v2[1] and v1[0] != v2[0] and v1[0] == "order" and v1+v2 not in v:
                v.append(v1 + v2)
                mr.emit(v1 + v2)
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open('records.json')
  mr.execute(inputdata, mapper, reducer)
  
  
  

  
  
