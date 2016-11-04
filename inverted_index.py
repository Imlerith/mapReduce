# -*- coding: utf-8 -*-
"""
Created on Sat May 28 23:55:53 2016

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
    doc_id = record[0]
    text = record[1]
    words = text.split()
    for w in words:
      mr.emit_intermediate(w, str(doc_id))

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = []
    for v in list_of_values:
      total.append(v)
      total = list(set(total))
    mr.emit((key, total))

# =============================
if __name__ == '__main__':
  inputdata = open('books.json')
  mr.execute(inputdata, mapper, reducer)
  
  for line in inputdata:
    print line
    
import os
os.chdir('C:/Documents/Python Scripts/assignment3')
  
  
  
