#!/usr/bin/env python


import sys
import os
from math import log,sqrt

D=10.0
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    wf,nNm=line.split('\t',1)
    word, file = wf.split(' ',1)
    n,N,m=nNm.split(' ',2)
    n=float(n)
    N=float(N)
    m=float(m)
    tfidf= (n/N)*log(D/m)
    print '%s\t%s\t%s' % (word,file,round(tfidf,6))