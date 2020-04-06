#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  5 22:10:48 2020

@author: stevenalsheimer
"""

from pyspark import SparkContext
import csv

def extractComplaints_CompMax(partId, list_of_records):
    if partId==0: 
        next(list_of_records) # skipping the first line, the header
    reader = csv.reader(list_of_records)
    for row in reader:
        if len(row)==18:#this is to parse out bad data
            (Date, Prod, Company) = (row[0], row[1], row[7]) # filter to keep the large schools
            Date = int(Date.split('-')[0])
            yield (Date, Prod, Company, 1)

def main(sc):
    COM_FS = 'complaints_sample.csv'

    Com = sc.textFile(COM_FS, use_unicode = True).cache()
    
    Comp_max_yr = Com.mapPartitionsWithIndex(extractComplaints_CompMax)
    
    Complaint = Comp_max_yr.map(lambda s: ((s[0],s[1],s[2]), s[3]))\
            .reduceByKey(lambda a,b: a+b)\
            .map(lambda s: ((s[0][0], s[0][1]),(1,s[1],s[1])))\
            .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], max(a[2],b[2])))\
            .map(lambda s: ((s[0][0],s[0][1],s[1][0],s[1][1]),s[1][2]))\
            .reduceByKey(max)\
            .map(lambda s: ((s[0][0],s[0][1],s[0][2],s[0][3]),(s[1],s[0][3])))\
            .mapValues(lambda x: 100*x[0]/x[1])\
            .map(lambda s: (s[0][0],s[0][1],s[0][3],s[0][2],s[1]))\
            .collect()
    with open(COM_out,'w') as myfile:
        wr = csv.writer(myfile)
        for item in Complaint:
            wr.writerow(item)
        
    return Complaint


if __name__ == "__main__":
    import sys
    sc = SparkContext()
    COM_FS = sys.argv[1]
    COM_out = sys.argv[2]
    main(sc)