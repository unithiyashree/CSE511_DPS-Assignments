#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
from Assignment1 import *
from crudOperations import *
from fileWriter import *


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    rangePartConst = "rangeratingspart"
    roundRobinConst = "roundrobinratingspart"
    outFileName = "RangeQueryOut.txt"
    numberOfPart = countData(openconnection,"rangeratingsmetadata")
    records = selectData(openconnection,"*","rangeratingsmetadata",None,None,None)
    tabstoQuery = []
    for x in records:
        if x[2] < ratingMinValue or x[1] >= ratingMaxValue:
            continue
        else:
            tabstoQuery.append(rangePartConst+str(x[0]))
    columns = ["rating","rating"]
    cond = [ratingMaxValue,ratingMinValue]
    oper = ["le","ge"]
    for tab in tabstoQuery:
        results = selectData(openconnection,"*",tab,columns,cond,oper)
        writeIntoFile(outFileName,tab,results)
    numPart = selectRRPartNum("roundrobinratingsmetadata",openconnection)
    results = []
    for i in range(numPart):
        results = selectData(openconnection,"*",roundRobinConst+str(i),columns,cond,oper)
        if len(results) != 0:
            writeIntoFile(outFileName,roundRobinConst+str(i),results)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    rangePartConst = "rangeratingspart"
    roundRobinConst = "roundrobinratingspart"
    outFileName = "PointQueryOut.txt"
    numberOfPart = countData(openconnection, "rangeratingsmetadata")
    records = selectData(openconnection, "*", "rangeratingsmetadata", None, None, None)
    tabstoQuery = []
    counter=0
    for x in records:
        counter+=1
        if (x[1] < ratingValue and x[2] >= ratingValue) or (counter==1 and ratingValue==x[1]):
            tabstoQuery.append(rangePartConst + str(x[0]))

    columns = ["rating"]
    cond = [ratingValue]
    oper = ["e"]
    for tab in tabstoQuery:
        results = selectData(openconnection, "*", tab, columns, cond, oper)
        writeIntoFile(outFileName, tab, results)
    numPart = selectRRPartNum("roundrobinratingsmetadata", openconnection)
    results = []
    for i in range(numPart):
        results = selectData(openconnection, "*", roundRobinConst + str(i), columns, cond, oper)
        if len(results) != 0:
            writeIntoFile(outFileName, roundRobinConst + str(i), results)

#RangeQuery("",1,2,getOpenConnection())
#PointQuery("",5,getOpenConnection())