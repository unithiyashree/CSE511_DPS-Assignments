#!/usr/bin/python2.7
#
# Assignment2 Interface
#

"""
Author : Nithiya Shree Uppara
ASU ID : 1215229834
Assignment No: Assignment 4
"""

import psycopg2
import os
import sys

RANGE_TABLE_PREFIX = 'RangeRatingsPart'
RROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursor = openconnection.cursor()
    output_fileName = "RangeQueryOut.txt"
    final_output = []
    final_output.append( ["PartitionName", "UserID", "MovieID", "Rating"])

    query1 = " SELECT * FROM RangeRatingsMetadata ;"
    cursor.execute( query1)
    all_data1 = cursor.fetchall()
    output_tables = []

    for row in all_data1:
    	if not (row[2] <= ratingMinValue or row[1] >= ratingMaxValue):
    		output_tables.append( RANGE_TABLE_PREFIX + str(row[0]))

    for tableName1 in output_tables:
   		cond_query1 = "SELECT * FROM " + tableName1 + " WHERE RATING >= " + str(ratingMinValue) + " AND RATING <= " + str(ratingMaxValue) + ";"
   		cursor.execute( cond_query1)
   		output1 = cursor.fetchall()
   		for j in output1 :
   			final_output.append( [tableName1] + list(j))


   	query2 = " SELECT partitionnum FROM RoundRobinRatingsMetadata ;"
   	cursor.execute( query2)
    parts = cursor.fetchall()[0][0]

    for i in range( 0, parts):
    	tableName2 = RROBIN_TABLE_PREFIX + str(i)
    	cond_query2 = "SELECT * FROM " + tableName2 + " WHERE RATING >= " + str(ratingMinValue) + " AND RATING <= " + str(ratingMaxValue) + ";"
    	cursor.execute( cond_query2)
    	output2 = cursor.fetchall()
    	for k in output2:
    		final_output.append( [tableName2] + list(k))

    writeToFile( output_fileName, final_output)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()
    output_fileName = "PointQueryOut.txt"
    final_output = []
    final_output.append( ["PartitionName", "UserID", "MovieID", "Rating"])

    query1 = " SELECT * FROM RangeRatingsMetadata ;"
    cursor.execute( query1)
    all_data1 = cursor.fetchall()
    output_tables = []

    for row in all_data1:
    	if ( ( row[0] == 0 and (ratingValue >= row[1]) and ( ratingValue <= row[2])) or ( row[0] != 0 and ( ratingValue > row[1]) and ( ratingValue <= row[2]))):
    		output_tables.append( RANGE_TABLE_PREFIX + str(row[0]))

    for tableName1 in output_tables:
   		cond_query1 = "SELECT * FROM " + tableName1 + " WHERE RATING = " + str(ratingValue) + ";"
   		cursor.execute( cond_query1)
   		output1 = cursor.fetchall()
   		for j in output1 :
   			final_output.append( [tableName1] + list(j))

    query2 = " SELECT partitionnum FROM RoundRobinRatingsMetadata ;"
   	cursor.execute( query2)
    parts = cursor.fetchall()[0][0]

    for i in range( 0, parts):
    	tableName2 = RROBIN_TABLE_PREFIX + str(i)
    	cond_query2 = "SELECT * FROM " + tableName2 + " WHERE RATING = " + str(ratingValue) + ";"
    	cursor.execute( cond_query2)
    	output2 = cursor.fetchall()
    	for k in output2:
    		final_output.append( [tableName2] + list(k))

    writeToFile( output_fileName, final_output)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
