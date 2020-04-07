#!/usr/bin/python2.7
#
# Assignment3 Interface
#

"""
Author : Nithiya Shree Uppara
ASU ID : 1215229834
Assignment No: Assignment 5
"""

import psycopg2
import os
import sys
import threading

def queryRangeofColumn(SortingColumnName,InputTable,openconnection):
	con = openconnection
	cursor = con.cursor()
	query = "SELECT MIN(" + SortingColumnName + "), MAX(" + SortingColumnName + ") FROM " + InputTable + ";"
	cursor.execute(query)
	values = cursor.fetchone()
	cursor.close()
	return values

def sortHelperTable(SortingColumnName,HelperTablePrefix,InputTable,min_range,max_range,i,openconnection):
	con = openconnection
	cursor = con.cursor()
	helper_sort_table_name = HelperTablePrefix + str(i)
	cursor.execute("drop table if exists " + helper_sort_table_name + ";")
	cursor.execute("create table " + helper_sort_table_name + " ( like " + InputTable + " including all );")
	if i!=0:
		cursor.execute("insert into "+helper_sort_table_name + " select * from " + InputTable + " where " + SortingColumnName + " > " + str(min_range) + " and " + SortingColumnName + " <= " + str(max_range) + " order by " + SortingColumnName + " asc;" )
	else:
		cursor.execute("insert into "+helper_sort_table_name + " select * from " + InputTable + " where " + SortingColumnName + " >= " + str(min_range) + " and " + SortingColumnName + " <= " + str(max_range) + " order by " + SortingColumnName + " asc;" )



# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
	#Implement ParallelSort Here.
	con = openconnection
	cursor = con.cursor()
	values = queryRangeofColumn(SortingColumnName,InputTable,openconnection)
	min_col_range = values[0]
	max_col_range = values[1]
	noOfThreads = 5
	threadRange = float(max_col_range-min_col_range)/noOfThreads

	HelperTablePrefix = "helper_sort_table"

	threads_list = []
	for i in range(noOfThreads):
		threads_list.append(0)

	for i in range(noOfThreads):
		min_range = min_col_range + i*threadRange
		max_range = min_range + threadRange
		threads_list[i] = threading.Thread(target = sortHelperTable, args=(SortingColumnName,HelperTablePrefix,InputTable,min_range,max_range,i,openconnection))
		threads_list[i].start()

	cursor.execute("drop table if exists " + OutputTable + ";")
	cursor.execute("create table " + OutputTable + " ( like " + InputTable + " including all );")

	for i in range(noOfThreads):
		threads_list[i].join()
		helper_sort_table_name = HelperTablePrefix + str(i)
		cursor.execute("insert into " + OutputTable + " select * from " + helper_sort_table_name + ";")
	cursor.close()
	con.commit()



def joinHelperTable(InputTable1,InputTable2,table1_schema,table2_schema,HelperTablePrefix1,HelperTablePrefix2,Table1JoinColumn,Table2JoinColumn,HelperOutputTable,min_range,max_range,i,openconnection):
	con = openconnection
	cursor = con.cursor()

	helper_join1_table = HelperTablePrefix1 + str(i)
	helper_join2_table = HelperTablePrefix2 + str(i)

	helper_output_table_join_name = HelperOutputTable + str(i)

	cursor.execute("drop table if exists " + helper_join1_table + ";")
	cursor.execute("drop table if exists " + helper_join2_table + ";")
	cursor.execute("drop table if exists " + helper_output_table_join_name + ";")

	cursor.execute("create table " + helper_join1_table + " ( like " + InputTable1 + " including all);")
	cursor.execute("create table " + helper_join2_table + " ( like " + InputTable2 + " including all);")

	cursor.execute("create table " + helper_output_table_join_name + " ( like " + InputTable1 + " including all);")

	query = "alter table " + helper_output_table_join_name + " "
	for vt in range(len(table2_schema)):
		if vt ==len(table2_schema)-1:
			query += "add column " + table2_schema[vt][0] + " " + table2_schema[vt][1] + ";"
		else:
			query += "add column " + table2_schema[vt][0] + " " + table2_schema[vt][1] + ","

	cursor.execute(query)

	if i!=0:
		cursor.execute("insert into "+helper_join1_table + " select * from " + InputTable1 + " where " + Table1JoinColumn + " > " + str(min_range) + " and " + Table1JoinColumn + " <= " + str(max_range) + ";")
		cursor.execute("insert into "+helper_join2_table + " select * from " + InputTable2 + " where " + Table2JoinColumn + " > " + str(min_range) + " and " + Table2JoinColumn + " <= " + str(max_range) + ";")
	else:
		cursor.execute("insert into "+helper_join1_table + " select * from " + InputTable1 + " where " + Table1JoinColumn + " >= " + str(min_range) + " and " + Table1JoinColumn + " <= " + str(max_range) + ";")
		cursor.execute("insert into "+helper_join2_table + " select * from " + InputTable2 + " where " + Table2JoinColumn + " >= " + str(min_range) + " and " + Table2JoinColumn + " <= " + str(max_range) + ";")

	cursor.execute("insert into " + helper_output_table_join_name + " select * from " + helper_join1_table + " inner join " + helper_join2_table + " on " + helper_join1_table + "." + Table1JoinColumn + " = " + helper_join2_table+ "."+Table2JoinColumn + ";")



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
	con = openconnection
	cursor = con.cursor()

	values= queryRangeofColumn(Table1JoinColumn,InputTable1,openconnection)
	min_col_range1,max_col_range1 = values[0],values[1]

	values= queryRangeofColumn(Table2JoinColumn,InputTable2,openconnection)
	min_col_range2,max_col_range2 = values[0],values[1]

	max_col_range = max(max_col_range1,max_col_range2)
	min_col_range = min(min_col_range1,min_col_range2)
	noOfThreads = 5

	threadRange = float(max_col_range-min_col_range)/noOfThreads

	cursor.execute("select column_name, data_type from information_schema.columns where table_name = '" + InputTable1 + "';")
	table1_schema = cursor.fetchall()

	cursor.execute("select column_name, data_type from information_schema.columns where table_name = '" + InputTable2 + "';")
	table2_schema = cursor.fetchall()

	threads_list = []
	for i in range(noOfThreads):
		threads_list.append(0)

	HelperTablePrefix1 = "helper_join1_table"
	HelperTablePrefix2 = "helper_join2_table"

	HelperOutputTable  = "helper_output_table_join"


	for i in range(noOfThreads):
		min_range = min_col_range + i*threadRange
		max_range = min_range + threadRange
		threads_list[i] = threading.Thread(target = joinHelperTable, args=(InputTable1,InputTable2,table1_schema,table2_schema,HelperTablePrefix1,HelperTablePrefix2,Table1JoinColumn,Table2JoinColumn,HelperOutputTable,min_range,max_range,i,openconnection))
		threads_list[i].start()

	cursor.execute("drop table if exists " + OutputTable + ";")
	cursor.execute("create table " + OutputTable + " ( like " + InputTable1 + " including all);")

	query = "alter table " + OutputTable + " "

	for vt in range(len(table2_schema)):
		if vt==len(table2_schema)-1:
			query += "add column " + table2_schema[vt][0] + " " + table2_schema[vt][1] + ";"
		else:
			query += "add column " + table2_schema[vt][0] + " " + table2_schema[vt][1] + ","

	cursor.execute(query)

	for i in range(noOfThreads):
		threads_list[i].join()
		helper_output_table_join_name = HelperOutputTable  + str(i)
		cursor.execute("insert into " + OutputTable + " select * from " + helper_output_table_join_name + ";")

	cursor.close()
	con.commit() 




