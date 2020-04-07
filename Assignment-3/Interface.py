#!/usr/bin/python2.7
#
# Interface for the assignement
#
"""
Author : Nithiya Shree Uppara
ASU ID : 1215229834
Assignment No: Assignment 3
"""

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()

    # If the table already exits, delete it
    dropTable = 'DROP TABLE IF EXISTS ' + ratingstablename + ' ;'
    cursor.execute( dropTable)

    create_table_query = "CREATE TABLE " + ratingstablename + " (userid INT, movieid INT, rating  FLOAT) ;"

    cursor.execute( create_table_query) 
    # print( 'Table Successfully Created.')

    with open( ratingsfilepath,'r') as inputFile:
        for row in inputFile:
            line = ','.join(row.split('::')[:3])
            insert_query = 'INSERT INTO ' + ratingstablename + ' values(' + line + ');'
            cursor.execute(insert_query)
    
    openconnection.commit()
    # print( 'Data successfully inserted.')
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):

    cursor = openconnection.cursor()

    rangeofRating = 5/numberofpartitions
    roundedRange = round( rangeofRating, 2)

    start = 0
    end = roundedRange

    for i in range( 0, numberofpartitions):
        table_name = 'range_part' + str(i)

        if( i == 0):
            createQuery = "CREATE TABLE " + table_name + " AS  SELECT * FROM " + \
                     ratingstablename + " WHERE rating >= " + str(start) + " AND rating <= " + str(end) + ";"

        else :
            createQuery = "CREATE TABLE " + table_name + " AS  SELECT * FROM " + \
                         ratingstablename + " WHERE rating > " + str(start) + " AND rating <= " + str(end) + ";"
            
        dropPartition = "DROP TABLE IF EXISTS " + table_name + ";"
        cursor.execute(dropPartition)
        cursor.execute(createQuery)

        start = end
        end = start + roundedRange

    openconnection.commit()
    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):

    cursor = openconnection.cursor()

    for i in range( 0, numberofpartitions):
        table_name = 'rrobin_part' + str(i)
        # print( table_name)
        dropPartition = 'DROP TABLE IF EXISTS ' + table_name + ' ;'
        cursor.execute( dropPartition)

        query = 'CREATE TABLE ' + table_name + ' ( userid INT, movieid INT, rating FLOAT);'
        cursor.execute( query)

    cursor.execute( "SELECT * FROM ratings;")
    ratingsData = cursor.fetchall()

    partNum = 0
    
    for row in ratingsData:
        table_name = 'rrobin_part' + str(partNum)
        insertQuery = 'INSERT INTO ' + table_name + ' VALUES(' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]) + ');'
        cursor.execute(insertQuery)
        partNum = partNum + 1
        partNum  = ( partNum) % numberofpartitions

    openconnection.commit()
    cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    insertQuery = 'INSERT INTO ' + ratingstablename + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');'
    cursor.execute( insertQuery)

    # totalParts = count_partitions( 'rrobin_part', openconnection)
    cursor.execute( "SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + "rrobin_part" + "%';")
    totalParts = cursor.fetchone()[0]

    cursor.execute( "SELECT COUNT(*) FROM " + ratingstablename + ";" )
    totalRows = (cursor.fetchall())[0][0]
    part = (totalRows - 1) % totalParts

    table_name = 'rrobin_part' + str(part)
    insertQuery = 'INSERT INTO ' + table_name + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');'
    cursor.execute( insertQuery)

    openconnection.commit()
    cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    
    cursor = openconnection.cursor()

    insertQuery = 'INSERT INTO ' + ratingstablename + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');'
    cursor.execute( insertQuery)

    cursor.execute( "SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + "range_part" + "%';")
    totalParts = cursor.fetchone()[0]

    #totalParts = count_partitions( 'rrobin_part', openconnection)
    rangeofRating = 5/totalParts
    roundedRange = round(rangeofRating, 2)

    part = int(rating/roundedRange)
    if rating % roundedRange == 0 and part != 0:
        part = part - 1

    table_name = 'range_part' + str( part)
    insertQuery = 'INSERT INTO ' + table_name + ' VALUES(' +str(userid) + ',' + str(itemid) + ',' + str(rating) + ');'
    cursor.execute( insertQuery)

    openconnection.commit()
    cursor.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()

# connection = create_connection()
# loadRatings( ratingstablename, '/Users/nithiya/Documents/ASU Studies/Spring 2020/Data Processing at Scale/Assignment-3/ml-10M100K/ratings.dat', connection)
# rangePartition( ratingstablename, numberofpartitions, connection)
# roundRobinPartition(ratingstablename, numberofpartitions, connection)
# roundrobininsert(ratingstablename, userid, itemid, rating, connection)
# rangeinsert(ratingstablename, userid, itemid, rating, connection)
