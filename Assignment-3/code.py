import psycopg2

# CREATE TABLE Ratings(
#    UserID int,
#    MovieID int,
#    Rating float CHECK ( Rating >= 0 and Rating <= 5),
#    PRIMARY KEY (UserID, MovieID)
# );

# Universal Variables
tableName = 'ratings'

def create_connection():

	connection = psycopg2.connect(user="nithiya",
									  password="saibaba",
									  host="127.0.0.1",
									  port="5432",
									  database="assignment3")
	
	return connection

	# finally:
	# 	#closing database connection.
	# 		if(connection):
	# 			cursor.close()
	# 			connection.close()
	# 			print("PostgreSQL connection is closed")


def create_table( connection):

	try:
		cursor = connection.cursor()

		# Print PostgreSQL Connection properties
		print( 'Connection Successfully Established.')
		print ( connection.get_dsn_parameters(),"\n")

		# If the table already exits, delete it
		dropTable = 'DROP TABLE IF EXISTS ' + tableName
		cursor.execute( dropTable)

		create_table_query = '''CREATE TABLE ratings
			  (userid INT     NOT NULL,
			  movieid           INT    NOT NULL,
			  rating         FLOAT); '''

		cursor.execute( create_table_query)
		
		connection.commit()
		print( 'Table Successfully Created.')
		cursor.close()

	except (Exception, psycopg2.Error) as error :
		print ("Error while connecting to PostgreSQL", error)

def load_ratings( connection, filepath):

	cursor = connection.cursor()

	with open( filepath,'r') as inputFile:
		for row in inputFile:
			line = ','.join(row.split('::')[:3])
			insert_query = 'INSERT INTO ' + tableName + ' values(' + line + ');'
			cursor.execute(insert_query)
	
	connection.commit()
	print( 'Data successfully inserted.')
	cursor.close()

def range_partition( tableName, numofPartitions):
	


connection = create_connection()
create_table( connection)
load_ratings( connection, '/Users/nithiya/Documents/ASU Studies/Spring 2020/Data Processing at Scale/Assignment-3/ml-10M100K/ratings.dat')


