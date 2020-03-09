#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading


def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Function to Parallel sort the InputTable on SortingColumnName and store results in OutputTable
	cur = openconnection.cursor()
	cur.execute('SELECT MAX(' + SortingColumnName + '), MIN(' + SortingColumnName + ') FROM ' + InputTable + ';')
	max_col_val, min_col_val = cur.fetchone()
	num_threads = 5
	delta = float(max_col_val - min_col_val) / num_threads
	TEMP_TABLE_PREFIX = 'temp_table_sort'
	thread_list = [0] * num_threads
	for i in range(num_threads):
		min_range = min_col_val + i * delta
		max_range = min_range + delta
		thread_list[i] = threading.Thread(target=sortTable, args=(InputTable, TEMP_TABLE_PREFIX, SortingColumnName, min_range, max_range, i, openconnection))
		thread_list[i].start()
	cur.execute('DROP TABLE IF EXISTS ' + OutputTable + ';')
	cur.execute('CREATE TABLE ' + OutputTable + ' ( LIKE ' + InputTable + ' INCLUDING ALL );')
	for i in range(num_threads):
		thread_list[i].join()
		temp_table_name = TEMP_TABLE_PREFIX + str(i)
		cur.execute('INSERT INTO ' + OutputTable +' SELECT * FROM ' + temp_table_name + ';')
	cur.close()
	openconnection.commit()


def sortTable(InputTable, temp_table_prefix, SortingColumnName, min_range, max_range, i, openconnection):
	#Function to sort a single partition based on SortingColumnName using a thread
	cur = openconnection.cursor()
	temp_table_name = temp_table_prefix + str(i)
	cur.execute('DROP TABLE IF EXISTS ' + temp_table_name + ';')
	cur.execute('CREATE TABLE ' + temp_table_name + ' ( LIKE ' + InputTable + ' INCLUDING ALL);')
	if i==0:
		cur.execute('INSERT INTO ' + temp_table_name +' SELECT * FROM ' + InputTable + ' WHERE ' + SortingColumnName + ' >= '+ str(min_range) + ' AND ' + SortingColumnName + ' <= ' + str(max_range) + ' ORDER BY ' + SortingColumnName + ' ASC;')
	else:
		cur.execute('INSERT INTO ' + temp_table_name +' SELECT * FROM ' + InputTable + ' WHERE ' + SortingColumnName + ' > '+ str(min_range) + ' AND ' + SortingColumnName + ' <= ' + str(max_range) + ' ORDER BY ' + SortingColumnName + ' ASC;')


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Function to Parallel join InputTable1 and InputTable2 on Table1JoinColumn and Table2JoinColumn and store results in OutputTable
	cur = openconnection.cursor()
	cur.execute('SELECT MAX(' + Table1JoinColumn + '), MIN(' + Table1JoinColumn + ') FROM ' + InputTable1 + ';')
	max_col_val1, min_col_val1 = cur.fetchone()
	cur.execute('SELECT MAX(' + Table2JoinColumn + '), MIN(' + Table2JoinColumn + ') FROM ' + InputTable2 + ';')
	max_col_val2, min_col_val2 = cur.fetchone()
	num_threads = 5
	max_col_val = max(max_col_val1, max_col_val2)
	min_col_val = min(min_col_val1, min_col_val2)
	delta = float(max_col_val - min_col_val) / num_threads
	cur.execute('SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'' + InputTable1 + '\';')
	schema1 = cur.fetchall()
	cur.execute('SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'' + InputTable2 + '\';')
	schema2 = cur.fetchall()
	thread_list = [0] * num_threads
	TEMP_TABLE1_PREFIX = 'temp_table1_join'
	TEMP_TABLE2_PREFIX = 'temp_table2_join'
	TEMP_OUTPUT_TABLE_PREFIX = 'temp_output_table_join'
	for i in range(num_threads):
		min_range = min_col_val + i * delta
		max_range = min_range + delta
		thread_list[i] = threading.Thread(target=joinTable, args=(InputTable1, InputTable2, schema1, schema2, TEMP_TABLE1_PREFIX, TEMP_TABLE2_PREFIX, Table1JoinColumn, Table2JoinColumn, TEMP_OUTPUT_TABLE_PREFIX, min_range, max_range, i, openconnection))
		thread_list[i].start()
	cur.execute('DROP TABLE IF EXISTS ' + OutputTable + ';')
	cur.execute('CREATE TABLE ' + OutputTable + ' ( LIKE ' + InputTable1 + ' INCLUDING ALL);')
	quer = "ALTER TABLE " + OutputTable + " "
	for j in range(len(schema2)):
		if j != len(schema2)-1:
			quer += "ADD COLUMN " + schema2[j][0] + " " + schema2[j][1] + ","
		else:
			quer += "ADD COLUMN " + schema2[j][0] + " " + schema2[j][1] + ";"
	cur.execute(quer)
	for i in range(num_threads):
		thread_list[i].join()
		temp_table1_name = TEMP_TABLE1_PREFIX + str(i)
		temp_table2_name = TEMP_TABLE2_PREFIX + str(i)
		temp_output_table_name = TEMP_OUTPUT_TABLE_PREFIX + str(i)
		quer = "INSERT INTO " + OutputTable +" SELECT * FROM " + temp_output_table_name + ";"	
		cur.execute(quer)
	cur.close()
	openconnection.commit()

	
def joinTable(InputTable1, InputTable2, schema1, schema2, temp_table1_prefix, temp_table2_prefix, Table1JoinColumn, Table2JoinColumn, temp_output_table_prefix, min_range, max_range, i, openconnection):
	#Function to join two single partition based on Table1JoinColumn and Table2JoinColumn using a thread	
	cur = openconnection.cursor()
	temp_table1_name = temp_table1_prefix + str(i)
	temp_table2_name = temp_table2_prefix + str(i)
	temp_output_table_name = temp_output_table_prefix + str(i)
	cur.execute('DROP TABLE IF EXISTS ' + temp_table1_name + ';')
	quer = 'CREATE TABLE ' + temp_table1_name + ' ( LIKE ' + InputTable1 + ' INCLUDING ALL);'
	cur.execute(quer)
	cur.execute('DROP TABLE IF EXISTS ' + temp_table2_name + ';')
	quer = 'CREATE TABLE ' + temp_table2_name + ' ( LIKE ' + InputTable2 + ' INCLUDING ALL);'
	cur.execute(quer)
	cur.execute('DROP TABLE IF EXISTS ' + temp_output_table_name + ';')
	quer = 'CREATE TABLE ' + temp_output_table_name + ' ( LIKE ' + InputTable1 + ' INCLUDING ALL);'
	cur.execute(quer)
	quer = 'ALTER TABLE ' + temp_output_table_name + ' '
	for j in range(len(schema2)):
		if j != len(schema2)-1:
			quer += 'ADD COLUMN ' + schema2[j][0] + ' ' + schema2[j][1] + ','
		else:
			quer += 'ADD COLUMN ' + schema2[j][0] + ' ' + schema2[j][1] + ';'
	cur.execute(quer)
	if i==0:
		quer = 'INSERT INTO ' + temp_table1_name +' SELECT * FROM ' + InputTable1 + ' WHERE ' + Table1JoinColumn + ' >= '+ str(min_range) + ' AND ' + Table1JoinColumn + ' <= ' + str(max_range) + ';'
		cur.execute(quer)
		quer = 'INSERT INTO ' + temp_table2_name +' SELECT * FROM ' + InputTable2 + ' WHERE ' + Table2JoinColumn + ' >= '+ str(min_range) + ' AND ' + Table2JoinColumn + ' <= ' + str(max_range) + ';'
		cur.execute(quer)
	else:
		quer = 'INSERT INTO ' + temp_table1_name +' SELECT * FROM ' + InputTable1 + ' WHERE ' + Table1JoinColumn + ' > '+ str(min_range) + ' AND ' + Table1JoinColumn + ' <= ' + str(max_range) + ';'
		cur.execute(quer)
		quer = 'INSERT INTO ' + temp_table2_name +' SELECT * FROM ' + InputTable2 + ' WHERE ' + Table2JoinColumn + ' > '+ str(min_range) + ' AND ' + Table2JoinColumn + ' <= ' + str(max_range) + ';'
		cur.execute(quer)
	quer = 'INSERT INTO ' + temp_output_table_name + ' SELECT * FROM ' + temp_table1_name + ' INNER JOIN ' + temp_table2_name + ' ON ' + temp_table1_name + '.' + Table1JoinColumn + ' = ' + temp_table2_name + '.' + Table2JoinColumn + ';'
	cur.execute(quer)


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
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
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


