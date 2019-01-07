#!/usr/bin/env python3
import mysql.connector as mariadb
import time



def drop_climo_table():
	"""
	Remove the climo_test table.
	"""

    mariadb_connection = mariadb.connect(user="WeatherPy", password="SnowStorm1991", database="weather")
    cursor = mariadb_connection.cursor()	
	
	status = True
	
	try:
		cursor.execute("DROP TABLE IF EXISTS climo_test;")
	except mariadb.Error as error:
		print("Error: {}".format(error))
		status = False
		
	return status




def create_climo_table():
    """
    Create MariaSQL table for KMSP climo data.
    """

    mariadb_connection = mariadb.connect(user="WeatherPy", password="SnowStorm1991", database="weather")
    cursor = mariadb_connection.cursor()
    
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS climo_test (climoID int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, stationID VARCHAR(12) NOT NULL, date DATETIME NOT NULL, high TINYINT(4) DEFAULT NULL, low TINYINT(4) DEFAULT NULL, qpf DECIMAL(4,2) DEFAULT NULL, snow DECIMAL(3,1) DEFAULT NULL, depth TINYINT(3) DEFAULT NULL, winter DECIMAL(6,2) NOT NULL) ENGINE=Aria DEFAULT CHARSET=latin1;")
    except mariadb.Error as error:
        print("Error: {}".format(error))


    return
	
	
	