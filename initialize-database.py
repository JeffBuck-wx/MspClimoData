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
    
    status = True
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS climo_test (climoID int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, stationID VARCHAR(12) NOT NULL, date DATETIME NOT NULL, high TINYINT(4) DEFAULT NULL, low TINYINT(4) DEFAULT NULL, qpf DECIMAL(4,2) DEFAULT NULL, snow DECIMAL(3,1) DEFAULT NULL, depth TINYINT(3) DEFAULT NULL, winter DECIMAL(6,2) NOT NULL) ENGINE=Aria DEFAULT CHARSET=latin1;")
    except mariadb.Error as error:
        print("Error: {}".format(error))
        status = False
    
    
    return status



start = time.time()
print("Starting script")

# Remove old climo table
#drop_climo_table()


# Loop through the climo files
number_of_files = 15
for i in range(1,number_of_files + 1):
    print("File #%s" % i)

    # Set file name and open file
    climo_file = "/home/jeff/Data/Climo/MspClimoData-%s.csv" % i
    fh = open(climo_file,'r')

    # read lines
    for line in fh.readlines():
        # strip extra white space/line break
        line = line.strip()
        
        # Skip comments
        if line[0] == "#":
            #print('Comment.')
            continue

        # split the line
        climo = line.split(",")
        print(climo)
    


    fh.close()
        

end = time.time()
print("Ending script")
print("Run time: %.4f" % (end-start))


