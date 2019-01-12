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
        cursor.execute(
                """CREATE TABLE IF NOT EXISTS climo_test (
                    climoID int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, 
                    stationID VARCHAR(12) NOT NULL, 
                    date DATETIME NOT NULL, 
                    high DECIMAL(4,2) DEFAULT NULL, 
                    low DECIMAL(4,2) DEFAULT NULL, 
                    qpf DECIMAL(4,2) DEFAULT NULL, 
                    trace_qpf TINYINT(1) DEFAULT "0", 
                    snow DECIMAL(4,2) DEFAULT NULL, 
                    trace_snow TINYINT(1) DEFAULT "0", 
                    depth DECIMAL(4,2) DEFAULT NULL, 
                    trace_depth TINYINT(1) DEFAULT "0"
                ) 
                ENGINE=Aria DEFAULT CHARSET=latin1;"""
                )
    except mariadb.Error as error:
        print("Error: {}".format(error))
        status = False
    
    
    return status


def write_data_file(fname,dlist,TRUNCATE=False):
    if TRUNCATE:
        fmode = 'w'
    else:
        fmode = 'a'

    with open(fname,fmode) as f:
        f.write(','.join(dlist) + '\n')
        

    


def process_climo_data(stid,climo):
    """
    Process climo data to be ready for database insertion.
    Accepts: climo list(data,high,low,qpf,snow,snowdepth)
    Returns: cleansed list
    """

    # station id
    stid = quote(stid)

    # date - good as is
    date = quote(climo[0])

    # high & low 
    high = parse_temperature(climo[1])
    low  = parse_temperature(climo[2])

    # qpf, snow, snowdepth
    qpf, trace_qpf = parse_qpf(climo[3])
    snow, trace_snow = parse_qpf(climo[4])
    snowdepth, trace_snowdepth = parse_qpf(climo[4])


    return [stid,date,high,low,qpf,trace_qpf,snow,trace_snow,snowdepth,trace_snowdepth]



def quote(s):
    return '"' + s + '"'



def parse_temperature(t):
    """ 
    identify missing values
    return temperature or NULL
    """
    if t == "M":
        value = "NULL"
    else:
        value = t
    return quote(value)



def parse_qpf(q):
    """ 
    Identify trace or missing values.
    Return: qpf value or null, trace (0|1)
    """
    if q == "M":
        value = "NULL"
        trace = "0"
    elif q == "T":
        value = "0.00"
        trace = "1"
    else:
        value = q
        trace = "0"

    return quote(value), quote(trace)




start = time.time()
print("Starting script")

# Remove old climo table
drop_climo_table()

# Create new table
create_climo_table()



# Loop through the climo files
number_of_files = 15
station_id = 'kmsp'

# set up and clear data file
data_file = '/home/jeff/Data/Climo/climo_%s.csv' % station_id
with open(data_file,'w'):
    pass



for i in range(1,number_of_files + 1):
    print("File #%s" % i)

    # Set file name and open file
    climo_file = "/home/jeff/Data/Climo/MspClimoData-%s.csv" % i
    fh = open(climo_file,'r')

    # read lines
    for line in fh.readlines():
        # strip extra white space/line break, remove qoutes
        line = line.strip().replace('"','')
        
        # Skip comments
        if line[0] == "#":
            #print('Comment.')
            continue

        # split the line
        climo = line.split(",")
        write_data_file(data_file,process_climo_data(station_id,climo))
        


    fh.close()
        

end = time.time()
print("Ending script")
print("Run time: %.4f" % (end-start))


