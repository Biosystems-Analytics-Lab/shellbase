from glob import glob
import sys

from pandas import DataFrame, read_csv
import pandas as pd
import psycopg2
import datetime
import math

pg_password = sys.argv[2]
#open the db
try:
    conn = psycopg2.connect("dbname='shellbase' user='postgres' password='"+pg_password+"'")

except:
    print ("Failed to open the db.")
    

#Initiate the cursor
cursor = conn.cursor()

file = sys.argv[1]

allFiles = glob(sys.argv[1]+"/*.csv")
for file in allFiles:

    print (file)

    #df = pd.read_excel(file, sheet_name="Sheet1",skiprows=2,header=None)
    df = pd.read_csv(file, skiprows=1,header=None)

    #print(df[1])

    for i in df.index:

        print (df[0][i])
        area = df[3][i]

        #lookup area_id from areas table
        query_area = "SELECT id from areas where name = '"+str(area)+"'"
        cursor.execute(query_area)

        result = cursor.fetchall()

        #print (result[0][0])
        area_id = result[0][0]

        #fill in other station fields
        full_station = df[14][i]
        lat = round(df[15][i],4)
        long = round(df[16][i],4)
        bottom_depth = df[5][i]

        #lookup station_id from stations table to see if inserted already
        query_station = "SELECT id from stations where name = '"+str(full_station)+"'"
        cursor.execute(query_station)
        result = cursor.fetchall()

        if not result and not math.isnan(lat): 

            #surface station
            query = "INSERT INTO stations (name,state,area_id,lat,long) VALUES (%s, %s, %s, %s, %s)"     
            values = (full_station, 'FL', area_id, lat, long)

            #print (values)

            # Execute sql Query
            
            try:
                cursor.execute(query, values)
            except psycopg2.IntegrityError:
                conn.rollback()
            else:
                conn.commit()

            #bottom station - same code just suffixing '_B' to station_name
            query = "INSERT INTO stations (name,state,area_id,lat,long,sample_depth_type,sample_depth) VALUES (%s, %s, %s, %s, %s, %s, %s)"     
            values = (full_station+'_B', 'FL', area_id, lat, long, 'B', bottom_depth)

            #print (values)

            # Execute sql Query
            
            try:
                cursor.execute(query, values)
            except psycopg2.IntegrityError:
                conn.rollback()
            else:
                conn.commit()
        else:
            print ('missing lat/long');
    
# Close the cursor
cursor.close()

# Close the database connection
conn.close()

