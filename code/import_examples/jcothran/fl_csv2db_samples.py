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

        print (file, df[0][i])
        full_station = df[14][i]

        #lookup station_id from stations table
        query_area = "SELECT id from stations where name = '"+str(full_station)+"'"
        cursor.execute(query_area)

        result = cursor.fetchall()

        if result: #station
            
            #print (result[0][0])
            station_id = result[0][0]

            #fill in other station fields
            sample_datetime = df[1][i]+' '+df[2][i]
            #print (sample_datetime)
            #add 1 hour to make EST timezone instead of CST
            sample_datetime = datetime.datetime.strptime(sample_datetime, '%Y-%m-%d %H:%M')+ datetime.timedelta(hours=1)
            #print (sample_datetime)

            #lookup sample_id from samples table to see if inserted already
            query_station = "SELECT id from samples where sample_datetime = '"+str(sample_datetime)+"' and station_id = "+str(station_id)
            #print (query_station)
            cursor.execute(query_station)
            result = cursor.fetchall()

            if not result: #sample row not inserted
                
                #surface station
                fc = float(df[6][i])
                sal = df[7][i]
                temp = df[8][i]
                do = df[9][i]
                ph = df[13][i]

                #================    
                #print (fc)
                if not math.isnan(fc):
                    query = "INSERT INTO samples (sample_datetime,station_id,type,units,value) VALUES (%s, %s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 1, 1, fc)

                    #print (values)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()

                #================    
                #print (sal)
                if not math.isnan(sal):
                    query = "INSERT INTO samples (sample_datetime,station_id,type,units,value) VALUES (%s, %s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 2, 2, sal)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()
                        
                #================    
                if not math.isnan(temp):
                    query = "INSERT INTO samples (sample_datetime,station_id,type,units,value) VALUES (%s, %s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 3, 3, temp)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()

                #================    
                if not math.isnan(do):           
                    query = "INSERT INTO samples (sample_datetime,station_id,type,units,value) VALUES (%s, %s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 5, 5, do)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()

                #================    
                #ph has no units
                if not math.isnan(ph):                          
                    query = "INSERT INTO samples (sample_datetime,station_id,type,value) VALUES (%s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 6, ph)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()            

                #================    
                #================    
                #bottom station
                sal_bottom = df[10][i]
                temp_bottom = df[11][i]
                do_bottom = df[12][i]

                #lookup station_bottom_id from stations table
                query_station = "SELECT id from stations where name = '"+full_station+"_B'"
                #print (query_station)
                cursor.execute(query_station)
                result = cursor.fetchall()
                station_id = result[0][0]

                #================    
                #print (sal_bottom)
                if not math.isnan(sal_bottom):
                    query = "INSERT INTO samples (sample_datetime,station_id,type,units,value) VALUES (%s, %s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 2, 2, sal_bottom)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()
                        
                #================    
                if not math.isnan(temp_bottom):
                    query = "INSERT INTO samples (sample_datetime,station_id,type,units,value) VALUES (%s, %s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 3, 3, temp_bottom)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()

                #================    
                if not math.isnan(do_bottom):           
                    query = "INSERT INTO samples (sample_datetime,station_id,type,units,value) VALUES (%s, %s, %s, %s, %s)"     
                    values = (sample_datetime, station_id, 5, 5, do_bottom)

                    # Execute sql Query
                    try:
                        cursor.execute(query, values)
                    except psycopg2.IntegrityError:
                        conn.rollback()
                    else:
                        conn.commit()
        
# Close the cursor
cursor.close()

# Close the database connection
conn.close()

