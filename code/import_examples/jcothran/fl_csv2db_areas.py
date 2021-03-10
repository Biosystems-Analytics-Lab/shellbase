from glob import glob
import sys

from pandas import DataFrame, read_csv
import pandas as pd
import psycopg2
import datetime

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

        area = str(df[3][i])
        state = 'FL'

        #lookup area_id from areas table to see if inserted already
        query_area = "SELECT id from areas where name = '"+area+"' and state = '"+state+"'"
        cursor.execute(query_area)
        result = cursor.fetchall()

        if not result: 

            query = "INSERT INTO areas (name,state) VALUES (%s, %s)"            
            values = (area, state)
            #print (values)

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

