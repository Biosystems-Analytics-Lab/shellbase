#from glob import glob
import sys

#from pandas import DataFrame, read_csv
#import pandas as pd
import psycopg2
import datetime

pg_password = sys.argv[1]
#open the db
try:
    conn = psycopg2.connect("dbname='shellbase' user='postgres' password='"+pg_password+"'")

except:
    print ("Failed to open the db.")
    

#Initiate the cursor
cursor = conn.cursor()

kml_file = open("shellbase_demo_query1.kml", "w")
query_file = open("shellbase_demo_query1.csv", "w")

header = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>fc values greater than 100</name>
    <Style id="paddle-a">
      <IconStyle>
        <Icon>
          <href>http://maps.google.com/mapfiles/ms/micons/red-dot.png</href>
        </Icon>
        <hotSpot x="32" y="1" xunits="pixels" yunits="pixels"/>
      </IconStyle>
    </Style>
"""
kml_file.write(header)


query = """select sample_datetime,areas.name,stations.id,stations.name,stations.lat,stations.long,value from samples,stations,areas
where stations.id = samples.station_id
  and areas.id = stations.area_id
and type = 1 and value > 100 order by sample_datetime;"""

cursor.execute(query)

result = cursor.fetchall()

query_file.write('sample_datetime,area_name,station_name,station_lat,station_long,fc,temp\n')

for row in result:

    sample_datetime = row[0]
    sample_datetime_kml = row[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    area_name = row[1]
    station_id = row[2]
    station_name = row[3]
    station_lat = row[4]
    station_long = row[5]
    sample_value = row[6]        

    #print (sample_datetime,station_name,station_lat,station_long,sample_value)

    query_file_row = str(sample_datetime)+','+area_name+','+station_name+','+str(station_lat)+','+str(station_long)+','+str(sample_value)
    query_file.write(query_file_row)

    #do cross-lookup for temp(type=3) based on sample row id
    query = """select value from samples
    where sample_datetime = '"""+str(sample_datetime)+"""' and station_id = """+str(station_id)+"""
    and type = 3;"""

    #print (query)
    cursor.execute(query)

    result = cursor.fetchall()

    temp = ''
    if result:
        temp = result[0][0]

    #suffix temp to csv outfile
    query_file.write(','+str(temp))    
    query_file.write('\n')
    
    body = """
        <Placemark>
          <name>fc:"""+str(sample_value)+' temp:'+str(temp)+"""</name>
          <description>"""+station_name+' '+str(sample_datetime)+"""</description>
          <TimeStamp>
            <when>"""+str(sample_datetime_kml)+"""</when>
          </TimeStamp>
          <styleUrl>#paddle-a</styleUrl>
          <Point>
            <coordinates>"""+str(station_long)+','+str(station_lat)+""",0</coordinates>
          </Point>
        </Placemark>
    """
    kml_file.write(body)

footer = """
 </Document>
</kml>
"""

kml_file.write(footer)
kml_file.close()
query_file.close()


# Close the cursor
cursor.close()

# Close the database connection
conn.close()

