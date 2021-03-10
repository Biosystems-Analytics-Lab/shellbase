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
    
except psycopg2.OperationalError as e:
        print ("Unable to connect!")
        print (e.pgcode)
        print (e.pgerror)
        print (e.diag.message_detail)
        sys.exit(1)

#Initiate the cursor
cursor = conn.cursor()

#print datetime.now statements for benchmarking time to complete
#print ('connected:'+str(datetime.datetime.now().strftime("%H:%M:%S")))

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

#multiple joins
#info on joins https://learnsql.com/blog/how-to-left-join-multiple-tables/
#select rows where fc > 100 and other associated(same sample_datetime,station_id) measurements ordered by sample_datetime
  
query = """select A.sample_datetime,areas.name as area_name,A.station_id,stations.name as station_name,stations.long,stations.lat,A.value as fc,B.value as temp,C.value as sal
      from samples A
      
      left join samples B 
        on A.sample_datetime = B.sample_datetime
        and A.station_id = B.station_id
        and B.type = 2
      
      left join samples C 
        on A.sample_datetime = C.sample_datetime
        and A.station_id = C.station_id
        and C.type = 3
      
      --add additional left joins here(samples D,...) for other measurement types
      
      left join stations
        on stations.id = A.station_id

      left join areas
        on areas.id = stations.area_id
      
      where A.type = 1 and A.value > 100
        
      order by sample_datetime;"""


cursor.execute(query)

result = cursor.fetchall()

query_file.write('sample_datetime,area_name,station_name,station_lat,station_long,fc,temp\n')

#print ('loop start:'+str(datetime.datetime.now().strftime("%H:%M:%S")))

for row in result:

    sample_datetime = row[0]
    sample_datetime_kml = row[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    area_name = row[1]
    station_id = row[2]
    station_name = row[3]
    station_lat = row[4]
    station_long = row[5]
    sample_value_fc = row[6]        
    sample_value_temp = row[7]  
    sample_value_sal = row[8]

    #print (sample_datetime,station_name,station_lat,station_long,sample_value_fc,sample_value_temp)

    query_file_row = str(sample_datetime)+','+area_name+','+station_name+','+str(station_lat)+','+str(station_long)+','+str(sample_value_fc)+','+str(sample_value_temp)+','+str(sample_value_sal)
    query_file.write('\n')
    query_file.write(query_file_row)
    
    body = """
        <Placemark>
          <name>fc:"""+str(sample_value_fc)+' temp:'+str(sample_value_temp)+' sal:'+str(sample_value_sal)+"""</name>
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

#print ('loop end:'+str(datetime.datetime.now().strftime("%H:%M:%S")))

# Close the cursor
cursor.close()

# Close the database connection
conn.close()

