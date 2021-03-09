3 script passes on the csv file in their data dependency order where areas->stations->samples .

Each of the below 3 scripts uses parameters like below, where subfolder is the name of a subfolder containing related csv files to be processed and pg_password is the postgres password for the associated user(postgres).

```
python fl_csv2db_areas.py <subfolder> <pg_password>
```

#insert areas
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_areas.py

#insert stations
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_stations.py

#insert samples
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_samples.py

The below file is a demo query file which runs the below query to produce a csv listing and kml formatted file(viewable in google earth) of stations with fc > 100 and other associated(same sample_datetime,station_id) measurements ordered by sample_datetime

https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_kml.py

#multiple joins - info on joins https://learnsql.com/blog/how-to-left-join-multiple-tables/

```sql
select A.sample_datetime,areas.name as area_name,A.station_id,stations.name as station_name,A.value as fc,B.value as temp,C.value as sal
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
        
      order by sample_datetime;
```

https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/shellbase_demo_query1.csv
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/shellbase_demo_query1.kml
