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

The below file is a demo query file which runs the below query to produce a csv listing and kml formatted file(viewable in google earth) of stations with fc > 100 and associated temperature.

https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_kml.py

```sql
select sample_datetime,areas.name,stations.id,stations.name,stations.lat,stations.long,value
  from samples,stations,areas
  where stations.id = samples.station_id
    and areas.id = stations.area_id
    and type = 1 and value > 100 order by sample_datetime;
```

https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/shellbase_demo_query1.csv
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/shellbase_demo_query1.kml
