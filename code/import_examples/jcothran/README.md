3 script passes on the csv file in their data dependency order where areas->stations->samples .

#insert areas
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_areas.py

#insert stations
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_stations.py

#insert samples
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_samples.py

The below file is a demo query file which runs the below query to produce a csv listing and kml formatted file(viewable in google earth) of stations with fc > 100 and associated temperature.

https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/fl_csv2db_kml.py

https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/shellbase_demo_query1.csv
https://github.com/Biosystems-Analytics-Lab/shellbase/blob/main/code/import_examples/jcothran/shellbase_demo_query1.kml
