South Carolina DHEC provides an ESRI endpoint to access the data: https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer
unfortunately they have disabled the browsing feature of the ESRI server.
  
Example command line:
sc_esri_to_db.py --ESRIDataEndpoint=https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/3/query --ESRISitesEndpoint=https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/0/query --ESRIHarvesClassificationsEndpoint=https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/2/query --UpdateStationData --StartYear=<start year> --EndYear=<end year>> --DBHost=<db host address>> --DBName=<db name> --DBUser=<db user> --DBPassword=<db password>
