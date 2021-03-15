import sys
import os
import optparse
import traceback
from datetime import datetime

import requests

'''
This is the service for the data:
https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/3

and to make sure of the count, you can check with this query.
https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/3/query?where=1=1&f=pjson&returnCountOnly=true

This service is tied into shellfish background services I created previously:
Shellfish Monitoring Sites
https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/0
Management Areas
https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/1
and Harvest Classifications
https://gis.dhec.sc.gov/arcgis/rest/services/environment/Shellfish_Closures_Background/MapServer/2

'''
def get_request(rest_endpoint, filter_params):
    try:
        req = requests.get(rest_endpoint, params=filter_params)
        if req.status_code == 200:
            json_data = req.json()
            return json_data
    except Exception as e:
        raise e
    return None

def main():
    parser = optparse.OptionParser()
    parser.add_option("--StationCSV", dest="station_csv_file", default=None,
                      help="If provided, this will write a CSV with the station lat/lon and info to the file.")

    parser.add_option("--ESRIDataEndpoint", dest="esri_data_endpoint", default=None,
                      help="")
    parser.add_option("--ESRISitesEndpoint", dest="esri_sites_endpoint", default=None,
                      help="")
    parser.add_option("--ESRIMgmtAreasEndpoint", dest="esri_mgmt_areas_endpoint", default=None,
                      help="")
    parser.add_option("--ESRIHarvesClassificationsEndpoint", dest="esri_harvest_classification_endpoint", default=None,
                      help="")
    parser.add_option("--StartYear", dest="start_year", default=None, type="int",
                      help="")
    parser.add_option("--EndYear", dest="end_year", default=None, type="int",
                      help="")
    parser.add_option("--DBHost", dest="db_host", default=None,
                      help="Address for the DB host.")
    parser.add_option("--DBName", dest="db_name", default=None,
                      help="Name of the database in the DB server.")
    parser.add_option("--DBUser", dest="db_user", default=None,
                      help="User login for the DB.")
    parser.add_option("--DBPassword", dest="db_pwd", default=None,
                      help="User password for the DB.")


    (options, args) = parser.parse_args()

    try:
        filter_params = {
            'f': "json",
            'where': "1 = 1",
            'returnGeometry': True,
            'spatialRel': "esriSpatialRelIntersects",
            'outFields': "*",
            'outSR': "102100",
            'resultOffset': 0
        }
        stations = get_request(options.esri_sites_endpoint, filter_params)
    except Exception as e:
        traceback.print_exc()
    else:
        for station_rec in stations['features']:
            year = options.start_year
            station_name = station_rec['attributes']['STAT']
            while year < options.end_year:
                data_filter_params = {
                    'f': 'json',
                    'where': "(ProjectYear = '{year}') AND(Station = '{station}') AND(1 = 1)"\
                        .format(year=year, station=station_name),
                    'returnGeometry': False,
                    'spatialRel': "esriSpatialRelIntersects",
                    'outFields': "*" ,
                    'orderByFields': "id ASC",
                    'resultOffset': "0"
                }
                try:
                    site_data = get_request(options.esri_data_endpoint, data_filter_params)
                except Exception as e:
                    traceback.print_exc()
            year += 1
    return

if __name__ == "__main__":
    main()