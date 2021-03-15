import sys
import os
import optparse
import traceback
from datetime import datetime, timedelta

import requests

from db_functions import *
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

def process_data(db_host, db_name, db_user, db_pwd, start_year, end_year, stations_endpoint, data_endpoint, stations_csv_file):
    try:
        db_conn = database_connect(type='postgres',
                                   db_host=db_host,
                                   db_name=db_name,
                                   db_user=db_user,
                                   db_pwd=db_pwd)
        db_cursor = db_conn.cursor()

        print("Successfully connected to database.")
    except Exception as e:
        print("ERROR connecting to database.")
        traceback.print_exc()
    else:
        station_ids = {}
        area_ids = {}
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
            print("Querying the station metadata.")
            stations = get_request(stations_endpoint, filter_params)
            for station_rec in stations['features']:
                sta_id = station_id(db_cursor, station_rec['attributes']['STAT'])
                '''
                def add_area(db_cursor, area_name, state):

                '''
                areaid = area_id(db_cursor, station_rec['attributes']['SF_AREA'], 'SC')
                if areaid is None:
                    try:
                        print("Adding area {area} state: {state}".format(area=station_rec['attributes']['SF_AREA'], state='SC'))
                        add_area(db_cursor, station_rec['attributes']['SF_AREA'],'SC')
                        db_conn.commit()
                    except psycopg2.IntegrityError as e:
                        e
                    except Exception as e:
                        print("ERROR adding area: {area}".format(area=station_rec['attributes']['SF_AREA']))
                    areaid = area_id(db_cursor, station_rec['attributes']['SF_AREA'], 'SC')

                if sta_id is None:
                    '''
                    (db_cursor,
                    station_name,
                    state,area_id,
                    longitude, latitude,
                    sample_depth, sample_depth_type):
                    '''
                    add_station(db_cursor, station_rec['attributes']['STAT'], 'SC', areaid,
                                station_rec['attributes']['LONGITUDE'],station_rec['attributes']['LATITUDE'],
                                0, '')
                #Save off the ids so we don't need to hit the database to look them up again.
                if station_rec['attributes']['STAT'] not in station_ids:
                    station_ids[station_rec['attributes']['STAT']] = sta_id
                if station_rec['attributes']['SF_AREA'] not in area_ids:
                    area_ids[station_rec['attributes']['SF_AREA']] = areaid
        except Exception as e:
            traceback.print_exc()
        else:
            for station_rec in stations['features']:
                year = start_year
                station_name = station_rec['attributes']['STAT']
                while year < end_year:
                    data_filter_params = {
                        'f': 'json',
                        'where': "(ProjectYear = '{year}') AND(Station = '{station}') AND(1 = 1)" \
                            .format(year=year, station=station_name),
                        'returnGeometry': False,
                        'spatialRel': "esriSpatialRelIntersects",
                        'outFields': "*",
                        'orderByFields': "id ASC",
                        'resultOffset': "0"
                    }
                    try:
                        print("Querying station: {station} for year: {year}".format(station=station_name, year=year))
                        site_data = get_request(data_endpoint, data_filter_params)
                        for data_rec in site_data['features']:
                            data_rec
                    except Exception as e:
                        traceback.print_exc()
                    year += 1

    return

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

    process_data(options.db_host,
                 options.db_name,
                 options.db_user,
                 options.db_pwd,
                 options.start_year,
                 options.end_year,
                 options.esri_sites_endpoint,
                 options.esri_data_endpoint,
                 None)

    return

if __name__ == "__main__":
    main()