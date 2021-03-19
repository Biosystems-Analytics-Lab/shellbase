import sys
import os
import optparse
import traceback
from datetime import datetime, timedelta

import requests
from dbfread import DBF

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
'''
2000 – Ebb
2100 – 1/4 Flood
2200 – 1/2 Flood
2300 – 3/4 Flood
4000 – Flood
4100 – 1/4 Ebb
4200 – 1/2 Ebb
4300 – 3/4 Ebb
'''
tide_map = {
    4000: "high",
    4300: "flood 3/4",
    4200: "flood 1/2",
    4100: "flood 1/4",
    2000: "low",
    2300: "ebb 3/4",
    2200: "ebb 1/2",
    2100: "ebb 1/4",
}

'''
Utility to convert DHEC's lookup tables to a csv.
'''
def convert_dbf_files():
    wind_dbf = "WIND.DBF"
    tide_dbf = "TIDE.DBF"
    weather_dbf = "WEATHER.DBF"

    with open("WIND.csv", "w") as wind_out:
        for ndx,record in enumerate(DBF(wind_dbf)):
            if ndx == 0:
                header = record.keys()
                wind_out.write(",".join(header))
                wind_out.write("\n")
            wind_out.write(",".join([rec[1] for rec in record.items()]))
            wind_out.write('\n')
    with open("weather.csv", "w") as wind_out:
        for ndx,record in enumerate(DBF(weather_dbf)):
            if ndx == 0:
                header = record.keys()
                wind_out.write(",".join(header))
                wind_out.write("\n")
            wind_out.write(",".join([rec[1] for rec in record.items()]))
            wind_out.write('\n')
    with open("TIDE.csv", "w") as wind_out:
        for ndx,record in enumerate(DBF(tide_dbf)):
            if ndx == 0:
                header = record.keys()
                wind_out.write(",".join(header))
                wind_out.write("\n")
            wind_out.write(",".join([rec[1] for rec in record.items()]))
            wind_out.write('\n')


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

        print("Successfully connected to database.")
    except Exception as e:
        print("ERROR connecting to database.")
        traceback.print_exc()
    else:
        station_ids = {}
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
            db_cursor = db_conn.cursor()
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
                    add_station(db_cursor, station_rec['attributes']['STAT'], 'SC', areaid,
                                station_rec['attributes']['LONGITUDE'],station_rec['attributes']['LATITUDE'],
                                0, '')
                #Save off the ids so we don't need to hit the database to look them up again when
                #we add the samples for each station below.
                if station_rec['attributes']['STAT'] not in station_ids:
                    station_ids[station_rec['attributes']['STAT']] = sta_id

            db_cursor.close()
        except Exception as e:
            traceback.print_exc()
        else:
            # I assume the strategy and reason are always the same.
            db_cursor = db_conn.cursor()
            strategy_type = "systematic random sampling"
            strategy_type_id = strategy_id(db_cursor, strategy_type)
            if strategy_type_id is None:
                add_strategy(db_cursor, strategy_type, "")
                strategy_type_id = strategy_id(db_cursor, strategy_type)
                db_conn.commit()
            flag = ''
            fc_analysis_method = '5-tube'
            db_cursor.close()

            for station_rec in stations['features']:
                db_cursor = db_conn.cursor()

                station_name = station_rec['attributes']['STAT']
                '''
                These are REST query params for the ESRI endpoint. Basically the important one here
                is the where clause where we ask for a specific station.
                '''
                data_filter_params = {
                    'f': 'json',
                    'where': "(Station = '{station}') AND(1 = 1)" \
                        .format(station=station_name),
                    'returnGeometry': False,
                    'spatialRel': "esriSpatialRelIntersects",
                    'outFields': "*",
                    'orderByFields': "id ASC",
                    'resultOffset': "0"
                }
                try:
                    print("Querying station: {station}".format(station=station_name))
                    site_data = get_request(data_endpoint, data_filter_params)
                    print("Station: {station} query returned {rec_count} records.".format(station=station_name,
                                                                               rec_count=len(site_data['features'])))
                    for data_rec in site_data['features']:
                        try:
                            date_val = datetime(1970, 1, 1) + timedelta(milliseconds=data_rec['attributes']['SF_Date'])
                            time_val = (datetime(1970, 1, 1) + timedelta(milliseconds=data_rec['attributes']['SF_Time'])).time()
                            date_time = datetime.combine(date_val, time_val).strftime('%Y-%m-%d %H:%M:%S')

                        except ValueError as e:
                            print("ERROR converting date: {date} time: {time}".format(date=data_rec['attributes']['SF_Date'],
                                                                                      time=data_rec['attributes']['SF_Time']))
                            traceback.print_exc()
                        else:
                            try:
                                if data_rec['attributes']['Tide'] is not None and \
                                        int(data_rec['attributes']['Tide']) in tide_map:
                                    tide_code = tide_map[int(data_rec['attributes']['Tide'])]
                                else:
                                    print("ERROR no tide value, assigning unknown.")
                                    tide_code = 'unknown'
                            except Exception as e:
                                print("ERROR no tide value, assigning unknown.")
                                tide_code = 'unknown'
                            reason = "routine"
                            if data_rec['attributes']['Type'] == 'S':
                                reason = "emergency"
                            #The TYPE field has the reason. R= routine and S = emergency sample
                            try:
                                water = float(data_rec['attributes']['Water'])
                                print("Station: {station} {date_time} adding water temperature: {value}".format(
                                    station=station_name,
                                    date_time=date_time,
                                    value = water
                                ))
                                add_sample(db_cursor,
                                           station_name,
                                           date_time, False,
                                           'water temperature', "C", water,
                                           tide_code,
                                           strategy_type,
                                           reason,
                                           fc_analysis_method,
                                           flag)
                            except psycopg2.IntegrityError:
                                print("Record already exists.")
                                db_conn.rollback()
                            except Exception as e:
                                print("ERROR adding water temperature record datetime: {sample_datetime}"\
                                      .format(sample_datetime=date_time))
                                traceback.print_exc()
                            try:
                                air = float(data_rec['attributes']['Air'])
                                print("Station: {station} {date_time} adding air temperature: {value}".format(
                                    station=station_name,
                                    date_time=date_time,
                                    value = air
                                ))

                                add_sample(db_cursor,
                                           station_name,
                                           date_time, False,
                                           'air temperature', "C", water,
                                           tide_code,
                                           strategy_type,
                                           reason,
                                           fc_analysis_method,
                                           flag)
                            except psycopg2.IntegrityError:
                                print("Record already exists.")
                                db_conn.rollback()
                            except Exception as e:
                                print("ERROR adding air temperature record datetime: {sample_datetime}"\
                                      .format(sample_datetime=date_time))
                                traceback.print_exc()
                            try:
                                salinity = float(data_rec['attributes']['Salinity'])
                                print("Station: {station} {date_time} adding salinity: {value}".format(
                                    station=station_name,
                                    date_time=date_time,
                                    value = salinity
                                ))

                                add_sample(db_cursor,
                                           station_name,
                                           date_time, False,
                                           'salinity', "ppt", water,
                                           tide_code,
                                           strategy_type,
                                           reason,
                                           fc_analysis_method,
                                           flag)
                            except psycopg2.IntegrityError:
                                print("Record already exists.")
                                db_conn.rollback()
                            except Exception as e:
                                print("ERROR adding salinity record datetime: {sample_datetime}"\
                                      .format(sample_datetime=date_time))
                                traceback.print_exc()
                            try:
                                wind = float(data_rec['attributes']['Wind'])
                                print("Station: {station} {date_time} adding wind direction: {value}".format(
                                    station=station_name,
                                    date_time=date_time,
                                    value = wind
                                ))

                                add_sample(db_cursor,
                                           station_name,
                                           date_time, False,
                                           'wind direction', "degrees", wind,
                                           tide_code,
                                           strategy_type,
                                           reason,
                                           fc_analysis_method,
                                           flag)
                            except psycopg2.IntegrityError:
                                print("Record already exists.")
                                db_conn.rollback()
                            except Exception as e:
                                print("ERROR adding wind direction record datetime: {sample_datetime}"\
                                      .format(sample_datetime=date_time))
                                traceback.print_exc()

                            db_conn.commit()
                    db_cursor.close()

                except Exception as e:
                    traceback.print_exc()
    db_conn.close()

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