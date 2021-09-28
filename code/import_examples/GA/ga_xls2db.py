'''
Requirements:
    pandas
Command line:
    ga_xls2db.py --ExcelFile=<path to the Excel file to import>
                --DBHost=<database server ip address> --DBName=<database name> --DBUser=<database user> --DBPassword=<db user password>
                --Shapefile=<path to the shapefile to use for determining from the station GPS what area it is in>
                --StationCSV=<output path to write a stations file>
'''
import sys
import optparse
import traceback
from datetime import datetime
import pandas as pd
import shapefile
import psycopg2
from functools import partial
from math import isnan
from shapely.geometry import Point, Polygon
from shapely.ops import transform
import pyproj
from db_functions import *

class Sample:
    def __init__(self):
        self.station_id = None
        self.longitude = None
        self.latitude = None
        self.sample_value = None
        self.tube_code = None
        self.sample_datetime = None
        self.tide_code = None
        self.temperature = None
        self.temp_units = 'celsius'
        self.dissolved_oxygen = None
        self._do_units = 'mg/L'
        self.conductivity = None
        self.conductivity_units = 'mS/cm'
        self.ph = None
        self.salinity = None


'''
1 - High
2 - Flood 3/4
3 - Ebb 1/4
4 - Flood 1/2
5 - Ebb 1/2
6 - Flood 1/4
7 - Low
8 - Ebb 3/4
'''
tide_map = {
    1: "HIGH",
    2: "3/4 FLD",
    3: "1/4 EBB",
    4: "1/2 FLD",
    5: "1/2 EBB",
    6: "1/4 FLD",
    7: "LOW",
    8: "3/4 EBB"
}

def process_data(db_host, db_name, db_user, db_pwd,
                 xl_file,
                 growing_areas,
                 stations_csv_file,
                 samples_sql_file):
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
        try:
            samples = parse_worksheet(xl_file, db_conn, growing_areas, stations_csv_file, samples_sql_file)
        except Exception as e:
            traceback.print_exc()
        db_conn.close()

def parse_worksheet(xls_filename, db_conn, growing_areas, stations_csv_file, samples_sql_file):
    samples = []
    stations_file = None
    try:
        xl_file = pd.read_excel(xls_filename)
        if stations_csv_file is not None:
            try:
                stations_file = open(stations_csv_file, "w")
                stations_file.write("Station Name,Longitude,Latitude\n")
                stations_written = []
            except IOError as e:
                print("Error opening the stations CSV file: %s" % (stations_csv_file))
                traceback.print_exc()
        samples_sql_file_obj = None
        if samples_sql_file is not None:
            try:
                samples_sql_file_obj = open(samples_sql_file, "w")
            except IOError as e:
                print("Error opening the samples SQL file: %s" % (samples_sql_file))
                traceback.print_exc()
    except Exception as e:
        print("Error opening file: %s" % (xls_filename))
        traceback.print_exc()
    else:
        wgs84 = pyproj.Proj(init="epsg:4326")
        epsg_26917 = pyproj.Proj(init="epsg:26917")
        project = partial(
            pyproj.transform,
            wgs84,
            epsg_26917)

        station_recs = {}
        db_cursor = db_conn.cursor()

        db_cursor.execute("SELECT id,name FROM lkp_fc_analysis_method")
        fc_analysis_methods = db_cursor.fetchall()
        fc_analysis_id_map = build_lookup_id_map(fc_analysis_methods)
        db_cursor.execute("SELECT id,name FROM lkp_sample_reason")
        reasons = db_cursor.fetchall()
        reasons_id_map = build_lookup_id_map(reasons)
        db_cursor.execute("SELECT id,name FROM lkp_tide")
        tides = db_cursor.fetchall()
        tides_id_map = build_lookup_id_map(tides)

        db_cursor.execute("SELECT id,name FROM lkp_sample_type")
        obs_types = db_cursor.fetchall()
        obs_types_id_map = build_lookup_id_map(obs_types)

        db_cursor.execute("SELECT id,name FROM lkp_sample_units")
        uom_types = db_cursor.fetchall()
        uom_types_id_map = build_lookup_id_map(uom_types)

        #I assume the strategy and reason are always the same.
        strategy_type = "systematic random sampling"
        strategy_type_id = strategy_id(db_cursor, strategy_type)
        if strategy_type_id is None:
            add_strategy(db_cursor, strategy_type, "")
            c = strategy_id(db_cursor, strategy_type)
        reason = 'routine'
        fc_analysis_method = '3-tube'
        current_growing_area = None
        ga_id = None
        for row_ndx, data_row in xl_file.iterrows():
            #print("Processing row: %d" % (row_ndx))
            try:
                sample = Sample()
                sample.station_id = data_row['StationID']
                parts = data_row['DateCollected'].split('/')
                #Zero pad the date and create a datetime object.
                date = datetime.strptime('%02d/%02d/%d' % (int(parts[0]), int(parts[1]), int(parts[2])), "%m/%d/%Y")
                time = data_row['Time']
                #We combine the individual date and time columns into a python datetime object.
                try:
                    sample.sample_datetime = datetime.combine(date, time)
                    #Sometimes the time comes back as a datetime object and not just a time object.
                except TypeError as e:
                    try:
                        if type(time) == datetime:
                            sample.sample_datetime = datetime.combine(date, time.time())
                    except TypeError as e:
                        e
                except Exception as e:
                    traceback.print_exc()

                sample.latitude = data_row['Latitude']
                sample.longitude = data_row['Longitude']
                #sample.sample_value = data_row['FecalColiform 100/mL']
                sample.sample_value = data_row['FecalColiform']
                sample.tube_code = data_row['TubeCode']
                if not isnan(data_row['TideCode']):
                    sample.tide_code = tide_map[int(data_row['TideCode'])]

                #sample.temperature = data_row['Temp °C']
                sample.temperature = data_row['Temp']
                #sample.dissolved_oxygen = data_row['DO mg/L']
                sample.dissolved_oxygen = data_row['DO']
                #sample.conductivity = data_row['Conductivity mS/cm']
                sample.conductivity = data_row['Conductivity']
                sample.ph = data_row['pH']
                #sample.salinity = data_row['Salinity ppt']
                sample.salinity = data_row['Salinity']

                if stations_file and\
                    str(sample.station_id) not in stations_written:
                        stations_file.write(
                            "{station_name},{longitude},{latitude}\n".format(station_name=sample.station_id,
                                                                             longitude=sample.longitude,
                                                                             latitude=sample.latitude))
                        stations_written.append(str(sample.station_id))

                #Determine the growing area.
                station_loc = transform(project, Point(sample.longitude, sample.latitude))

                growing_area_record = get_growing_area(growing_areas, station_loc)
                if growing_area_record is not None:
                    if growing_area_record.County != current_growing_area:
                        ga_id = growing_area_id(db_cursor, growing_area_record.County)
                        if ga_id is None:
                            try:
                                add_growing_area(db_cursor, growing_area_record.County, 'GA', 'approved')
                                db_conn.commit()
                                ga_id = growing_area_id(db_cursor, growing_area_record.County)
                            except Exception as e:
                                print("ERROR Row: {row_ndx} adding growing area: {area_name}"\
                                      .format(row_ndx=row_ndx, area_name=growing_area_record.County))
                                traceback.print_exc()
                        current_growing_area = growing_area_record.County
                else:
                    print("ERROR unable to geo locate station: {station_name} to growing area."\
                          .format(station_name=sample.station_id))

                if ga_id:
                    #Check if we already added station.
                        sta_id = station_id(db_cursor, str(sample.station_id))

                        if sta_id == None:
                            try:
                                if add_station(db_cursor,
                                                str(sample.station_id),
                                                'GA',
                                                ga_id,
                                                sample.longitude,
                                                sample.latitude,
                                                0.0,
                                                '',
                                               True):
                                    db_conn.commit()
                                    sta_id = station_id(db_cursor, str(sample.station_id))

                            except Exception as e:
                                db_conn.rollback()
                                print("ERROR row: {row_ndx} adding Station: {station} Datetime: {date_time}"\
                                      .format(row_ndx=row_ndx,
                                              station=sample.station_id,
                                              date_time=sample.sample_datetime))
                                traceback.print_exc()
                        if sta_id:
                            station_recs[str(sample.station_id)] = sta_id
                flag = 0
                #Add the samples.
                stationid = station_recs[str(sample.station_id)]
                tide_code_id = None
                if sample.tide_code is not None:
                    tide_code_id = tides_id_map[sample.tide_code]
                reason_id = reasons_id_map[reason]
                fc_analysis_id = fc_analysis_id_map[fc_analysis_method]
                try:
                    if not isnan(sample.temperature):
                        add_sample_with_ids(db_cursor,
                                   stationid,
                                   sample.sample_datetime, False,
                                   obs_types_id_map['water temperature'], uom_types_id_map["C"],
                                   sample.temperature,
                                   tide_code_id,
                                   strategy_type_id,
                                   reason_id,
                                   fc_analysis_id,
                                   flag,
                                   samples_sql_file_obj)
                    else:
                        print("ERROR did not add temperature record row: {row_ndx} datetime: {sample_datetime}" \
                              .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                except Exception as e:
                    print("ERROR adding temperature record row: {row_ndx} datetime: {sample_datetime}"\
                          .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                    traceback.print_exc()

                try:
                    if not isnan(sample.dissolved_oxygen):
                        add_sample_with_ids(db_cursor,
                                   stationid,
                                   sample.sample_datetime, False,
                                   obs_types_id_map['dissolved oxygen'], uom_types_id_map["mg/L"],
                                   sample.dissolved_oxygen,
                                   tide_code_id,
                                   strategy_type_id,
                                   reason_id,
                                   fc_analysis_id,
                                   flag,
                                   samples_sql_file_obj)
                    else:
                        print("ERROR did not add DO record row: {row_ndx} datetime: {sample_datetime}" \
                              .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))

                except Exception as e:
                    print("ERROR adding DO record row: {row_ndx} datetime: {sample_datetime}"\
                          .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                    traceback.print_exc()
                try:
                    if not isnan(sample.salinity):
                        add_sample_with_ids(db_cursor,
                                   stationid,
                                   sample.sample_datetime, False,
                                   obs_types_id_map['salinity'], uom_types_id_map["ppt"],
                                   sample.salinity,
                                   tide_code_id,
                                   strategy_type_id,
                                   reason_id,
                                   fc_analysis_id,
                                   flag,
                                   samples_sql_file_obj)
                    else:
                        print("ERROR did not add salinity record row: {row_ndx} datetime: {sample_datetime}" \
                              .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))

                except Exception as e:
                    print("ERROR adding salinity record row: {row_ndx} datetime: {sample_datetime}"\
                          .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                    traceback.print_exc()
                try:
                    if not isnan(sample.conductivity):
                        add_sample_with_ids(db_cursor,
                                   stationid,
                                   sample.sample_datetime, False,
                                   obs_types_id_map['conductivity'], uom_types_id_map["mS/cm"],
                                   sample.conductivity,
                                   tide_code_id,
                                   strategy_type_id,
                                   reason_id,
                                   fc_analysis_id,
                                   flag,
                                   samples_sql_file_obj)
                    else:
                        print("ERROR did not add conductivity record row: {row_ndx} datetime: {sample_datetime}" \
                              .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))

                except Exception as e:
                    print("ERROR adding conductivity record row: {row_ndx} datetime: {sample_datetime}"\
                          .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                    traceback.print_exc()
                try:
                    if not isnan(sample.ph):
                        add_sample_with_ids(db_cursor,
                                   stationid,
                                   sample.sample_datetime, False,
                                   obs_types_id_map['ph'], None,
                                   sample.ph,
                                   tide_code_id,
                                   strategy_type_id,
                                   reason_id,
                                   fc_analysis_id,
                                   flag,
                                   samples_sql_file_obj)
                    else:
                        print("ERROR did not add ph record row: {row_ndx} datetime: {sample_datetime}" \
                              .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))

                except Exception as e:
                    print("ERROR adding ph record row: {row_ndx} datetime: {sample_datetime}"\
                          .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                    traceback.print_exc()
                try:
                    if not isnan(sample.sample_value):
                        add_sample_with_ids(db_cursor,
                                   stationid,
                                   sample.sample_datetime, False,
                                   obs_types_id_map['fc'], uom_types_id_map['cfu/100 mL'],
                                   sample.sample_value,
                                   tide_code_id,
                                   strategy_type_id,
                                   reason_id,
                                   fc_analysis_id,
                                   flag,
                                   samples_sql_file_obj)
                    else:
                        print("ERROR did not add fecal coliform record row: {row_ndx} datetime: {sample_datetime}" \
                              .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                except Exception as e:
                    print("ERROR adding fecal coliform record row: {row_ndx} datetime: {sample_datetime}" \
                          .format(row_ndx=row_ndx, sample_datetime=sample.sample_datetime))
                    traceback.print_exc()
                db_conn.commit()
            except Exception as e:
                print("Error on row: %d" % (row_ndx))
                traceback.print_exc()
    if stations_file:
        stations_file.close()
    if samples_sql_file_obj:
        samples_sql_file_obj.close()
    return samples

def save_to_database(samples):
    return

def main():
    parser = optparse.OptionParser()
    parser.add_option("--ExcelFile", dest="xl_file", default=None,
                      help="Excel data file to import.")
    parser.add_option("--DBHost", dest="db_host", default=None,
                      help="Address for the DB host.")
    parser.add_option("--DBName", dest="db_name", default=None,
                      help="Name of the database in the DB server.")
    parser.add_option("--DBUser", dest="db_user", default=None,
                      help="User login for the DB.")
    parser.add_option("--DBPassword", dest="db_pwd", default=None,
                      help="User password for the DB.")
    parser.add_option("--Shapefile", dest="shapefile", default=None,
                      help="Shapefile for the growing areas.")
    parser.add_option("--StationCSV", dest="station_csv_file", default=None,
                      help="If provided, this will write a CSV with the station lat/lon and info to the file.")
    parser.add_option("--SamplesSQLFile", dest="samples_sql_file", default=None,
                      help="If provided, this will write a SQL file with INSERT statements for the samples")


    (options, args) = parser.parse_args()

    if options.xl_file is None:
        print("No Excel file to process, use --ExcelFile parameter.")
    else:
        growing_areas = None
        if options.shapefile is not None:
            growing_areas = shapefile.Reader(options.shapefile)
        print("Excel file: %s to be processed" % (options.xl_file))
        process_data(db_host=options.db_host,
                     db_name=options.db_name,
                     db_user=options.db_user,
                     db_pwd=options.db_pwd,
                     xl_file=options.xl_file,
                     growing_areas=growing_areas,
                     stations_csv_file=options.station_csv_file,
                     samples_sql_file=options.samples_sql_file)
    return

if __name__ == "__main__":
    main()