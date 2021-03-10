'''
Requirements:
    pandas
Command line:
    python ga_xls2db.py --ExcelFile=<path to the datafile>

'''
import sys
import optparse
import traceback
from datetime import datetime
import pandas as pd
import shapefile
import psycopg2
from functools import partial

from shapely.geometry import Point, Polygon
from shapely.ops import transform
import pyproj

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
    1: "high",
    2: "flood 3/4",
    3: "ebb 1/4",
    4: "flood 1/2",
    5: "ebb 1/2",
    6: "flood 1/4",
    7: "low",
    8: "ebb 3/4"
}

def process_data(db_host, db_name, db_user, db_pwd, xl_file, growing_areas, stations_csv_file):
    try:
        db_conn = psycopg2.connect(host=db_host,
                                   dbname=db_name,
                                   user=db_user,
                                   password=db_pwd)
    except Exception as e:
        print("Error connecting to database.")
        traceback.print_exc()
    else:
        try:
            samples = parse_worksheet(xl_file, db_conn, growing_areas, stations_csv_file)
        except Exception as e:
            traceback.print_exc()
        db_conn.close()


def get_id(db_cursor, sql):
    try:
        db_cursor.execute(sql)
        rec = db_cursor.fetchone()
        if rec:
            return rec[0]
    except Exception as e:
        raise e
    return None

def obs_id(db_cursor, obs_name):
    sql = "SELECT id FROM lkp_sample_type WHERE name='%s'" % (obs_name)
    return get_id(db_cursor, sql)

def uom_id(db_cursor, uom_name):
    sql = "SELECT id FROM lkp_sample_type WHERE name='%s'" % (uom_name)
    return get_id(db_cursor, sql)

def fc_analysis_method_id(db_cursor, analysis_name):
    sql = "SELECT id FROM lkp_fc_analysis_method WHERE name='%s'" % (analysis_name)
    return get_id(db_cursor, sql)

def sample_reason_id(db_cursor, sample_reason_name):
    sql = "SELECT id FROM lkp_sample_reason WHERE name='%s'" % (sample_reason_name)
    return get_id(db_cursor, sql)

def tide_id(db_cursor, tide_name):
    sql = "SELECT id FROM lkp_tide WHERE name='%s'" % (tide_name)
    return get_id(db_cursor, sql)

def strategy_id(db_cursor, strategy_name):
    sql = "SELECT id FROM lkp_sample_strategy WHERE name='%s'" % (strategy_name)
    return get_id(db_cursor, sql)

def reason_id(db_cursor, reason_name):
    sql = "SELECT id FROM lkp_sample_reason WHERE name='%s'"%(reason_name)
    return get_id(db_cursor, sql)

def area_id(db_cursor, state):
    sql = "SELECT id FROM areas WHERE state='%s'" % (state)
    return get_id(db_cursor, sql)

def add_area(db_cursor, area_name, state):
    try:
        sql = "INSERT INTO areas (name,state) VALUES('%s','%s')" % (area_name, state)
        db_cursor.execute(sql)
    except Exception as e:
        raise e
    return False

def station_id(db_cursor, station_name):
    sql = "SELECT id FROM stations WHERE name='%s'" % (station_name)
    return get_id(db_cursor, sql)

def classification_id(db_cursor, classification_type):
    sql = "SELECT id FROM lkp_area_classification WHERE name='%s'" % (classification_type)
    return get_id(db_cursor, sql)

def growing_area_id(db_cursor, growing_area_name):
    sql = "SELECT id FROM areas WHERE name='%s'" % (growing_area_name)
    return get_id(db_cursor, sql)

def add_growing_area(db_cursor, growing_area_name, state, classification_type):
    #Get the classification ID
    try:
        class_id = classification_id(db_cursor, classification_type)
        if class_id is not None:
            print("Inserting growing area: {area} state: {state} classification: {classification}({class_id})".format(
                area=growing_area_name,
                state=state,
                classification=classification_type, class_id=class_id
            ))
            sql = "INSERT INTO areas (name, state, classification)"\
                  "VALUES('{growing_area}','{state}',{class_id})"\
                .format(growing_area=growing_area_name, state=state, class_id=class_id)
            db_cursor.execute(sql)
            return True
        else:
            print("ERROR, could not find classification: %s" % (classification_type))
    except Exception as e:
        raise e
    return False

def add_station(db_cursor,
                station_name,
                state,area_id,
                longitude, latitude,
                sample_depth, sample_depth_type):
    try:
        print("Adding Station: %s State: %s lon: %f lat: %f" % (station_name, state, longitude, latitude))
        sql = "INSERT INTO stations\
            (name, state, area_id, lat, long, sample_depth_type, sample_depth)\
            VALUES('%s','%s',%d,%f,%f,'%s',%f)" % \
              (station_name, state, area_id, latitude, longitude, sample_depth_type, sample_depth)
        db_cursor.execute(sql)
        return True
    except Exception as e:
        raise e
    return False

def add_sample(db_cursor,
               station_id,
               sample_date,
               date_only,
               obs_type,
               obs_uom,
               obs_value,
               tide,
               strategy,
               reason,
               fc_analysis_method,
               flag):
    try:

        obs_name_id = obs_id(db_cursor, obs_type)
        uom_name_id = obs_id(db_cursor, obs_uom)
        tide_name_id = tide_id(db_cursor, tide)
        strategy_name_id = strategy_id(db_cursor, strategy)
        reasonid = reason_id(db_cursor, reason)
        fc_analysis_method_name_id = fc_analysis_method_id(db_cursor, reason)

        sql = "INSERT INTO samples\
            (sample_datetime,date_only,station_id,tide,strategy,reason,fc_analysis_method,type,units,flag)\
            VALUES('%s',%d,%d,%d,%d,%d,%d,%d,%d,%f,'%s')" %\
              (sample_date)
    except Exception as e:
        raise e
    return
def get_growing_area(growing_areas, station_loc):
    growing_area = None
    '''
    wgs84 = pyproj.Proj(init="epsg:4326")
    epsg_26917 = pyproj.Proj(init="epsg:26917")
    project = partial(
        pyproj.transform,
        epsg_26917,
        wgs84)
    '''
    for ndx,shape in enumerate(growing_areas.iterShapes()):
        area_rec = None
        rec = growing_areas.record(ndx)
        if shape.shapeTypeName.lower() == "polygon":
            growing_area = Polygon(shape.points)
            #growing_area = transform(project, Polygon(shape.points))
        if growing_area is not None:
            if station_loc.within(growing_area):
                area_rec = growing_areas.record(ndx)
                break

    return area_rec

def parse_worksheet(xls_filename, db_conn, growing_areas, stations_csv_file):
    samples = []
    stations_file = None
    try:
        xl_file = pd.read_excel(xls_filename)
        if stations_csv_file is not None:
            try:
                stations_file = open(stations_csv_file, "w")
                stations_file.write("Station Name,Longitude,Latitude\n")
            except IOError as e:
                print("Error opening the stations CSV file: %s" % (stations_csv_file))
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
        db_cursor.execute("SELECT * FROM areas WHERE state='GA';")
        area_recs = db_cursor.fetchall()

        db_cursor.execute("SELECT name FROM lkp_sample_type;")
        sample_types = db_cursor.fetchall()

        db_cursor.execute("SELECT name,long_name FROM lkp_sample_units;")
        sample_units = db_cursor.fetchall()

        area_id = 0
        row_entry_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_growing_area = None
        ga_id = None
        for row_ndx, data_row in xl_file.iterrows():
            print("Processing row: %d" % (row_ndx))
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
                except TypeError as e:
                    try:
                        if type(time) == datetime:
                            sample.sample_datetime = datetime.combine(date, time.time())
                    except TypeError as e:
                        e
                except Exception as e:
                    e
                sample.latitude = data_row['Latitude']
                sample.longitude = data_row['Longitude']
                sample.sample_value = data_row['FecalColiform 100/mL']
                sample.tube_code = data_row['TubeCode']
                sample.tide_code = data_row['TideCode']

                sample.temperature = data_row['Temp °C']
                sample.dissolved_oxygen = data_row['DO mg/L']
                sample.conductivity = data_row['Conductivity mS/cm']
                sample.ph = data_row['pH']
                sample.salinity = data_row['Salinity ppt']

                #Determine the growing area.
                station_loc = transform(project, Point(sample.longitude, sample.latitude))
                #station_loc = Point(sample.longitude, sample.latitude)
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
                if ga_id:
                    #Check if we already added station.
                    if str(sample.station_id) not in station_recs:
                        if stations_file:
                            stations_file.write("{station_name},{longitude},{latitude}\n".format(station_name=sample.station_id,
                                                                                                 longitude=sample.longitude,
                                                                                                 latitude=sample.latitude))
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
                                                ''):
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

            except Exception as e:
                print("Error on row: %d" % (row_ndx))
                traceback.print_exc()
    if stations_file:
        stations_file.close()
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
                     stations_csv_file=options.station_csv_file)
    return

if __name__ == "__main__":
    main()