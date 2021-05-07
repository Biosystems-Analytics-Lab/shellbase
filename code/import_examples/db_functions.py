import psycopg2
from shapely.geometry import Polygon
import traceback
from multiprocessing import Process, Queue, Event
import time

def database_connect(**kwargs):
    try:
        if kwargs.get('type', 'postgres') == 'postgres':
            db_conn = psycopg2.connect(host=kwargs['db_host'],
                                       dbname=kwargs['db_name'],
                                       user=kwargs['db_user'],
                                       password=kwargs['db_pwd'])
            return db_conn
    except Exception as e:
        raise e


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
    sql = "SELECT id FROM lkp_sample_units WHERE name='%s'" % (uom_name)
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
def add_strategy(db_cursor, strategy_name, description):
    try:
        sql = "INSERT INTO lkp_sample_strategy (id,name,description) VALUES(%d,'%s','%s')" % (0,strategy_name, description)
        db_cursor.execute(sql)
    except Exception as e:
        raise e
    return False

def reason_id(db_cursor, reason_name):
    sql = "SELECT id FROM lkp_sample_reason WHERE name='%s'"%(reason_name)
    return get_id(db_cursor, sql)

def area_id(db_cursor, name, state):
    sql = "SELECT id FROM areas WHERE name='%s' AND state='%s'" % (name, state)
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
                sample_depth, sample_depth_type,
                active):
    try:
        print("Adding Station: %s State: %s lon: %f lat: %f" % (station_name, state, longitude, latitude))
        sql = "INSERT INTO stations\
            (name, state, area_id, lat, long, sample_depth_type, sample_depth,active)\
            VALUES('%s','%s',%d,%f,%f,'%s',%f,%r)" % \
              (station_name, state, area_id, latitude, longitude, sample_depth_type, sample_depth,active)
        db_cursor.execute(sql)
        return True
    except Exception as e:
        raise e
    return False

def add_sample(db_cursor,
               station_name,
               sample_date,
               date_only,
               obs_type,
               obs_uom,
               obs_value,
               tide,
               strategy,
               reason,
               fc_analysis_method,
               flag,
               sql_file=None):
    try:
        sta_id = station_id(db_cursor, station_name)
        obs_name_id = obs_id(db_cursor, obs_type)
        uom_name_id = uom_id(db_cursor, obs_uom)
        tide_name_id = tide_id(db_cursor, tide)
        strategy_name_id = strategy_id(db_cursor, strategy)
        reasonid = reason_id(db_cursor, reason)
        fc_analysis_method_name_id = fc_analysis_method_id(db_cursor, fc_analysis_method)
        if uom_name_id is None:
            uom_name_id = 'NULL'
        if tide_name_id is None:
            tide_name_id = 'NULL'
        sql = "INSERT INTO samples"\
            "(sample_datetime,date_only,station_id,tide,strategy,reason,fc_analysis_method,type,units,value,flag)"\
            "VALUES('{sample_date}',{date_only},{sta_id},{tide_name_id},{strategy_name_id},{reasonid}," \
              "{fc_analysis_method_name_id},{obs_name_id},{uom_name_id},{obs_value},'{flag}');".format(
                sample_date=sample_date,
                date_only=date_only,
               sta_id=sta_id,
               tide_name_id=tide_name_id,
               strategy_name_id=strategy_name_id,
               reasonid=reasonid,
               fc_analysis_method_name_id=fc_analysis_method_name_id,
               obs_name_id=obs_name_id,
               uom_name_id=uom_name_id,
               obs_value=obs_value,
               flag=flag)

        '''
        sql = "INSERT INTO samples"\
            "(sample_datetime,date_only,station_id,tide,strategy,reason,fc_analysis_method,type,units,value,flag)"\
            "VALUES('%s',%r,%d,%d,%d,%d,%d,%d,%d,%f,'%s')"%\
              (sample_date, date_only,
               sta_id,
               tide_name_id,
               strategy_name_id,
               reasonid,
               fc_analysis_method_name_id,
               obs_name_id,
               uom_name_id,
               obs_value,
               flag)
        '''
        if sql_file is None:
            db_cursor.execute(sql)
        else:
            sql_file.write(sql)
            sql_file.write('\n')
        return True
    except Exception as e:
        raise e
    return False

def add_sample_with_ids(db_cursor,
               sta_id,
               sample_date,
               date_only,
               obs_name_id,
               uom_name_id,
               obs_value,
               tide_name_id,
               strategy_name_id,
               reasonid,
               fc_analysis_method_name_id,
               flag,
               sql_file=None):
    try:
        if uom_name_id is None:
            uom_name_id = 'NULL'
        if tide_name_id is None:
            tide_name_id = 'NULL'
        sql = "INSERT INTO samples"\
            "(sample_datetime,date_only,station_id,tide,strategy,reason,fc_analysis_method,type,units,value,flag)"\
            "VALUES('{sample_date}',{date_only},{sta_id},{tide_name_id},{strategy_name_id},{reasonid}," \
              "{fc_analysis_method_name_id},{obs_name_id},{uom_name_id},{obs_value},'{flag}');".format(
                sample_date=sample_date,
                date_only=date_only,
               sta_id=sta_id,
               tide_name_id=tide_name_id,
               strategy_name_id=strategy_name_id,
               reasonid=reasonid,
               fc_analysis_method_name_id=fc_analysis_method_name_id,
               obs_name_id=obs_name_id,
               uom_name_id=uom_name_id,
               obs_value=obs_value,
               flag=flag)

        if sql_file is None:
            db_cursor.execute(sql)
        else:
            sql_file.write(sql)
            sql_file.write('\n')
        return True
    except Exception as e:
        raise e
    return False

def get_growing_area(growing_areas, station_loc):
    growing_area = None

    for ndx,shape in enumerate(growing_areas.iterShapes()):
        area_rec = None
        if shape.shapeTypeName.lower() == "polygon":
            growing_area = Polygon(shape.points)
        if growing_area is not None:
            if station_loc.within(growing_area):
                area_rec = growing_areas.record(ndx)
                break

    return area_rec

def add_classification_area(db_cursor,
                            area_name,
                            classification_name,
                            is_current,
                            start_date,
                            end_data,
                            comments):
    try:
        areaid = area_id(db_cursor, area_name)
        classificationid = classification_id(db_cursor, classification_name)

        sql = "INSERT INTO history_areas_classification (area_id,classification,current,start_date,end_date,comments)"\
            "VALUES(%d,%d,%r,'%s','%s','%s')" % (areaid,classificationid,is_current,start_date,end_data,comments)
        db_cursor.execute(sql)
        return True

    except Exception as e:
        raise e
    return False

def build_lookup_id_map(db_recs):
    lookup_map = {}
    for rec in db_recs:
        lookup_map[rec[1]] = rec[0]
    return lookup_map

class sample_saver(Process):
    def __init__(self):
        Process.__init__(self)
        self._host = None
        self._dbname = None
        self._user = None
        self._password = None
        self._input_queue = Queue()
        self._stop_event = Event()
        return
    def initialize(self, **kwargs):
        self._host = kwargs['db_host'],
        self._dbname = kwargs['db_name'],
        self._user = kwargs['db_user'],
        self._password = kwargs['db_pwd']
    @property
    def input_queue(self):
        return self._input_queue
    @property
    def stop_event(self):
        return self._stop_event
    def run(self):
        try:
            db_conn = database_connect(type='postgres',
                                       db_host=self._db_host,
                                       db_name=self._db_name,
                                       db_user=self._db_user,
                                       db_pwd=self._db_pwd)

            for sample in iter(self._input_queue.get, 'STOP'):
                tot_file_time_start = time.time()
                if logger:
                    logger.debug("ID: %s processing file: %s" % (current_process().name, xmrg_filename))

        except Exception as e:
            traceback.print_exc()