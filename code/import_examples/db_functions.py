import psycopg2

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
               flag):
    try:
        sta_id = station_id(db_cursor, station_name)
        obs_name_id = obs_id(db_cursor, obs_type)
        uom_name_id = uom_id(db_cursor, obs_uom)
        tide_name_id = tide_id(db_cursor, tide)
        strategy_name_id = strategy_id(db_cursor, strategy)
        reasonid = reason_id(db_cursor, reason)
        fc_analysis_method_name_id = fc_analysis_method_id(db_cursor, fc_analysis_method)

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
        db_cursor.execute(sql)
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
