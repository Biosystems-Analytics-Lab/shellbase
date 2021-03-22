-- as postgres user
-- create database shellbase;

-- from command line to run below sql against new shellbase, -q for quiet for large insert files
-- psql -U postgres -d shellbase -f sb_obs.sql

-- psql command line checks
-- \l list databases
-- \c connect database
-- \d+ describe tables,indexes

-- notes:
-- all timestamps are assumed EST
-- lkp tables are manually edited and not given auto-index sequence id
-- some lookup fields will be unset/null due to data provider not providing that type of data
-- create order listing below from smaller independent sub-tables to larger dependent tables

--------
--------

CREATE TABLE lkp_area_classification (
    id integer PRIMARY KEY NOT NULL,
    name varchar(50)
);

INSERT INTO lkp_area_classification (id,name)
    values
    (1,'approved'),
    (2,'conditionally approved'),
    (3,'restricted'),
    (4,'prohibited'),
    (5,'unknown');

--------
--------

CREATE TABLE lkp_tide (
    id integer PRIMARY KEY NOT NULL,
    name varchar(50)
);

INSERT INTO lkp_tide (id,name)
    values
    (-1, 'unknown'),
    (1,'high'),
    (2,'flood 3/4'),
    (3,'ebb 1/4'),
    (4,'flood 1/2'),
    (5,'ebb 1/2'),
    (6,'flood 1/4'),
    (7,'low'),
    (8,'ebb 3/4'),
    (9,'first flood'),
    (10,'last flood'),
    (11,'first ebb'),
    (12,'last ebb'),
    (13,'wind'),
    (14,'wind avg'),    
    (15,'wind above avg');

--------
--------

CREATE TABLE lkp_sample_strategy (
    id integer PRIMARY KEY NOT NULL,
    name varchar(50),
    description varchar(400)
);

    
--------
--------

CREATE TABLE lkp_sample_reason (
    id integer PRIMARY KEY NOT NULL,
    name varchar(50)
);

INSERT INTO lkp_sample_reason (id,name)
    values
    (1,'emergency'),
    (2,'conditional'),
    (3,'routine');

--------
--------

CREATE TABLE lkp_fc_analysis_method (
    id integer PRIMARY KEY NOT NULL,
    name varchar(50)
);

INSERT INTO lkp_fc_analysis_method (id,name)
    values
    (1,'direct plating'),
    (2,'5-tube'),
    (3,'3-tube');

--------
--------

CREATE TABLE lkp_sample_type (
    id integer PRIMARY KEY NOT NULL,
    name varchar(50)
);

INSERT INTO lkp_sample_type (id,name)
    values
    (1,'fc'),
    (2,'water temperature'),
    (3,'salinity'),
    (4,'conductivity'),
    (5,'dissolved oxygen'),
    (6,'ph'),
    (7,'wind direction'),
    (8,'air temperature');

--------
--------

CREATE TABLE lkp_sample_units (
    id integer PRIMARY KEY NOT NULL,
    name varchar(50),
    long_name varchar(100)
);

INSERT INTO lkp_sample_units (id,name,long_name)
    values
    (1,'cfu/100 mL','colony forming units / 100 milliliters'),
    (2,'C','celsius'),
    (3,'ppt','parts per thousand'),
    (4,'mS/cm','microsiemens per centimeter'),
    (5,'mg/L','milligrams / liter'),
    (6,'F','fahrenheit'),
    (7,'degrees','degrees from north')
;

--------
--------

CREATE TABLE areas (

    id integer PRIMARY KEY NOT NULL, 
    row_update_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    
    name varchar(50),
    state char(2),
    classification integer REFERENCES lkp_area_classification(id)
    
);

CREATE SEQUENCE areas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY areas ALTER COLUMN id SET DEFAULT nextval('areas_id_seq'::regclass);

--------
--------

CREATE TABLE history_areas_closure (

    id integer PRIMARY KEY NOT NULL, 
    
    area_id integer REFERENCES areas(id),
    
    current boolean,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    
    comments varchar(400)
);

CREATE SEQUENCE history_areas_closure_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY history_areas_closure ALTER COLUMN id SET DEFAULT nextval('history_areas_closure_id_seq'::regclass);

--------
--------

CREATE TABLE history_areas_classification (

    id integer PRIMARY KEY NOT NULL, 
    
    area_id integer REFERENCES areas(id),
    classification integer REFERENCES lkp_area_classification(id),
    
    current boolean,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    
    comments varchar(400)
);

CREATE SEQUENCE history_areas_classification_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY history_areas_classification ALTER COLUMN id SET DEFAULT nextval('history_areas_classification_id_seq'::regclass);

--------
--------

CREATE TABLE stations (

    id integer PRIMARY KEY NOT NULL, 
    row_update_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    
    name varchar(50),
    state char(2),
    area_id integer REFERENCES areas(id),
    lat double precision,
    long double precision,
    
    sample_depth_type char(2),
    sample_depth double precision
    
);

CREATE SEQUENCE stations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY stations ALTER COLUMN id SET DEFAULT nextval('stations_id_seq'::regclass);

--------
--------

CREATE TABLE samples (

    id integer PRIMARY KEY NOT NULL,
    row_update_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    
    sample_datetime timestamp without time zone,
    date_only boolean,
    
    station_id integer REFERENCES stations(id),
    tide integer REFERENCES lkp_tide(id),
    strategy integer REFERENCES lkp_sample_strategy(id),
    reason integer  REFERENCES lkp_sample_reason(id),
    fc_analysis_method integer REFERENCES lkp_fc_analysis_method(id),
    
    type integer REFERENCES lkp_sample_type(id),
    units integer REFERENCES lkp_sample_units(id),
    value double precision,
    flag varchar(50)
    
);

CREATE SEQUENCE samples_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY samples ALTER COLUMN id SET DEFAULT nextval('samples_id_seq'::regclass);

CREATE UNIQUE INDEX i_areas ON areas USING btree (state,name);
CREATE UNIQUE INDEX i_stations ON stations USING btree (area_id,name);
CREATE UNIQUE INDEX i_samples ON samples USING btree (sample_datetime,station_id,type);

-- samples_wide view(6 current observation types, but can be extended with additional joins as needed for new observations types) allows for a much simpler query for related sample observations
-- for example
-- select * from samples_wide where fc > 100 limit 10;

CREATE VIEW samples_wide AS 

select A.sample_datetime,areas.name as area_name,A.station_id,stations.name as station_name,A.value as fc,B.value as temp,C.value as sal,D.value as cond,E.value as do,F.value as ph
      from samples A
      
      left join samples B 
        on A.sample_datetime = B.sample_datetime
        and A.station_id = B.station_id
        and B.type = 2
      
      left join samples C 
        on A.sample_datetime = C.sample_datetime
        and A.station_id = C.station_id
        and C.type = 3

      left join samples D 
        on A.sample_datetime = D.sample_datetime
        and A.station_id = D.station_id
        and D.type = 4

      left join samples E 
        on A.sample_datetime = E.sample_datetime
        and A.station_id = E.station_id
        and E.type = 5

      left join samples F 
        on A.sample_datetime = F.sample_datetime
        and A.station_id = F.station_id
        and F.type = 6
      
      --add additional left joins here(samples G,...) for other measurement types
      
      --join our other main tables stations and areas - areas->stations->samples

      left join stations
        on stations.id = A.station_id

      left join areas
        on areas.id = stations.area_id
      
      where A.type = 1
        
      order by sample_datetime;

-- reset table sequence id to 1	

-- ALTER SEQUENCE areas_id_seq RESTART WITH 1;
-- ALTER SEQUENCE stations_id_seq RESTART WITH 1;
-- ALTER SEQUENCE samples_id_seq RESTART WITH 1;

-- dump tables as inserts
-- pg_dump -U postgres --column-inserts --data-only --table=areas shellbase > areas_insert.sql	  



	  
	  
