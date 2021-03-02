#set database password as needed below

#from glob import glob
import sys

from pandas import DataFrame, read_csv
import pandas as pd
import psycopg2
import datetime


#open the db
try:
    conn = psycopg2.connect("dbname='shellbase' user='postgres' password='???'")
except:
    print ("Failed to open the db.")
    

#Initiate the cursor
cursor = conn.cursor()

file = sys.argv[1]

#df = pd.read_excel(file, sheet_name="Sheet1",skiprows=2,header=None)
df = pd.read_csv(file, skiprows=1,header=None)

'''
import xlrd
xls = xlrd.open_workbook(file, on_demand=True)
print xls.sheet_names()
quit
'''

print(df[1])

query = "INSERT INTO samples (dataset_id,m_date,temp,spcond,sal,do_pct,do_mgl,depth,ph,turb) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

dataset_id = ''

for i in df.index:

    if isinstance(df[0][i], datetime.date) and df[0][i] is not pd.NaT:
        m_date = df[0][i].strftime('%Y-%m-%d')
        m_date = m_date+' '+df[1][i].strftime('%H:%M:%S')
        #print ":"+m_date+":"
        
        temp = df[2][i]
        if temp == '.':
            temp = None
        spcond = df[3][i]
        if spcond == '.':
            spcond = None
        sal = df[4][i]
        if sal == '.':
            sal = None
            
        do_pct = df[5][i]
        #if do_pct == '.':
        #    do_pct = None
        if isinstance(do_pct, (float)) == False:
            do_pct = None
            
        do_mgl = df[6][i]
        if isinstance(do_mgl, datetime.date):
            do_mgl = None
        if do_mgl == '.':
            do_mgl = None
        depth = df[7][i]
        if depth == '.' or depth == '..':
            depth = None
        ph = df[8][i]
        if ph == '.':
            ph = None
        turb = df[9][i]
        if turb == '.' or turb == '..':
            turb = None
            
        values = (dataset_id, m_date,temp,spcond,sal,do_pct,do_mgl,depth,ph,turb)

        print (values)

        # Execute sql Query
        '''
        try:
            cursor.execute(query, values)
        except psycopg2.IntegrityError:
            conn.rollback()
        else:
            conn.commit()
        '''
        
    #print df[0][i].date.today().year
    
# Close the cursor
cursor.close()

# Close the database connection
conn.close()

