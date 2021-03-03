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

#import psycopg2

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



def parse_worksheet(xls_filename):
    samples = []
    try:
        #OPen up the excel file as a workbook.
        #workbook = load_workbook(filename=xls_filename)
        xl_file = pd.read_excel(xls_filename)
    except Exception as e:
        traceback.print_exc()
    else:
        #try:
            #For the time being, we just assume we are interested in the first worksheet.
            #worksheet = workbook.active
        #except Exception as e:
        #    traceback.print_exc()
        #else:
        for row_ndx, data_row in xl_file.iterrows():
        #for row_ndx, data_row in enumerate(worksheet.iter_rows()):
            print("Processing row: %d" % (row_ndx))
            try:
                sample = Sample()
                sample.station_id = data_row['StationID']
                parts = data_row['DateCollected'].split('/')
                #Zero pad the date and create a datetime object.
                date = datetime.strptime('%02d/%02d/%d' % (int(parts[0]), int(parts[1]), int(parts[2])), "%m/%d/%Y")
                time = data_row['Time']
                #We combine the individual date and time columns into a python datetime object.
                sample.sample_datetime = datetime.combine(date, time)
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

                samples.append(sample)

            except Exception as e:
                print("Error on row: %d" % (row_ndx))
                traceback.print_exc()
    return samples

def save_to_database(samples):
    return
def main():
    parser = optparse.OptionParser()
    parser.add_option("--ExcelFile", dest="xl_file", default=None,
                      help="Excel data file to import.")


    (options, args) = parser.parse_args()

    if options.xl_file is None:
        print("No Excel file to process, use --ExcelFile parameter.")
    else:
        print("Excel file: %s to be processed" % (options.xl_file))
        samples = parse_worksheet(options.xl_file)
        save_to_database(samples)
    return

if __name__ == "__main__":
    main()