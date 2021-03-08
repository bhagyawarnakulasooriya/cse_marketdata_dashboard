# This runs daily
# ex: Using Azure Logic Apps to schedule at 6pm daily

import requests
import glob
from datetime import datetime
from datetime import date, timedelta
import pandas as pd
import pyodbc

# A file was constructed by copying the Instrument names of the Marketwatch web page.
#          (ex: Abans Electricals PLC (ABAN.N0000)	XCOL	Housewares
#               Abans Finance PLC (AFSL.N0000)	XCOL	Finance Companies ....etc)
# The interest is to create a list of instrument labels from this file.
# That is to extract the part within the brackets.
# The code has been written to extract the part within the brackets.
# But Some instruments contain more than one pair of brackets
#                       (ex:Cargills (Ceylon) PLC (CARG.N0000)	XCOL	Food Retail)
# These type of instrument names are included in excluded_files.txt.csv with the first bracket being deleted.
# The rest of readily available instrument names are included in available_file.

def get_instrument_list_from_file(file_name):
    instrument_list = []
    with open(file_name, 'r') as instrument_file:
        for line in instrument_file:
            text = str(line)

            start = text.find('(') + 1
            end = text.find(')')

            instrument_label = text[start:end]
            instrument_list.append(instrument_label)
        print(instrument_list)
        return instrument_list


readily_available_instruments_list = get_instrument_list_from_file('available_files.txt')
excluded_instruments_list = get_instrument_list_from_file('excluded_files.txt')

all_instruments_in_one_list = readily_available_instruments_list + excluded_instruments_list

today = datetime.today().strftime('%m-%d-%y')
yesterday = (date.today() - timedelta(days=1)).strftime('%m-%d-%y')

for instrument in all_instruments_in_one_list:
    url = 'https://www.marketwatch.com/investing/stock/' + instrument + '/downloaddatapartial?' \
                                                                        'partial=true&startdate=yesterday%2000:00' \
                                                                        ':00&enddate=today%2000:00:00&' \
                                                                        'daterange=d30&frequency=p1d&csvdownload' \
                                                                        '=true&downloadpartial=false&newdates=false' \
                                                                        '&countrycode=lk '

    response = requests.get(url, allow_redirects=True)    # Get requests were made to fetch data for the url

    # Write a file with the data for each instrument (ex:ABAN.N0000.csv)

    instrument_file_name = instrument + '.csv'

    f = open(instrument_file_name, 'wb')

    f.write(response.content)

    f.close()

list_of_file_names = []
path = "/Users/Bhagya/Documents/cse_mktdata_dashboard_scripts/*.csv"

# The filepath of each file was retrieved by specifying the directory where they are located.
# They were added to the list_of_file_names list.
# The instrument label was taken out of the filepath and appended to each row of the file
# and all the files were written to a new file named Historical_data_daily.csv


for filename in glob.glob(path):
    list_of_file_names.append(filename)

with open("Historical_data_daily.csv", "") as f1:
    for filename in list_of_file_names:
        part_of_instrument_name = filename[30:]
        instrument_name = part_of_instrument_name[:-4]
        with open(filename) as f:
            for line in f:
                if instrument_name + ',Date,Open,High,Low,' not in line:    # ignore repeating headers
                    new_row = line + instrument_name + ','
                    new_row = new_row.replace("Rs.", "")
                    new_row = new_row.replace('"', '')
                    f1.write(new_row)

# Writing data to a pandas dataframe
# Changing data types of columns of the dataframe to match the database

df = pd.read_csv("/Users/Bhagya/Documents/cse_mktdata_dashboard_scripts/Historical_data_daily.csv")

# Connection between the python script and the database is now been made
# The data is appended to the database

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};Server=tcp:cse-dashboard-db-server.database.windows.net,"
    "1433;Database=CSEDB;Uid=bhagyaw;Pwd=password;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")

cursor = conn.cursor()

for row in df.itertuples(index=True, name='Pandas'):
    cursor.execute(
        'INSERT INTO Historical_data(Instrument, Date, Open, High, Low, Close, Volume) values(?,?,?,?,?,?,?,?,?,?,?,'
        '?,?,?,?,?,?)',
        row.Instrument, row.Date, row.Open, row.High, row.Low, row.Close, row.Volume)

conn.commit()
cursor.close()
conn.close()
