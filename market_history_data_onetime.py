# This is a one time script
# Collects historical data for all instruments in CSE for 3 years and fills the database

import requests
import time
import pyodbc
import pandas as pd
import glob


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

# 3 urls were used because it is allowed only to fetch historical data of one year at a time.

for instrument in all_instruments_in_one_list:

    url_1 = 'https://www.marketwatch.com/investing/stock/' + instrument + '/downloaddatapartial?' \
                                                                          'partial=true&startdate=02/03/2020%2000:00' \
                                                                          ':00&enddate=02/01/2021%2000:00:00&' \
                                                                          'daterange=d30&frequency=p1d&csvdownload' \
                                                                          '=true&downloadpartial=false&newdates=false' \
                                                                          '&countrycode=lk '

    url_2 = 'https://www.marketwatch.com/investing/stock/' + instrument + '/downloaddatapartial?' \
                                                                          'partial=true&startdate=02/04/2019%2000:00' \
                                                                          ':00&enddate=02/03/2020%2000:00:00&' \
                                                                          'daterange=d30&frequency=p1d&csvdownload' \
                                                                          '=true&downloadpartial=false&newdates=false' \
                                                                          '&countrycode=lk '

    url_3 = 'https://www.marketwatch.com/investing/stock/' + instrument + '/downloaddatapartial?' \
                                                                          'partial=true&startdate=02/02/2018%2000:00' \
                                                                          ':00&enddate=02/04/2019%2000:00:00&' \
                                                                          'daterange=d30&frequency=p1d&csvdownload' \
                                                                          '=true&downloadpartial=false&newdates=false' \
                                                                          '&countrycode=lk '

    # Get requests were made to fetch data for the urls

    response1 = requests.get(url_1, allow_redirects=True)
    response2 = requests.get(url_2, allow_redirects=True)
    response3 = requests.get(url_3, allow_redirects=True)

    response_list = [response1, response2, response3]

    # Write a file with the data for each instrument (ex:ABAN.N0000.csv)

    instrument_file_name = instrument + '.csv'

    for r in response_list:
        f = open(instrument_file_name, 'wb')
        f.seek(0)
        f.write(r.content)

        f.close()
        time.sleep(1)

list_of_file_names = []
path = "/Users/Bhagya/Documents/cse_mktdata_dashboard_scripts/*.csv"

# The filepath of each file was retrieved by specifying the directory where they are located.
# They were added to the list_of_file_names list.
# The instrument label was taken out of the filepath and appended to each row of the file
# and all the files were written to a new file named All_files_in_one_file.csv


for filename in glob.glob(path):
    list_of_file_names.append(filename)

with open("all_files_in_one_file.csv", "w") as f1:
    for filename in list_of_file_names:
        part_of_instrument_name = filename[30:]
        instrument_name = part_of_instrument_name[:-4]
        with open(filename) as f:
            for line in f:
                if instrument_name + ',Date,Open,High,Low,' not in line:  # ignore repeating headers
                    new_row = line + instrument_name + ','
                    new_row = new_row.replace("Rs.", "")
                    new_row = new_row.replace('"', '')
                    f1.write(new_row)

# Writing data to a pandas dataframe
# Changing data types of columns of the dataframe to match the database

df = pd.read_csv("/Users/Bhagya/Documents/cse_mktdata_dashboard_scripts/all_files_in_one_file.csv")

df['Instrument'] = df['Instrument'].astype(str)
df['Date'] = df['Date'].astype('datetime64[ns]')

# Connection between the python script and the database is now been made
# The data is written to the database


conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};Server=tcp:cse-dashboard-db-server.database.windows.net,"
    "1433;Database=CSEDB;Uid=bhagyaw;Pwd=password@;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")

cursor = conn.cursor()

for row in df.itertuples():
    cursor.execute(
        'INSERT INTO Historical_data(Instrument, Date, "Open", High, Low, "Close", Volume) values(?,?,?,?,?,?,?)',
        row.Instrument, row.Date, row.Open, row.High, row.Low, row.Close, row.Volume)

conn.commit()
cursor.close()
conn.close()
