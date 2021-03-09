# This script runs daily at 6pm
# Scheduled using Azure Logic Apps

import requests
from bs4 import BeautifulSoup
import time
import csv
import pyodbc
import pandas as pd


# MarketWatch.com shows instruments for Sri Lanka in the following URLs as two pages
# https://www.marketwatch.com/tools/markets/stocks/country/sri-lanka
# https://www.marketwatch.com/tools/markets/stocks/country/sri-lanka/2

# Latter part of the instrument urls are added to a list (ex:['/investing/Stock/ABAN.N0000?countryCode=LK',
#                                            '/investing/Stock/AFSL.N0000?countryCode=LK', etc...]

def get_partial_instrument_links_from_page(url, maxlinks):
    list_of_instrument_urls_in_page = []
    items = requests.get(url)
    soup_it = BeautifulSoup(items.content,
                            'html5lib')

    while len(list_of_instrument_urls_in_page) < maxlinks:
        for links_page1 in soup_it.find_all('a'):
            link = links_page1.get('href')

            if link:
                if '/investing/Stock/' in link:
                    list_of_instrument_urls_in_page.append(link)
    return list_of_instrument_urls_in_page


def add_instrument_data_to_dic(dic, name_of_mkt_data, value_of_mkt_data):
    if name_of_mkt_data not in dic:
        dic[name_of_mkt_data] = []
    dic[name_of_mkt_data].append(value_of_mkt_data)


url_page1 = 'https://www.marketwatch.com/tools/markets/stocks/country/sri-lanka'
url_page2 = 'https://www.marketwatch.com/tools/markets/stocks/country/sri-lanka/2'

list_of_instruments_page1 = get_partial_instrument_links_from_page(url_page1, 150)
list_of_instruments_page2 = get_partial_instrument_links_from_page(url_page2, 124)

print("URLs from page 1: " + str(len(list_of_instruments_page1)))
print("URLs from page 2: " + str(len(list_of_instruments_page2)))

List_of_instruments = list_of_instruments_page1 + list_of_instruments_page2

# # # # # # # # # # # # #

List_of_dic = []

instrument_data = {}

# The following for loop will:
#           1. Create the instrument urls
#           2. Scrape the webpage to get the needed information from the urls
#           3. Add them to instrument_data dictionary (one dictionary to each instrument)
#           4. Append the dictionaries to a list

for instrument in List_of_instruments:
    url = 'https://www.marketwatch.com' + instrument

    r = requests.get(url)

    soup = BeautifulSoup(r.content,
                         'html5lib')
    soup.prettify()

    data_for_dic = soup.findAll("li", {"class": "kv__item"})

    for row in data_for_dic:
        tag = row.find_all("small", {"class": "label"})
        value = row.find_all("span", {"class": "primary"})

        add_instrument_data_to_dic(instrument_data, tag[0].string, value[0].string)

    instrument_data['Instrument'] = instrument[17:]

    List_of_dic.append(instrument_data)

    print(len(List_of_dic))

    time.sleep(1)

# Writing a csv file using the data

csv_columns = ['Open', 'Day Range', '52 Week Range', 'Market Cap',
               'Shares Outstanding', 'Public Float', 'Beta', 'Rev. per Employee', 'P/E Ratio', 'EPS',
               'Yield', 'Dividend', 'Ex-Dividend Date', 'Short Interest', '% of Float Shorted',
               'Average Volume', 'Instrument']

csv_file = "data/Market_key_data.csv"

try:
    with open(csv_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in L:
            writer.writerow(data)
except IOError:
    print("I/O error")

# Copy the data into Market_key_data_sanitized.csv after removing unnecessary characters and words in
#                                                                                        Market_key_data.csv

with open("data/Market_key_data.csv", "r") as infile, open("Market_key_data_sanitized.csv", "w") as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    conversion = set("]['")
    for row in reader:
        newrow = ["".join("" if c in conversion else c for c in entry) for entry in row]
        for index, item in enumerate(newrow):
            newrow[index] = newrow[index].replace("රු.", "")
            newrow[index] = newrow[index].replace("?countryCode=LK", "")
            newrow[index] = newrow[index].replace("N/A", "0")
            newrow[index] = newrow[index].replace(',', '')
        writer.writerow(newrow)

# Feed the data into a pandas dataframe
# Edit the column names to match the column names of the database
# change the data types to match the database accordingly

df = pd.read_csv("/Users/Bhagya/Documents/cse_mktdata_dashboard_scripts/Market_key_data_sanitized.csv")
df.columns = df.columns.str.replace('%', '')
df.columns = df.columns.str.replace('[/,-]', ' ')
df.columns = df.columns.str.replace(' ', '_')

df.rename(columns={'52_Week_Range': 'my_52_Week_Range', '_of_Float_Shorted': 'of_Float_Shorted',
                   'Rev._per_Employee': 'Rev_per_Employee'}, inplace=True)

df[['Open', 'Beta', 'P_E_Ratio', 'EPS', 'Dividend', 'Short_Interest', 'of_Float_Shorted']] = df[
    ['Open', 'Beta', 'P_E_Ratio', 'EPS', 'Dividend', 'Short_Interest', 'of_Float_Shorted']].astype(str)
print(df.dtypes)

# Connection between the python script and the database is now been made
# The data is overwritten to the database
# Overwriting is done because the interest is about the data on a particular day for each instrument.
#                                                                                      (ex: EPS, Market_cap)

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};Server=tcp:cse-dashboard-db-server.database.windows.net,"
    "1433;Database=CSEDB;Uid=bhagyaw;Pwd=password;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")

cursor = conn.cursor()

cursor.execute('''
                DELETE FROM CSEDB.dbo.Market_data_test
               ''')

for row in df.itertuples(index=True, name='Pandas'):
    cursor.execute('INSERT INTO Market_data("Open", Day_Range, "_52_Week_Range", Market_Cap, Shares_Outstanding, '
                   'Public_Float, Beta, Rev_per_Employee, P_E_Ratio, EPS, Yield, Dividend, Ex_Dividend_Date, '
                   'Short_Interest, of_Float_Shorted, Average_Volume, Instrument) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'
                   '?,?,?)',
                   row.Open, row.Day_Range, row.my_52_Week_Range, row.Market_Cap, row.Shares_Outstanding,
                   row.Public_Float, row.Beta, row.Rev_per_Employee, row.P_E_Ratio, row.EPS, row.Yield, row.Dividend,
                   row.Ex_Dividend_Date, row.Short_Interest, row.of_Float_Shorted, row.Average_Volume, row.Instrument)

conn.commit()
cursor.close()
conn.close()
