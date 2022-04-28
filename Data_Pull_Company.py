import os
import xlrd
import time
import pymysql
import sys
import pyodbc
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime as dt
from datetime import timedelta
import xlsxwriter

hostname = "192.168.3.2:1212"
dbname = "teleport_data123"
uname = "user"
pwd = "nyasha_password"
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

while True:
    conn_str = pyodbc.connect('Driver={Pervasive ODBC Interface};server=192.168.0.4;DBQ=SSCFEB21;')
    row = "SELECT * FROM VWPASTELDASH"
    df = pd.read_sql(row, conn_str)
    df = df.T.drop_duplicates().T
    df.to_sql('pasteldata_temp', engine, if_exists='replace', chunksize=10000, index=False, index_label='DocumentNumber')
    cnx = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=Q:\Teleport_Quoting System v1.154\TeleportCData.accdb;')
    querywk = "SELECT * FROM qryDataReport'"
    df = pd.read_sql(querywk, cnx)
    df['DatePrinted'] = pd.to_datetime(df['DatePrinted']) - pd.to_timedelta(7, unit='d')
    df = df.groupby([pd.Grouper(key='DatePrinted', freq='W-SUN')])['Line Total']
    df = df.sum('Line Total')
    df = df.reset_index()
    df = df.sort_values('DatePrinted')
    df.to_sql('powerappdatawk_temp', engine, if_exists='replace', chunksize=10000, index=False)
    supprow = "SELECT * FROM vwSuppZoomMaster"
    df = pd.read_sql(supprow, conn_str)
    df.to_sql('pastelsupplierdata_temp', engine, if_exists='replace', chunksize=10000, index=False, index_label='Code')
    openrow = "SELECT * FROM OpenItem"
    df = pd.read_sql(openrow, conn_str)
    df.to_sql('pastelopenitem_temp', engine, if_exists='replace', chunksize=10000, index=False)
    conn_str = pyodbc.connect('Driver={Pervasive ODBC Interface};server=192.168.0.4;DBQ=SSCFEB21;')
    row = "SELECT * FROM vwCustZoomMaster"
    df = pd.read_sql(row, conn_str)
    df = df.T.drop_duplicates().T
    df.to_sql('customerdata_temp', engine, if_exists='replace', chunksize=10000, index=False, index_label='Code')
    cnxx = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=Q:\TELEPORT_Quoting System v1.154\TELEPORTCData.accdb;')
    query = "SELECT * FROM qryDataReport'"
    df = pd.read_sql(query, cnxx)
    df.to_sql('powerappdata_temp', engine, if_exists='replace', chunksize=10000, index=False, index_label='QuoteID')
    pricetable = "SELECT * FROM tblPricing'"
    df = pd.read_sql(pricetable, cnxx)
    df.to_sql('tblpricingdata_temp', engine, if_exists='replace', chunksize=10000, index=False, index_label='ID')
    processtable = "SELECT * FROM tblProcessRates'"
    df = pd.read_sql(processtable, cnxx)
    df.to_sql('tblprocessrates_temp', engine, if_exists='replace', chunksize=10000, index=False, index_label='ID')
    files = [file for file in os.listdir('V:/Opus_SigmaData/Temp/')]
    cuttingdata = pd.DataFrame()

    for file in os.listdir('V:/Opus_SigmaData/Temp/'):
        name, ext = os.path.splitext(file)
        if ext == '.XLS':
            xf=pd.read_excel("V:/Opus_SigmaData/Temp/"+file)
            cuttingdata=pd.concat([cuttingdata,xf])
            cuttingdata.to_sql('sigmadata_temp', engine, if_exists='replace', chunksize=10000, index=False, index_label='Cutting Plan ID')
    
    cnx = pyodbc.connect(
        r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=Q:\TELEPORT_Quoting System v1.154\TELEPORT_CData.accdb;')
    querywk = "SELECT * FROM qryDataReport'"
    df = pd.read_sql(querywk, cnx)
    df = pd.DataFrame(df, columns=['QuoteID',
                                   'CompanyName',
                                   'ContactName',
                                   'Phone',
                                   'DeliveryTime',
                                   'DatePrinted',
                                   'Line Total',
				   'OrderNo',
				   'Status',
				   'OrderDate',
                                   'Name']);
    df['DatePrinted'] = pd.to_datetime(df['DatePrinted'])

    range_max = df['DatePrinted'].max()
    range_min = range_max - timedelta(days=10)

    sliced_df = df[(df['DatePrinted'] >= range_min) &
                   (df['DatePrinted'] <= range_max)]

# print(sliced_df)
    df = pd.DataFrame(sliced_df)
    df.to_sql('teleport_opus_dailyflows_10day', engine, if_exists='replace', chunksize=10000, index=False, index_label='QuoteID')

# writer = pd.ExcelWriter('TELEPORT_OPUS_DailyFlows.xlsx', engine='xlsxwriter')
# df.to_excel(writer, sheet_name='DailyFlows', index=False)
# writer.save()

# Create a Pandas Excel writer using XlsxWriter as the engine.
    datestring = dt.strftime(dt.now(), ' %Y_%m_%d')
    writer = pd.ExcelWriter('Teleport_OPUS_DailyFlows' + '_' + datestring + '.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='DailyFlows', index=False)

# Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['DailyFlows']

# Add some cell formats.
    format1 = workbook.add_format({'num_format': '', 'align': 'center', 'valign': 'vcenter'})
    format2 = workbook.add_format({'num_format': 'R #,##0.00', 'align': 'center', 'valign': 'vcenter'})

# Set the column width and format.
    worksheet.set_column('A:A', 25, format1)
    worksheet.set_column('B:B', 45, format1)
    worksheet.set_column('C:F', 25, format1)
    worksheet.set_column('G:G', 25, format2)
    worksheet.set_column('H:H', 25, format1)

# Close the Pandas Excel writer and output the Excel file.
    writer.save()

    df = df.groupby([pd.Grouper(key='Name')])['Line Total']
    df = df.sum('Line Total')
    df = df.reset_index()
    df = df.sort_values(by=['Line Total'], ascending=False)
    df = df.reset_index()
    df.to_sql('10daysaleschamp_temp', engine, if_exists='replace', chunksize=10000, index=True)
    time.sleep(180)