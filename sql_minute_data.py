from uuid import uuid4
from datetime import datetime
import time
import sqlite3

def sql_data_pull(pair, start_date, dbname):
	conn = sqlite3.connect(dbname)
	c = conn.cursor()
	logCheck = {}
	logData = []
	c.execute('SELECT  Unix, Volume  FROM "{}" WHERE Unix >= ?'.format(pair.replace('"', '""')), (start_date,))

	for entry in c.fetchall():
		logCheck.update({entry[0]:entry[1]})
		logData.append(entry[0])
	conn.close()
	#print(logCheck)
	return [logCheck, logData]




def sql_log_minute(pair, datum, dbname):
	print(f'Data has been logged to SQL {dbname}')
	conn = sqlite3.connect(dbname)
	c = conn.cursor()

	#-- ENTER CONFLUENCE_RATING VALUES -------------------------------------------------------------
	#c.execute('INSERT INTO {}(UUID,Market, Unix, UTC_Date, Local_Date, Open, High, Low, Close, Volume) VALUES(:UUID, :Market, :Unix, :UTC_Date, :Local_Date, :Open, :High, :Low, :Close, :Volume)'.format(pair.replace('"', '"')),
	for data in datum:
		entry_uuid = str(uuid4())
		entry_uuid_string = '`'+entry_uuid+'`'
		c.execute('INSERT INTO "{}"(UUID,Market, Unix, UTC_Date, Local_Date, Open, High, Low, Close, Volume) VALUES(:UUID, :Market, :Unix, :UTC_Date, :Local_Date, :Open, :High, :Low, :Close, :Volume)'.format(pair.replace('"', '""')),
				{
				'UUID':entry_uuid,
				'Market':pair,
				'Unix':data[0],
				'UTC_Date':time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime((data[0])/1000)),
				'Local_Date':time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((data[0]/1000))),
				'Open':data[1],
				'High':data[2],
				'Low':data[3],
				'Close':data[4],
				'Volume':data[5]
				})
	conn.commit()


	# SWAP DATA AND DATUM
