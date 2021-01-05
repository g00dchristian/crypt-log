import sqlite3
import time
import datetime
import ccxt
from languageHandled import languageHandler
from sql_minute_data import sql_data_pull, sql_log_minute

bn = ccxt.binance()


'''
Minute data for Binance Pairs

1. Pull data from SQL minute database 
2. Pull data from binance /pair
3. Run through SQL logs and then append missing data

'''


class Minute_Data_Log():
	def __init__(self, pairs, database):
		print('Minute Data Log Initiated')
		self.pairs=pairs
		self.minute_log_db=database
		for pair in pairs:
			self.Binance_Pull(pair)


	def Binance_Pull(self, pair):
		"""Binance Rest API returns 8 hours (480 minutes) of minute candle data"""
		BN_pair_data=bn.fetch_ohlcv(pair, timeframe='1m')
		SQL_pair_data=sql_data_pull(pair, BN_pair_data[0][0], self.minute_log_db)
		if len(SQL_pair_data[1]) == 0:
			print('error: missing logs -- logging all data')
			#log all data
			sql_log_minute(pair,BN_pair_data[:-1],self.minute_log_db)
		else:
			for candle in BN_pair_data[:-1]:
				try:
					if SQL_pair_data[0][candle[0]]!=candle[5]:
						print('error: mismatch in log data')
						print(f'SQL Candle: {SQL_pair_data[0][candle]}\nBN Candle: {candle}')
				except:
					sql_log_minute(pair,[candle],self.minute_log_db)
					
		#sql_log_minute(pair,BN_pair_data[:-1],self.minute_log_db)








Minute_Data_Log(['BTC/USDT','ETH/USDT'],r"C:\Users\RP Trading\Desktop\Python Scripts\Minute Data Logs\minute_ticker_log.db")

#MAKE THE VOLUME CHECK AND THEN SEND REST TO LOG - BE SURE TO DIVIDE AND MULTIPLY UNIX BY 1000