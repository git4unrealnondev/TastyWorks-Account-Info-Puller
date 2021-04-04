import sqlite3
import aiohttp
import asyncio
import calendar
import json
import logging
from os import environ
from datetime import date, timedelta
from decimal import Decimal

from tastyworks.models import option_chain, underlying
from tastyworks.models.option import Option, OptionType
from tastyworks.models.order import (Order, OrderDetails, OrderPriceEffect,
                                     OrderType)
from tastyworks.models.session import TastyAPISession
from tastyworks.models.trading_account import TradingAccount
from tastyworks.models.underlying import UnderlyingType
from tastyworks.streamer import DataStreamer
from tastyworks.tastyworks_api import tasty_session


LOGGER = logging.getLogger(__name__)

class screwy():
	def __init__(self, session, account):
		self.session = session
		self.account = account
	async def get_balance(self):
		url = '{}/accounts/{}/balances'.format(
		self.session.API_url,
		self.account.account_number
		)

		async with aiohttp.request('GET', url, headers=self.session.get_request_headers()) as response:
			if response.status != 200:
				raise Exception('Could not get trading account balance info from Tastyworks...')
			data = (await response.json())['data']
		return data
	async def get_positons(self):
		url = '{}/accounts/{}/positions'.format(
		self.session.API_url,
		self.account.account_number
		)

		async with aiohttp.request('GET', url, headers=self.session.get_request_headers()) as response:
			if response.status != 200:
				raise Exception('Could not get open positions info from Tastyworks...')
			data = (await response.json())['data']['items']
		return data
		
	async def get_history(self):
		
		url = '{}/accounts/{}/transactions?page=2'.format(
		self.session.API_url,
		
		self.account.account_number
		)

		async with aiohttp.request('GET', url, headers=self.session.get_request_headers()) as response:
			if response.status != 200:
				raise Exception('Could not get history info from Tastyworks...')
			data = (await response.json())['data']['items']
			with open("data.json", "w") as outfile:
				json.dump(await response.json(), outfile)
		return data
		
	async def get_history_pull(self, start_at: str = None, 
		end_at: str = None, per_page: int = None, page_offset: int = 0):
		"""Get Transaction History
        Warning for Windows users:
            This will likely throw OSError: [WinError 10038] ...
            It doesn't seem to effect the actual event loop, however, it is recommended that
            you define the event loop policy for asyncio if you want to ensure quality before
            retrieving the event loop.
            Example:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            loop = asyncio.get_event_loop()
        Args:
            session (TastyAPISession): An active and logged-in session object against which to query.
            start_at (str): DD-MM-YYYY
            end_at (str): DD-MM-YYYY
            per_page (int): # of items for the API to return per page
            page_offset (int): Page # to start with
        Returns:
            list: A list of transactions (dict)
        """

		total_pages = page = 0
		history = []
		params = {"page-offset": page_offset}

		# Ensure we don't pass None to aiohttp.request
		if start_at is not None:
			params["start-at"] = start_at
		if end_at is not None:
			params["end-at"] = end_at
		if per_page is not None:
			params["per-page"] = per_page

		url = '{}/accounts/{}/transactions'.format(
			self.session.API_url,
			self.account.account_number
			)
        

		while params["page-offset"] <= total_pages:
			url = '{}/accounts/{}/transactions'.format(
				self.session.API_url,
				self.account.account_number
				)

			async with aiohttp.request('GET', url, headers=self.session.get_request_headers(), params=params) as response:
				if response.status != 200:
					raise Exception(
						f'Failed retrieving transaction history, Response status: {response.status}; message: {response.json()["error"]["message"]}'
                    )

				current_page = await response.json()
				history.extend(current_page["data"]["items"])

				if not total_pages and 'pagination' in current_page.keys():
					total_pages = current_page["pagination"]["total-pages"]

			params["page-offset"] += 1


		return history


class database():
	

	def __init__(self, db_loc):
		self.con = sqlite3.connect("./" + db_loc)
		self.c = self.con.cursor()
		self.initial_setup()
	
	def __del__(self):
		self.con.commit()
		self.con.close()
		
	def initial_setup(self):
		self.c.execute("CREATE TABLE IF NOT EXISTS active_options (symbol text, open text, exp text, corp text, dte int, breakeven real, strike real, premium real, amount int, fees real, exitprice real, closedate text)")
		
		self.c.execute("CREATE TABLE IF NOT EXISTS history (id int, accountnumber text, instrumenttype text, underlyingsymbol text, transactiontype text, porc text, strike text, exitdte text, action text, quantity int, price real, executedat text, transactiondate text, value real, valueeffect text, regulatoryfees real, regulatoryfeeseffect text, clearingfees real, clearingfeeseffect text, commission real, commissioneffect text, orderid int, exchange text, netvalue real, netvalueeffect text, isestimatedfee text)")
		
async def main_loop(session: TastyAPISession, streamer: DataStreamer, datab):

	sub_values = {
		"Quote": ["/ES"]
	}

	accounts = await TradingAccount.get_remote_accounts(session)
	LOGGER.info('Accounts available: %s', accounts)

	for acct in accounts:
		LOGGER.info("SELECTED ACCOUNT: %s", acct)
		
		orders = await Order.get_remote_orders(session, acct)
		#print(orders, acct)
		object_methods = [method_name for method_name in dir(TradingAccount) if callable(getattr(TradingAccount, method_name))]
		#for each in object_methods:
		#	print(each)

		test = screwy(session, acct)
		positions = await test.get_positons()
		
		#print("ACTIVE ORDERS: ", orders)	
		#for each in orders:
			#print(each.details)

		#historys = await test.get_history()
		#get_history_pull(self, start_at: str = None, end_at: str = None, per_page: int = None, page_offset: int = 0
		historys = await test.get_history_pull("01-01-1998", "12-12-3000", 500, 0)
		print(type(historys))
		for each in historys:
			
			#datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], each["regulatory-fees"], each["regulatory-fees-effect"], each["clearing-fees"], each["clearing-fees-effect"], each["commission"], each["commission-effect"], each["order-id"], each["exchange"], each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			if each["transaction-sub-type"] == "Balance Adjustment":
				#
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], "Balance Adjustment", None, each["transaction-type"], None, None, None, None, None, None, each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Dividend":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, None, None, None, each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Assignment":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, None, each["quantity"], None, each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Expiration":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, None, each["quantity"], None, each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Exercise":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, None, each["quantity"], None, each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			#elif each["transaction-sub-type"] == "Buy to Open":
				#print (each)
				#datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], each["description"].split( )[4], each["description"].split( )[5], each["description"].split( )[3], each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, each["clearing-fees"], each["clearing-fees-effect"], None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			
			elif each["transaction-sub-type"] in ("Buy to Open", "Sell to Open", "Sell to Close", "Buy to Close") and each["instrument-type"] == "Equity Option":
			#elif ( each["instrument-type"] == "Equity Option" and each["transaction-sub-type"] == "Buy to Open" or each["transaction-sub-type"] == "Sell to Open" or each["transaction-sub-type"] == "Sell to Close" or each["transaction-sub-type"] == "Buy to Close"):
				print (each)
				print (each["description"].split( ))
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], each["description"].split( )[4], each["description"].split( )[5],each["description"].split( )[3], each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], each["regulatory-fees"], each["regulatory-fees-effect"], each["clearing-fees"], each["clearing-fees-effect"], each["commission"], each["commission-effect"], each["order-id"], each["exchange"], each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Buy to Open" or each["transaction-sub-type"] == "Sell to Open" or each["transaction-sub-type"] == "Sell to Close" or each["transaction-sub-type"] == "Buy to Close" and each["instrument-type"] == "Equity":
				#print (each)
				print (each["description"].split( ))
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, each["clearing-fees"], each["clearing-fees-effect"], None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			#elif each["transaction-sub-type"] == "Sell to Close" and each["instrument-type"] == "Equity":
				#print (each)
				#print (each["description"].split( ))
				#datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], each["regulatory-fees"], each["regulatory-fees-effect"], each["clearing-fees"], each["clearing-fees-effect"], None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			#elif each["transaction-sub-type"] == "Sell to Close" and each["instrument-type"] == "Equity Option":
				#print (each)
				#print (each["description"].split( ))
				#datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], each["description"].split( )[4], each["description"].split( )[5],each["description"].split( )[3], each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], each["regulatory-fees"], each["regulatory-fees-effect"], each["clearing-fees"], each["clearing-fees-effect"], None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Credit Interest":		
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["transaction-sub-type"], None, each["transaction-type"], None,None, None, None, None, None, None, each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Transfer" and each["transaction-type"] == "Money Movement":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], None, None, each["transaction-type"], None,None, None, None, None, None, each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "ACAT":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["transaction-sub-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Transfer":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			elif each["transaction-sub-type"] == "Special Dividend":
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, None, None, None, each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], None, None, None, None, None, None, None, None, each['net-value'], each['net-value-effect'], each['is-estimated-fee']))
			else:
				print("History: " , each, type(each), each["id"])
				print(each.keys())
				datab.c.execute('insert into history values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (each["id"], each["account-number"], each["instrument-type"], each["underlying-symbol"], each["transaction-type"], None, None, None, each["action"], each["quantity"], each["price"], each["executed-at"], each["transaction-date"], each["value"], each["value-effect"], each["regulatory-fees"], each["regulatory-fees-effect"], each["clearing-fees"], each["clearing-fees-effect"], each["commission"], each["commission-effect"], each["order-id"], each["exchange"], each['net-value'], each['net-value-effect'], each['is-estimated-fee']))

		
def main():	
	main = database("main.db")

	tasty_client = tasty_session.create_new_session("Username", "Password")

	streamer = DataStreamer(tasty_client)


	LOGGER.info('Streamer token: %s' % streamer.get_streamer_token())
	loop = asyncio.get_event_loop()

	loop.run_until_complete(main_loop(tasty_client, streamer, main))



main()
