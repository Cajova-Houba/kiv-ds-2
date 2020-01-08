#
# This script will simulate the bank server. It will accept transaction orders
# from the shuffler, sort them by their id and execute them. There will be a couple
# of instances of this server running at the same time and each of them will use its'
# own DB.
#
# API offered by bank is described in doc/bank-api.yml.
#
# I used Bottle (https://falcon.readthedocs.io/en/stable/) framework for the API.
# It can be installed by 
# pip install bottle
#
# Test it using curl:
#  curl -X POST --header "Content-Type: application/json" --data '{"amount":5, "id":1}' localhost:8100/credit 
#

from bottle import Bottle, template, request, HTTPResponse
import mysql.connector
import heapq
import logging

class Transaction:
	"""
	Wrapper for bank transaction so that heap can be used.
	"""
	
	def __init__(self, id, amount, type):
		self.id = id
		self.amount = amount
		self.type = type
		
	def __cmp__(self, other):
		return self.id < other.id
		
	def __lt__(self, other):
		return self.id < other.id
		
	def is_debit(self):
		return self.type == "debit"
		
	def to_string(self):
		return "{id: %s, amount: %s, type: %s}" % (self.id, self.amount, self.type)

class DbConnector:
	"""
	Implementation of DB connector.
	"""
	
	def __init__(self, host = 'localhost', user = 'root', password = 'r00t', schema = 'bank_server', account_id = 1):
		self._host = host
		self._user = user
		self._password = password
		self._schema = schema
		self._account_id = account_id
		self._connection = self._get_connection()
	
	def _get_connection(self):
		return mysql.connector.connect(
			host=self._host,
			user=self._user,
			passwd=self._password,
			database=self._schema
		)
	
	def _perform_update_query(self, query, amount):
		"""
		Executes and commits the update query.
		"""
		db = self._connection
		cursor = db.cursor()
		cursor.execute(query, (amount, self._account_id))
		db.commit()
		cursor.close()
	
	def close_connection(self):
		self._connection.close()
	
	def credit_money(self, amount):
		"""
		Credits given amount of money to the account.
		"""
		logging.info("crediting %d" % amount)
		self._perform_update_query(
			"update account set balance = balance + %s where id = %s;",
			amount
		)
		
	def debit_money(self, amount):
		"""
		Debits given amount of money from the account.
		"""
		logging.info("debiting %d" % amount)
		self._perform_update_query(
			"update account set balance = balance - %s where id = %s;",
			amount
		)
	
	def get_amount(self):
		"""
		Returns the current amount of money in the bank account.
		"""
		db = self._connection
		cursor = db.cursor()

		cursor.execute("select balance from account")
		return cursor.fetchone()
		
		

class Bank:
	"""
	Implementation of the bank server.
	"""
	
	def __init__(self, host, port, debug, db_connector):
		"""
		Initializes this server with given values.
		"""
		self._host = host
		self._port = port
		self._debug = debug
		self._db_connector = db_connector
		self._expected_id = 0
		self._transaction_heap = []
		self._app = Bottle()
		self._route()
	
	def start(self):
		"""
		Starts the server.
		"""
		self._app.run(host=self._host, port=self._port, debug = self._debug)
	
	def _route(self):
		"""
		Sets up routes to endpoints.
		"""
		self._app.route('/credit', method="POST", callback=self._credit)
		self._app.route('/debit', method="POST", callback=self._debit)		
		
	def _credit(self):
		"""
		Handler for /credit API.
		"""
		transaction = request.json
		logging.info("Credit: " + str(transaction))
		self._add_transaction(Transaction(transaction["id"], transaction["amount"], "credit"))
		return HTTPResponse(status = 200)
	
	def _debit(self):
		"""
		Handler for /debit API.
		"""
		transaction = request.json
		logging.info("Debit: " + str(transaction))
		self._add_transaction(Transaction(transaction["id"], transaction["amount"], "debit"))
		return HTTPResponse(status = 200)
	
	def _add_transaction(self, transaction):
		"""
		Adds transaction to the heap and if the expected id is at the top of heap
		the transaction is executed.
		
		:param Transaction transaction: Transaction to be added to the queue.
		"""
		heapq.heappush(self._transaction_heap, transaction)
		
		# expected transaction id -> pop from heap and execute it
		curr_balance = None
		while len(self._transaction_heap) > 0 and self._transaction_heap[0].id == self._expected_id:
			t = heapq.heappop(self._transaction_heap)
			logging.info("Executing transaction: %s" % t.to_string())
			if t.is_debit():
				self._db_connector.debit_money(t.amount)
			else:
				self._db_connector.credit_money(t.amount)
				
			self._expected_id = self._expected_id + 1
			curr_balance = self._db_connector.get_amount()
			
		if curr_balance is not None:
			with open("balance.txt", "w") as out_f:
				out_f.write(str(curr_balance[0]))
				out_f.write("\n")
			
		
				
		
		
def main():
	"""
	Main method of the script, starts the server.
	"""
	logging.basicConfig(filename='log.txt',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
	logging.info("Bank starting")
	db_connector = DbConnector()
	amount = db_connector.get_amount();
	if amount is not None:
		logging.info("Original balance: %s" % str(amount[0]));
	else:
		logging.warning("No original amount.");
	
	bank = Bank('0.0.0.0', 8100, True, db_connector)
	bank.start()
	db_connector.close_connection()
		

# Script body
main()

