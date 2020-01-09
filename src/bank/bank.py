#
# This script will simulate the bank server. It will send/receive CREDIT and DEBIT messages
# with random amounts (interval <10000;50000>) to/from its' direct peers. Every DB server has 
# it's own DB.
# 
# Banks use ZeroMQ to communicate with eachother.
#

import mysql.connector
import logging
import zmq
from random import randrange

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
		

class Message:
	"""
	Message sent between banks
	"""
	
	def __init__(self, type, amount):
		self.type = type
		self.amount = amount

class Bank:
	"""
	Implementation of the bank server.
	"""
	
	def __init__(self, host, port, debug, db_connector, other_banks):
		"""
		Initializes this server with given values.
		
		:param string host: IP address of this bank.
		:param port: Port this bank should listen on. If None, bank will not expect any connections.
		:param list other_banks: List of banks this one hould connect to via ZeroMQ. Each entry should be in format <host>:<port>.
		"""
		self._host = host
		self._port = port
		self._debug = debug
		self._db_connector = db_connector
		self._context = zmq.Context()
		
		# socket to given peer can be accessed as _peers["host:port"]
		self._peers = []
		
		self._init_queues(other_banks)
	
	def _init_queues(self, other_banks):
		"""
		Initializes connections to other banks and starts to listen on given port (if the port is set).
		"""
		
		# start listening if port is set
		if self._port is not None:
			self._socket = self._context.socket(zmq.PAIR)
			self._socket.bind("tcp://*:%s" % port)
			self._poller.register(self._socket)
		else:
			self._socket = None
			
		# connect to neighbours
		self._poller = zmq.Poller()
		for other_bank in other_banks:
			s = self._context.socket(zmq.PAIR)
			s = self._context.connect("tcp://%s" % other_bank)
			self._peers.append(s)
			self._poller.register(s)
			
	def _start_server(self):
		"""
		Starts banking server - message sending and receiving.
		"""
		
		logging.info("Starting receive/send loop.")
		self._should_run = True
		while self._should_run:
			self._recv_messages()
			self._generate_message()
			
		logging.info("Loop finished gracefully.")
			
	
	def _generate_message(self):
		"""
		Generate and send one message to direct neighbor.
		"""
		logging.info("Generating message.")
		amount = 10000 + randrange(40001)
		
		# choose target to send message to
		# either main socket this bank is listening on
		# or one of the peers this bank is connected to
		peer_cnt = len(self._peers)
		total_sockets = peer_cnt + (1 if self._socket is not None else 0)
		target = None
		rand = randrange(total_sockets)
		if rand < peer_cnt:
			target = self._peers[rand]
		else:
			target = self._socket
		
		if randrange(2) == 0:
			self._send_credit(amount, target)
		else:
			self._send_debit(amount, target)
	
	def _recv_messages(self):
		"""
		Poll for messages from ZeroMQ. Timeout is between 10 and 100 ms.
		"""
		
		t = 10 + randrange(101)
		socks = dict(self._poller.poll(timeout = t))
		if len(socks) > 0:
			logging.info("%d sockets polled." % len(socks))
			# todo: go through polled sockets and process messages
			# todo: convert json to Message 
		
		
	def _process_messages(self, message):
		"""
		Process one message from queue.
		
		:param message: Message received from queue.
		"""
		if message.type == "CREDIT":
			self._credit(message.amount)
		else:
			self._debit(message.amount)
	
	def _credit(self, amount):
		"""
		Credits given amount to this bank.
		"""
		logging.info("Credit: " + str(amount))
		self._db_connector.credit_money(amount)
	
	def _debit(self, amount, target):
		"""
		Sends given amount of money back to target or sends REFUSE if there's not enough money in the account.
		"""
		logging.info("Debit: " + str(amount) + " message from " + str(target))
		self._send_credit(amount, target)
	
	def _send_credit(self, amount, target):
		"""
		Deducts given amount from this bank's account and sends CREDIT message to target.
		
		:param Socket target: Socket to send message to.
		"""
		curr_amount = self._db_connector.get_amount()
		if curr_amount < amount:
			self._send_refuse(target)
		else:
			self._db_connector.debit_money(amount)
			target.send_json(Message("CREDIT", amount))
			
	def _send_debit(self, amount, target):
		"""
		Sends DEBIT message for given amount to given target.
		"""
		target.send_json(Message("DEBIT", amount))
		
	def _send_refuse(self, target):
		"""
		Sends REFUSED message to target.
		"""
		target.send_json(Message("REFUSED", -1))
		
				
		
		
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

