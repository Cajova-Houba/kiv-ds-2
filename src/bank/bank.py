#
# This script will simulate the bank server. It will send/receive CREDIT and DEBIT messages
# with random amounts (interval <10000;50000>) to/from its' direct peers. Every DB server has 
# it's own DB.
# 
# Banks use ZeroMQ to communicate with eachother.
#
import os
import sys

import mysql.connector
import logging
import zmq
from random import randrange


class DbConnector:
	"""
	Implementation of DB connector.
	"""

	def __init__(self, host='localhost', user='root', password='r00t', schema='bank_server', account_id=1):
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
		logging.debug("DB: Crediting %d" % amount)
		self._perform_update_query(
			"update account set balance = balance + %s where id = %s;",
			amount
		)

	def debit_money(self, amount):
		"""
		Debits given amount of money from the account.
		"""
		logging.debug("DB: Debiting %d" % amount)
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
		return cursor.fetchone()[0]


class Message:
	"""
	Message sent between banks
	"""

	@staticmethod
	def from_dict(other):
		return Message(other["type"], other["amount"])

	@staticmethod
	def credit(amount):
		return Message("CREDIT", amount)

	@staticmethod
	def debit(amount):
		return Message("DEBIT", amount)

	@staticmethod
	def refused():
		return Message("REFUSED", -1)

	@staticmethod
	def connect():
		return Message("CONNECT", -1)

	@staticmethod
	def marker(marker_id):
		return Message("MARKER", marker_id)

	@staticmethod
	def ok():
		return Message("OK", -1)

	def __init__(self, message_type, amount):
		self.type = message_type
		self.amount = amount

	def is_credit(self):
		return self.type == "CREDIT"

	def is_debit(self):
		return self.type == "DEBIT"

	def is_connect(self):
		return self.type == "CONNECT"

	def is_ok(self):
		return self.type == "OK"

	def is_marker(self):
		return self.type == "MARKER"

	def to_dict(self):
		return dict(
			type=self.type,
			amount=self.amount
		)

	def __str__(self):
		return str(self.to_dict())


class LocalState:
	"""
	Data structure to hold info about local state.

	This structure is valid for one instance of CH-L algorithm.
	"""

	def __init__(self, status, channel, max_channel_count):
		"""
		Initializes new structure for capturing the local state.

		:param int status: Status of the process - should be current amount of money in bank.
		:param channel: Sender which has sent the MARKER message (empty message list is created)
		:param int max_channel_count: Number of channels to record. After all channels are recorded, status
		is marked as complete.
		:return:
		"""
		self._status = status
		self._max_channel_count = max_channel_count

		# each channel gets its own list for capturing messages
		self._pending_channel_messages = dict()

		# once the marker is received from channel, its' messages are moved
		# from _pending_channel_message here
		self._complete_chanel_messages = {}
		if channel is not None:
			self._complete_chanel_messages[channel] = []

		# False by default but in some cases, state may be completed right
		# at the beginning of algorithm
		self._complete = len(self._complete_chanel_messages) == max_channel_count

	def add_message(self, channel, message):
		"""
		Adds message for given channel.

		:param channel: Channel from which the message was received.
		:param Message message: Received message.
		:return:
		"""
		if channel not in self._pending_channel_messages and channel not in self._complete_chanel_messages:
			self._pending_channel_messages[channel] = []

		self._pending_channel_messages[channel].append(message)

	def is_complete(self):
		return self._complete

	def mark_channel_as_complete(self, channel):
		"""
		Moves messages for this channel from pending to complete list.

		:param channel: Channel on which communication is to be recorded no longer.
		:return:
		"""
		logging.debug("Marking channel '%s' as complete." % channel)

		if channel in self._pending_channel_messages:
			self._complete_chanel_messages[channel] = self._pending_channel_messages[channel]
			self._pending_channel_messages.pop(channel)
		else:
			self._complete_chanel_messages[channel] = []

		logging.debug("Remaining pending channels: %s." % str(len(self._pending_channel_messages)))
		logging.debug("Complete channels: %s." % str(len(self._complete_chanel_messages)))
		logging.debug("Max channel count: %s." % self._max_channel_count)
		if len(self._complete_chanel_messages) == self._max_channel_count:
			self._complete = True

	def to_dict(self):
		return dict(
			status=self._status,
			channel_messages=self._pending_channel_messages
		)


class StatesHolder:
	"""
	Class that holds info about all local states of one bank (more than snapshot can be taken at a time).
	"""

	def __init__(self):
		# marker_id -> state
		self._states = {}

	def any_capture_active(self):
		"""
		Checks if any snapshot of global state is being taken at a time.
		:return:
		"""
		return len(self._states) > 0

	def new_global_state(self, marker_id, status, sender, max_channel_count):
		"""
		Adds a new global state structure for given marker_id.

		:param int marker_id: Unique id of marker message.
		:param int status: Node status.
		:param sender: Sender who has sent the MARKER message.
		:param int max_channel_count: Number of channels to record.
		:return:
		"""
		self._states[marker_id] = LocalState(status, sender, max_channel_count)

	def is_state_recorded(self, marker_id):
		"""
		Checks if the state for given marker_id was already recorded.
		:param int marker_id: Id of marker.
		:return:
		"""
		return marker_id in self._states

	def capture_message(self, sender, message):
		"""
		Adds message from given sender to all global states.

		:param sender: Sender of the message.
		:param Message message: Received message.
		:return:
		"""

		for marker_id, status in self._states:
			status.add_message(sender, message)

	def mark_channel_as_complete(self, marker_id, sender):
		"""
		Marks channel in state object given by marker_id as complete and messages will
		no longer be recorded for this channel.

		:param int marker_id: Id of marker message.
		:param sender: Channel from which the marker message was received.
		:return:
		"""
		if marker_id in self._states:
			self._states[marker_id].mark_channel_as_complete(sender)

	def is_status_complete(self, marker_id):
		"""
		Checks if the status with given marker_id si complete.
		:param int marker_id: Id of MARKER message.
		:return: True if the status is complete.
		"""
		if marker_id in self._states:
			c = self._states[marker_id].is_complete()
			logging.debug("Local state: marker_id=%s; complete=%s" % (marker_id, str(c)))
			return c
		else:
			return False

	def get_state(self, marker_id):
		return self._states[marker_id]

	def clear_state(self, marker_id):
		"""
		Clears state for given marker id.

		:param str marker_id:
		:return:
		"""
		if marker_id in self._states:
			self._states.pop(marker_id)


class Bank:
	"""
	Implementation of the bank server.
	"""

	def __init__(self, bank_id, host, ports, debug, db_connector, other_banks, state_collector):
		"""
		Initializes this server with given values.

		:param string bank_id: Id of this bank (unique in distributed system).
		:param string host: IP address of this bank.
		:param list ports: Ports this bank should listen on. If empty, bank will not expect any connections.
		:param list other_banks: List of banks this one should connect to via ZeroMQ. Each entry should be in format <host>:<port>.
		:param string state_collector: Address and port of state collector.
		"""

		self._bank_id = bank_id
		self._host = host
		self._ports = ports
		self._debug = debug
		self._db_connector = db_connector
		self._context = zmq.Context()

		# coefficient used in randrage() to decide
		# whether a message should be generated or not
		self._max_time_between_messages = 5

		# sockets bank is listening on
		self._my_sockets = []

		# socket to given peer can be accessed as _peers["host:port"]
		self._peers = []

		# whether or not can messages be sent/received through main socket
		# when client connects to this socket, simple handshake will happen
		# which will set this condition to True
		self._sockets_ready = dict()

		# Condition for main server loop
		self._should_run = True

		# object for collecting global status
		self._status_holder = StatesHolder()

		# flag indicating whether a CH-L algorithm
		# initiated by this bank is currently running
		self._ch_l_running = False

		self._connect_to_state_collector(state_collector)

		self._init_queues(other_banks)

	def _connect_to_state_collector(self, state_collector):
		"""
		Connects to state collector. Sending connection message is not required
		but it's polite and also serves as a way of logging.

		:param string state_collector: Address of the collector service.
		:return:
		"""
		logging.info("Connecting to state collector on address: %s.", state_collector)
		self._collector_socket = self._context.socket(zmq.PAIR)
		self._collector_socket.connect("tcp://%s" % state_collector)
		self._collector_socket.send_json(Message("Bank '%s' connected." % self._bank_id, -1).to_dict())

	def _peer_handshake(self, other_peer):
		"""
		Do the initial handshake with peer this bank is trying to connect to.
		:param string other_peer: Address bank is trying to connect to
		:return: Socket if handshake is successful.
		"""

		logging.info("Handshake with \"%s\"." % other_peer)
		s = self._context.socket(zmq.PAIR)
		s.connect("tcp://%s" % other_peer)
		s.send_json(Message.connect().to_dict())
		resp = s.recv_json()
		msg = Message.from_dict(resp)
		if msg.is_ok():
			logging.info("Handshake successful.")
			return s
		else:
			logging.warning("Bad handshake response: %s.", str(msg))
			return None

	def _init_queues(self, other_banks):
		"""
		Initializes connections to other banks and starts to listen on given port (if the port is set).

		:param list other_banks:
		"""

		self._poller = zmq.Poller()

		# start listening if ports are set
		for port in self._ports:
			logging.info("Listening on port: %s." % port)
			socket = self._context.socket(zmq.PAIR)
			socket.bind("tcp://*:%s" % port)
			self._my_sockets.append(socket)
			self._poller.register(socket, zmq.POLLIN)
			self._sockets_ready[socket] = False

		# connect to neighbours
		for other_bank in other_banks:
			logging.info("Connecting to: %s.", other_bank)
			s = self._peer_handshake(other_bank)
			if s is not None:
				self._peers.append(s)
				self._poller.register(s, zmq.POLLIN)

	def _get_available_peers(self, include_my_if_not_ready=False):
		"""
		Returns all sockets - peers + the ones bank is listening on.

		:param bool include_my_if_not_ready: If the flag is set, my sockets will be included even if it is not ready yet.
		"""
		peers = [] + self._peers

		for my_socket in self._my_sockets:
			if include_my_if_not_ready or self._sockets_ready[my_socket]:
				peers.append(my_socket)

		return peers

	def _check_amount(self, amount):
		"""
		Checks whether given amount of money can be withdrawn.

		:param int amount: Amount of money to withdraw.
		:return: True if the amount is ok.
		"""
		curr_amount = self._db_connector.get_amount()
		return curr_amount >= amount

	def start_server(self):
		"""
		Starts banking server - message sending and receiving.
		"""

		logging.info("Starting receive/send loop.")
		while self._should_run:
			self._check_marker_file()
			self._recv_messages()
			self._generate_message()

		logging.info("Loop finished gracefully.")

	def _generate_message(self):
		"""
		Generate and send one message to direct neighbor. Always generates DEBIT
		message if there's not enough money in the bank.
		"""

		# choose target to send message to
		# either main socket this bank is listening on
		# or one of the peers this bank is connected to
		peers = self._get_available_peers()
		if len(peers) == 0:
			return

		# random time between messages
		if randrange(self._max_time_between_messages) != 0:
			return

		logging.debug("Generating message.")

		amount = 10000 + randrange(40001)
		rand = randrange(len(peers))
		target = peers[rand]

		rand = randrange(2)
		if rand == 0 and self._check_amount(amount):
			self._send_credit(amount, target)
		else:
			self._send_debit(amount, target)

	def _check_connection_message(self, message, socket):
		"""
		Checks for incoming CONNECT message on main socket. If it is, OK message is immediately sent back.
		Otherwise REFUSED is sent back

		:param Message message: Received message.
		:return:
		"""

		if message.is_connect():
			logging.info("Connection message received on main socket. Main socket ready.")
			socket.send_json(Message.ok().to_dict())
			self._sockets_ready[socket] = True
		else:
			logging.warning("Wrong message received on main socket.")
			socket.send_json(Message.refused().to_dict())

	def _recv_messages(self):
		"""
		Poll for messages from ZeroMQ. Timeout is between 10 and 100 ms.
		"""
		t = 10
		socks = dict(self._poller.poll(timeout=t))

		if len(socks) > 0:
			logging.debug("%d sockets polled." % len(socks))

			# find which socket has received the message
			for socket in self._get_available_peers(True):
				if socket in socks and socks[socket] == zmq.POLLIN:
					msg = Message.from_dict(socket.recv_json())
					logging.info("Message received: %s." % msg)

					if self._is_my_socket_that_is_not_ready(socket):
						# message on main socket that is not ready yet received
						# check if it's connection or not
						self._check_connection_message(msg, socket)

					else:
						# receive normal message from socket
						self._process_message(msg, socket)

	def _process_message(self, message, sender):
		"""
		Process one message from queue. If it's DEBIT and there's not enough money
		REFUSE will bse sent back to SENDER.
		
		:param Message message: Message received from queue.
		:param Socket sender: Sender of the received message.
		"""

		if message.is_credit():
			self._credit(message.amount)
		elif message.is_debit():
			if self._check_amount(message.amount):
				self._debit(message.amount, sender)
			else:
				self._send_refuse(sender)
		elif message.is_marker():
			logging.info("Processing marker message: %s." % str(message))
			self._handle_global_state(message, sender)
		else:
			logging.info("Refused from %s." % str(sender))

	def _handle_global_state(self, message, sender):
		"""
		Handles incoming MARKER message. Chandy-Lamport
		algorithm is implemented here.

		:param message: Received marker message. Message.amount is used as marker ID.
		:param sender: Sender of the message.
		:return:
		"""
		marker_id = message.amount

		if not self._status_holder.is_state_recorded(marker_id):
			logging.info("Status for marker '%s' not recorded yet." % marker_id)
			# 1. mark my current state and send markers to other peers (state = amount of money in the bank)
			# 2. mark the state of sender as empty list
			self._mark_my_status(marker_id, sender)
			self._send_markers(marker_id)

			# 3. all incoming messages will be recorded
		else:
			logging.info("Status for marker '%s' already marked. Marking send %s as complete." % (marker_id, sender))
			# token with given marker_id was already received -> my state was already marked down
			# stop recording messages from sender
			self._status_holder.mark_channel_as_complete(marker_id, sender)

		# messages from all channels recorded -> algorithm ends
		if self._status_holder.is_status_complete(marker_id):
			logging.info("Algorithm for marker %s is complete." % marker_id)
			self._report_status(marker_id)
			self._ch_l_cleanup(marker_id)

	def _credit(self, amount):
		"""
		Credits given amount to this bank.
		"""
		self._db_connector.credit_money(amount)

	def _debit(self, amount, target):
		"""
		Sends given amount of money back to target or sends REFUSE if there's not enough money in the account.
		"""
		self._send_credit(amount, target)

	def _send_credit(self, amount, target):
		"""
		Deducts given amount from this bank's account and sends CREDIT message to target.
		
		:param Socket target: Socket to send message to.
		"""
		if self._check_amount(amount):
			self._db_connector.debit_money(amount)
			target.send_json(Message.credit(amount).to_dict())
		else:
			logging.info("Not enough funds in bank, cannot credit %s." % str(amount))
			self._send_refuse(target)

	def _send_debit(self, amount, target):
		"""
		Sends DEBIT message for given amount to given target.
		"""
		target.send_json(Message.debit(amount).to_dict())

	def _send_refuse(self, target):
		"""
		Sends REFUSED message to target.
		"""
		target.send_json(Message.refused().to_dict())

	def _send_markers(self, marker_id):
		"""
		Sends MARKER message with given id to all peers.
		:param int marker_id: Id of marker message.
		:return:
		"""
		peers = self._get_available_peers()
		for peer in peers:
			msg = Message.marker(marker_id)
			logging.info("Sending marker message: %s." % msg)
			peer.send_json(msg.to_dict())

	def _mark_my_status(self, marker_id, sender):
		"""
		Creates new GlobalState object for marker_id
		:param int marker_id: Id of marker message.
		:param sender: Peer from which the marker message was received.
		:return:
		"""
		self._status_holder.new_global_state(marker_id, self._db_connector.get_amount(),
											sender, len(self._get_available_peers()))

	def _report_status(self, marker_id):
		"""
		Reports my status to the global state collector service.


		:param int marker_id: Id of snapshot to report.
		:return:
		"""
		local_state = self._status_holder.get_state(marker_id).to_dict()
		local_state["bank_id"] = self._bank_id
		local_state["marker_id"] = marker_id
		logging.info("Reporting local state (%s) for marker %s." % (str(local_state), marker_id))
		self._collector_socket.send_json(local_state)

	def _is_my_socket_that_is_not_ready(self, socket):
		"""
		Checks if the given socket is 'my socket' (the one the bank is listening on) that is not ready yet.

		:param socket:
		:return:
		"""
		return socket in self._my_sockets and not self._sockets_ready[socket]

	def _check_marker_file(self):
		"""
		Checks if the MARKER file is present and if it is, the global state algorithm is started.

		:return:
		"""

		marker_filename = "MARKER"

		if os.path.isfile(marker_filename) and not self._ch_l_running:
			logging.info("'%s' file detected, starting CH-L with id %s." % (marker_filename, self._bank_id))
			os.remove(marker_filename)
			self._ch_l_running = True
			self._handle_global_state(Message.marker(self._bank_id), None)

	def _ch_l_cleanup(self, marker_id):
		"""
		Cleanup after chandy lamport algorithm. If the marker id is same
		as id of this bank, ch_l_running flag is set to False.

		:param str marker_id: Marker ID.
		:return:
		"""
		self._status_holder.clear_state(marker_id)
		if marker_id == self._bank_id:
			self._ch_l_running = False


def load_configuration(bank_id):
	"""
	Loads configuration of bank addresses and state collector address.

	:param string bank_id: Id of bank to load configuration for.

	:return: Dict with configuration.
	"""
	bank_addr_file = "bank-addrs.csv"
	state_collect_file = "state-collector.csv"

	logging.info("Loading configuration")

	res = None
	if not os.path.isfile(bank_addr_file):
		logging.error("Configuration file '%s' with bank addresses not found." % bank_addr_file)
		return res

	if not os.path.isfile(state_collect_file):
		logging.error("Configuration file '%s' with state collector not found." % state_collect_file)
		return res

	res = dict()
	bank_conf = dict()

	state_collector_conf = dict()
	with open(state_collect_file, "r") as f:
		for line in f.readlines():
			items = line.rstrip().split(',')
			state_collector_conf[items[0]] = items[1]

	with open(bank_addr_file, "r") as f:
		for line in f.readlines():
			items = line.rstrip().split(',')
			if items[0] not in bank_conf:
				bank_conf[items[0]] = dict(
					ports=[],
					other_banks=[]
				)

				if len(items) > 1:
					bank_conf[items[0]]["ports"] = items[1:]
			else:
				bank_conf[items[0]]["other_banks"] = items[1:]

	res["bank_conf"] = bank_conf[bank_id] if bank_id in bank_conf else dict(ports=[], other_banks=[])
	res["state_collector"] = state_collector_conf[bank_id]
	logging.info("Configuration for bank %s: %s.", bank_id, str(res))

	return res


def load_bank_id():
	"""
	Bank id is expected to be the first console argument.

	:return: Id of this bank or None if none was passed.
	"""
	if (len(sys.argv)) != 2:
		logging.error("Wrong number of arguments, expected just one, got: %s.", str(sys.argv))
		return None
	else:
		return sys.argv[1]


def configure_logging(include_console=False):
	if os.path.isfile("log.txt"):
		os.remove("log.txt")

	logging.basicConfig(filename='log.txt',
						filemode='a',
						format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
						datefmt='%H:%M:%S',
						level=logging.DEBUG)

	if include_console:
		console = logging.StreamHandler()
		console.setLevel(logging.DEBUG)
		logging.getLogger('').addHandler(console)


def main():
	"""
	Main method of the script, starts the server.
	"""
	configure_logging(True)

	bank_id = load_bank_id()
	if bank_id is None:
		return

	configuration = load_configuration(bank_id)
	if configuration is None:
		exit(1)

	logging.info("Bank '%s' starting" % bank_id)
	db_connector = DbConnector()
	amount = db_connector.get_amount()
	if amount is not None:
		logging.info("Original balance: %s" % str(amount))
	else:
		logging.warning("No original amount.")

	bank = Bank(bank_id,
				host='0.0.0.0',
				ports=configuration["bank_conf"]["ports"],
				debug=True,
				db_connector=db_connector,
				other_banks=configuration["bank_conf"]["other_banks"],
				state_collector=configuration["state_collector"])
	bank.start_server()
	db_connector.close_connection()


# Script body
main()
