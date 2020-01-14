import zmq
import logging
import os

def print_state_message(message):
	logging.info(message)


def start_listening(configuration):
	"""
	Starts listening on ports given by configuration and starts to
	poll bound sockets for incoming messages.

	:param dict configuration: "port" should contain list with ports to bind to.
	:return:
	"""
	# initialize sockets and poller
	context = zmq.Context()
	sockets = []
	poller = zmq.Poller()
	for port in configuration["ports"]:
		logging.info("Listening on port: %s" % port.rstrip())
		s = context.socket(zmq.PAIR)
		s.bind("tcp://*:%s" % port.rstrip())
		poller.register(s, zmq.POLLIN)
		sockets.append(s)

	should_run = True

	# run in loop
	while should_run:
		messages = dict(poller.poll(timeout=10))
		if len(messages) > 0:
			for socket in sockets:
				if socket in messages and messages[socket] == zmq.POLLIN:
					message = socket.recv_json()
					print_state_message(message)


def load_configuration():
	"""
	Loads port this collector should listen on from configuration file.
	Each line is expected to contain exactly one port number.

	:return:
	"""
	file_name = "collector.txt"
	if not os.path.isfile(file_name):
		logging.error("Configuration file %s does not exist." % file_name)
		return None

	with open(file_name, "r") as f:
		lines = f.readlines()

	return dict(
		ports=lines
	)


def configure_logging(include_console=False):
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
	configure_logging(True)

	configuration = load_configuration()
	if configuration is None:
		return

	logging.info("Starting global state collector.")
	start_listening(configuration)


# script body
main()
