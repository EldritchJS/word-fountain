import argparse
import gzip
import os
import random
import time
import stomp

parser = argparse.ArgumentParser(description='AMQ words sender')
parser.add_argument('--servers', help='The AMQP server', default='broker-amq-stomp')
parser.add_argument('--port', help = 'The AMQP port', default='61613')
parser.add_argument('--queue', help='Queue to publish to', default='salesq')
parser.add_argument('--rate', type=int, help='Records per second', default=1)
parser.add_argument('--count', type=int, help='Total records to publish', default=-1)
args = parser.parse_args()

server = os.getenv('SERVERS', args.servers)
port = int(os.getenv('PORT', args.port))
queue = os.getenv('QUEUE', args.queue)
rate = int(os.getenv('RATE', args.rate))
count = int(os.getenv('COUNT', args.count))

#print('servers={}, queue={}, rate={}, count={}'.format(servers, queue, rate, count))

#producer = KafkaProducer(bootstrap_servers=servers)
dest = '/queue/' + queue
c = stomp.Connection([(server, port)])
c.start()
c.connect('daikon', 'daikon', wait=True)

with gzip.open('words.gz', 'r') as f:
    words = f.readlines()
    # subset words to produce more duplicates
    words = [random.choice(words).strip() for i in range(max(42, rate ** 2))]

while count:
#    producer.send(topic, random.choice(words))
    c.send(body=random.choice(words), destination = dest)
    count -= 1
    time.sleep(1.0 / rate)

