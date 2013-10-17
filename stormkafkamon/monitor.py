#!/usr/bin/env python

import argparse
import sys
from prettytable import PrettyTable
import requests
import simplejson as json
import socket
import time

from zkclient import ZkClient, ZkError
from processor import process, ProcessorError

def sizeof_fmt(num):
    for x in [' bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def null_fmt(num):
    return num

def display(summary, friendly=False):
    if friendly:
        fmt = sizeof_fmt
    else:
        fmt = null_fmt

    table = PrettyTable(['Broker', 'Topic', 'Partition', 'Earliest', 'Latest',
                        'Depth', 'Spout', 'Current', 'Delta'])
    table.align['broker'] = 'l'

    for p in summary.partitions:
        table.add_row([p.broker, p.topic, p.partition, p.earliest, p.latest,
                      fmt(p.depth), p.spout, p.current, fmt(p.delta)])
    print table.get_string(sortby='Broker')
    print
    print 'Number of brokers:       %d' % summary.num_brokers
    print 'Number of partitions:    %d' % summary.num_partitions
    print 'Total broker depth:      %s' % fmt(summary.total_depth)
    print 'Total delta:             %s' % fmt(summary.total_delta)

def post_json(endpoint, zk_data):
    fields = ("broker", "topic", "partition", "earliest", "latest", "depth", "spout", "current", "delta")
    json_data = {}
    for p in zk_data.partitions:
        guts = {}
        for name in fields:
            guts[name] = getattr(p, name)
        json_data["%s-%s" % (p.broker, p.partition)] = guts

    total_fields = ('depth', 'delta')

    total = {}
    for fieldname in total_fields:
        total[fieldname] = 0
        for p in zk_data.partitions:
            total[fieldname] += getattr(p, fieldname)
    
    total['partitions'] = zk_data.num_partitions
    total['brokers'] = zk_data.num_brokers
    json_data['total'] = total    

    requests.post(endpoint, data=json.dumps(json_data))

def send_to_graphite(graphiteserver, graphiteport, zk_data):
    timestamp = int(time.time())
    events = []
    fields = ("earliest", "latest", "depth", "current", "delta")
    json_data = {}
    for p in zk_data.partitions:
        for name in fields:
            events.append('%s.%s.%s.%s.%s.%s %s %d' % ('stormkafka', p.broker, p.topic, p.partition, p.spout, name, getattr(p, name), timestamp))

    message = '\n'.join(events) + '\n'
    
    # print 'sending message: %s\n' % message
    sock = socket.socket()
    sock.connect((graphiteserver, graphiteport))
    sock.sendall(message)
    sock.close()

######################################################################

def true_or_false_option(option):
    if option == None:
        return False
    else:
        return True

def read_args():
    parser = argparse.ArgumentParser(
        description='Show complete state of Storm-Kafka consumers')
    parser.add_argument('--zserver', default='localhost',
        help='Zookeeper host (default: localhost)')
    parser.add_argument('--zport', type=int, default=2181,
        help='Zookeeper port (default: 2181)')
    parser.add_argument('--topology', type=str, required=True,
        help='Storm Topology')
    parser.add_argument('--spoutroot', type=str, required=True,
        help='Root path for Kafka Spout data in Zookeeper')
    parser.add_argument('--friendly', action='store_const', const=True,
                    help='Show friendlier data')
    parser.add_argument('--graphitehost', type=str,
                    help='Graphite host')
    parser.add_argument('--graphiteport', type=int, default=2003,
                    help='Graphite port (default: 2003)')
    parser.add_argument('--postjson', type=str,
                    help='endpoint to post json data to')
    return parser.parse_args()

def main():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)

    try:
        zk_data = process(zc.spouts(options.spoutroot, options.topology))
    except ZkError, e:
        print 'Failed to access Zookeeper: %s' % str(e)
        return 1
    except ProcessorError, e:
        print 'Failed to process: %s' % str(e)
        return 1
    else:
        if options.postjson:
            post_json(options.postjson, zk_data)
        elif options.graphitehost and options.graphiteport:
            send_to_graphite(options.graphitehost, options.graphiteport, zk_data)
        else:
            display(zk_data, true_or_false_option(options.friendly))
    return 0

if __name__ == '__main__':
    sys.exit(main())
