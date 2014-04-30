#!/usr/bin/env python

import argparse
import sys
import curses
import time
import datetime

from prettytable import PrettyTable

from zkclient import ZkClient, ZkError
from processor import process, ProcessorError
from summary_aggregator import SummaryAggregator


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


def curses_display(window, lines, start_x, start_y):
    height, width = window.getmaxyx()

    for i, line in enumerate(lines):
        y = start_y + i

        if y < height:
            window.move(y, start_x)
            window.addstr(line[:(width - 1)])
            window.clrtoeol()


######################################################################

def true_or_false_option(option):
    if option == None:
        return False
    else:
        return True


def read_args():
    parser = argparse.ArgumentParser(description='Show complete state of Storm-Kafka consumers')
    parser.add_argument('--zserver', default='localhost', help='Zookeeper host (default: localhost)')
    parser.add_argument('--zport', type=int, default=2181, help='Zookeeper port (default: 2181)')
    parser.add_argument('--topology', type=str, required=True, help='Storm Topology')
    parser.add_argument('--spoutroot', type=str, required=True, help='Root path for Kafka Spout data in Zookeeper')
    parser.add_argument('--friendly', action='store_const', const=True, help='Show friendlier data')
    parser.add_argument('--update_interval', type=float, default=3.0, help='Interval between updates in seconds')
    return parser.parse_args()


def main():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)
    zc.start()

    try:
        try:
            display(process(zc.spouts(options.spoutroot, options.topology)),
                    true_or_false_option(options.friendly))
        except ZkError, e:
            print 'Failed to access Zookeeper: %s' % str(e)
            return 1
        except ProcessorError, e:
            print 'Failed to process: %s' % str(e)
            return 1
    finally:
        zc.stop()

    return 0


def get_delta(prev_time):
    delta = 0.0
    now = datetime.datetime.utcnow()

    if prev_time is not None:
        date_diff = (now - prev_time)
        delta = ((date_diff.seconds * 1000000) + date_diff.microseconds) / 1000000.0

    return delta


def curses_main(window, args):
    zc = args[0]
    options = args[1]
    aggregator = SummaryAggregator(options.topology, options.zserver + ':' + str(options.zport))

    while True:
        last_update = datetime.datetime.utcnow()
        spouts = zc.spouts(options.spoutroot, options.topology)
        summary = process(spouts)
        aggregator.add_summary(summary, datetime.datetime.utcnow())

        header_lines = aggregator.get_header_lines()
        partition_data_lines = aggregator.get_partition_data_lines()

        if window is not None:
            window.erase()
            curses_display(window, header_lines, 0, 0)
            curses_display(window, partition_data_lines, 0, len(header_lines) + 1)
            window.refresh()
        else:
            print '\r\n'.join(header_lines)
            print
            print '\r\n'.join(partition_data_lines)

        delta = get_delta(last_update)
        sleep_time = options.update_interval - delta

        if sleep_time > 0.0:
            time.sleep(sleep_time)


def top():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)
    zc.start()

    try:
        curses.wrapper(curses_main, [zc, options])
        #curses_main(None, [zc, options])

    except KeyboardInterrupt:

        pass

    finally:
        zc.stop()

if __name__ == '__main__':
    sys.exit(main())
