# Takes lists of objects returned by the zkclient module, and
# consolidates the information for display.

import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger('kafka.codec').addHandler(NullHandler())

import struct
import socket
from collections import namedtuple
from kafka.client import KafkaClient
from kafka.common import KafkaUnavailableError
from kafka.common import OffsetRequest

class ProcessorError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

PartitionState = namedtuple('PartitionState',
    [
        'broker',           # Broker host
        'topic',            # Topic on broker
        'partition',        # The partition
        'earliest',         # Earliest offset within partition on broker
        'latest',           # Current offset within partition on broker
        'depth',            # Depth of partition on broker.
        'spout',            # The Spout consuming this partition
        'current',          # Current offset for Spout
        'delta'             # Difference between latest and current
    ])
PartitionsSummary = namedtuple('PartitionsSummary',
    [
        'total_depth',      # Total queue depth.
        'total_delta',      # Total delta across all spout tasks.
        'num_partitions',   # Number of partitions.
        'num_brokers',      # Number of Kafka Brokers.
        'partitions'        # Tuple of PartitionStates
    ])


def unreachable_partition_state(spout, partition):
    return PartitionState._make([
        partition['broker']['host'],
        partition['topic'],
        partition['partition'],
        -1,
        -1,
        -1,
        spout.id,
        -1,
        -1])


def process(spouts):
    '''
    Returns a named tuple of type PartitionsSummary.
    '''
    results = []
    total_depth = 0
    total_delta = 0
    brokers = []
    for s in spouts:
        for p in s.partitions:
            try:
                k = KafkaClient(p['broker']['host'] + ':' + str(p['broker']['port']))
            except KafkaUnavailableError:
                results.append(unreachable_partition_state(s, p))
                continue
            except socket.gaierror, e:
                raise ProcessorError('Failed to contact Kafka broker %s (%s)' %
                                     (p['broker']['host'], str(e)))
            earliest_off = OffsetRequest(p['topic'], p['partition'], -2, 1)
            latest_off = OffsetRequest(p['topic'], p['partition'], -1, 1)

            earliest_resps = k.send_offset_request([earliest_off])
            latest_resps = k.send_offset_request([latest_off])

            earliest = earliest_resps[0].offsets[0]
            latest = latest_resps[0].offsets[0]
            current = p['offset']

            brokers.append(p['broker']['host'])
            total_depth = total_depth + (latest - earliest)
            total_delta = total_delta + (latest - current)

            results.append(PartitionState._make([
                p['broker']['host'],
                p['topic'],
                p['partition'],
                earliest,
                latest,
                latest - earliest,
                s.id,
                current,
                latest - current]))

    return PartitionsSummary(total_depth=total_depth,
                             total_delta=total_delta,
                             num_partitions=len(results),
                             num_brokers=len(set(brokers)),
                             partitions=tuple(sorted(results, key=lambda x: x.partition)))
