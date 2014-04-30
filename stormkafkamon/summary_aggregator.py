class MovingAverage(object):
    def __init__(self, interval):
        self.values = []
        self.times = []
        self.interval = float(interval)

    def add_value(self, value, time):
        self.values.append(value)
        self.times.append(time)
        self.update(time)

    def update(self, now):
        for i, time in enumerate(self.times):
            delta = now - time
            microseconds = delta.seconds * 1000000 + delta.microseconds
            seconds = microseconds / 1000000.0

            if seconds < self.interval:
                break

        for j in range(i):
            self.values.pop(0)
            self.times.pop(0)

    def current_value(self):
        num_times = len(self.times)
        if num_times < 2:
            return -1
        else:
            delta = self.times[num_times - 1] - self.times[0]
            microseconds = delta.seconds * 1000000 + delta.microseconds
            seconds = microseconds / 1000000.0

            return sum(self.values) / seconds


class PartitionDelta(object):
    def __init__(self):
        self.broker = None
        self.current = None
        self.earliest = None
        self.latest = None
        self.depth = None
        self.delta = None

        self.added = 0
        self.removed = 0
        self.net = 0

    def update(self, partition_data):
        if self.latest is not None:
            self.added = partition_data.latest - self.latest
            self.removed = partition_data.current - self.current
            self.net = partition_data.delta - self.delta

        self.broker = partition_data.broker
        self.latest = partition_data.latest
        self.earliest = partition_data.earliest
        self.depth = partition_data.depth
        self.current = partition_data.current
        self.delta = partition_data.delta

    def get_added(self):
        return self.added

    def get_removed(self):
        return self.removed

    def get_net(self):
        return self.net

    def get_earliest(self):
        return self.earliest

    def get_current(self):
        return self.current

    def get_latest(self):
        return self.latest

    def get_depth(self):
        return self.depth

    def get_delta(self):
        return self.delta


class SummaryAggregator(object):
    MOVING_AVG_INTERVALS = [30, 60, 300, 600]

    @staticmethod
    def get_moving_average_counts(self):
        return len(SummaryAggregator.MOVING_AVG_INTERVALS)

    @staticmethod
    def get_moving_average_interval(self, index):
        return SummaryAggregator.MOVING_AVG_INTERVALS[index]

    @staticmethod
    def seconds_delta(now, prev_time):
        if now is None or prev_time is None or now == prev_time:
            return 0

        delta = now - prev_time
        microseconds = delta.seconds * 1000000 + delta.microseconds
        return microseconds / 1000000.0

    def __init__(self, topology, zookeeper):
        self.partitions = {}
        self.total_added = 0
        self.total_removed = 0
        self.summaries_added = 0
        self.added_averages = {}
        self.removed_averages = {}
        self.prev_summary = None
        self.prev_time = None
        self.seconds_between_updates = None
        self.start_time = None
        self.seconds_running = 0
        self.added = 0
        self.removed = 0

        self.brokers = None
        self.topic = None
        self.topology = topology
        self.zookeeper = zookeeper

        for interval in SummaryAggregator.MOVING_AVG_INTERVALS:
            self.removed_averages[interval] = MovingAverage(interval)
            self.added_averages[interval] = MovingAverage(interval)

    def add_summary(self, summary, time):
        self.added = 0
        self.removed = 0
        for partition in summary.partitions:
            partition_number = partition.partition

            if partition_number not in self.partitions:
                self.partitions[partition_number] = PartitionDelta()

            self.partitions[partition_number].update(partition)

            self.added += self.partitions[partition_number].get_added()
            self.removed += self.partitions[partition_number].get_removed()

        for interval in SummaryAggregator.MOVING_AVG_INTERVALS:
            self.removed_averages[interval].add_value(self.removed, time)
            self.added_averages[interval].add_value(self.added, time)

        if self.prev_summary is not None:
            self.total_added += self.added
            self.total_removed += self.removed
            self.summaries_added += 1
            self.seconds_between_updates = SummaryAggregator.seconds_delta(time, self.prev_time)

        self.prev_summary = summary
        self.prev_time = time

        if self.start_time is None:
            self.start_time = time
        else:
            self.seconds_running = SummaryAggregator.seconds_delta(time, self.start_time)

    def get_added_moving_average(self, interval):
        return self.added_averages[interval]

    def get_removed_moving_average(self, interval):
        return self.removed_averages[interval]

    def get_average_added(self):
        return self.total_added / self.seconds_running

    def get_removed_added(self):
        return self.total_removed / self.seconds_running

    def get_header_lines(self):
        lines = list()

        lines.append("Zookeeper: %s Topology: %s" % (self.zookeeper, self.topology))
        lines.append("")

        lines.append("Total Depth: %18d     Total Delta: %18d" % (self.prev_summary.total_depth, self.prev_summary.total_delta))
        lines.append("")

        display_delta = self.seconds_between_updates if self.seconds_between_updates != None else 0.0
        lines.append("Delta Time %10.02fs Added %12d Removed %12d Net %12d" % (display_delta, self.added, self.removed, self.added-self.removed))

        if self.seconds_between_updates is  None or self.seconds_between_updates == 0:
            lines.append("%22s Add/s %12d Rem/s   %12d Net %12d" % ('', 0, 0, 0))
        else:
            lines.append("%22s Add/s %12d Rem/s   %12d Net %12d" % ('', self.added / self.seconds_between_updates, self.removed / self.seconds_between_updates, (self.added-self.removed)/self.seconds_between_updates))

        lines.append("")
        lines.append("            last 30 sec | last minute | last 5 minutes | last 10 minutes")
        lines.append("  Added/sec % 12d|% 13d|% 16d|% 16d" % (self.added_averages[30].current_value(),
                                                              self.added_averages[60].current_value(),
                                                              self.added_averages[300].current_value(),
                                                              self.added_averages[600].current_value()))
        lines.append("Removed/sec % 12d|% 13d|% 16d|% 16d" % (self.removed_averages[30].current_value(),
                                                              self.removed_averages[60].current_value(),
                                                              self.removed_averages[300].current_value(),
                                                              self.removed_averages[600].current_value()))
        lines.append("    Net/sec % 12d|% 13d|% 16d|% 16d" % (self.added_averages[30].current_value() - self.removed_averages[30].current_value(),
                                                              self.added_averages[60].current_value() - self.removed_averages[60].current_value(),
                                                              self.added_averages[300].current_value() - self.removed_averages[300].current_value(),
                                                              self.added_averages[600].current_value() - self.removed_averages[600].current_value()))

        return lines

    def get_partition_data_lines(self):
        lines = list()
        lines.append("  #  |   Earliest   |    Current   |    Latest    |     Depth    |     Delta    | Delta Delta/s|")

        for i in range(len(self.partitions)):
            partition = self.partitions[i]
            net_per_second = partition.get_net() / self.seconds_between_updates if self.seconds_between_updates is not None else 0

            lines.append(" % 3d |% 13d |% 13d |% 13d |% 13d |% 13d |% 13d |" % (i,
                                                                         partition.get_earliest(),
                                                                         partition.get_current(),
                                                                         partition.get_latest(),
                                                                         partition.get_depth(),
                                                                         partition.get_delta(),
                                                                         net_per_second ))

        return lines
