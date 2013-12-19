import warnings
from circus.plugins.statsd import BaseObserver
from circus.util import human2bytes
from collections import defaultdict
import six
import signal
from datetime import datetime, timedelta
from circus import logger

VALID_ACTIONS = ['restart', 'reload']
VALID_ACTIONS += filter(lambda s: s.startswith('SIG'), dir(signal))
VALID_ACTIONS = map(lambda s: s.lower(), VALID_ACTIONS)


class ResourceWatcher(BaseObserver):

    def __init__(self, *args, **config):
        super(ResourceWatcher, self).__init__(*args, **config)

        # Watcher/service parameter
        self.watcher = config.get("watcher", None)
        self.service = config.get("service", None)
        if self.service is not None:
            warnings.warn("ResourceWatcher.service is deprecated "
                          "please use ResourceWatcher.watcher instead.",
                          category=DeprecationWarning)
            if self.watcher is None:
                self.watcher = self.service
        if self.watcher is None:
            self.statsd.stop()
            raise NotImplementedError('watcher is mandatory for now.')

        # Memory/CPU parameters
        self.max_cpu = float(config.get("max_cpu", 90))     # in %
        self.max_mem = config.get("max_mem")
        if self.max_mem is None:
            self.max_mem = 90.
        else:
            try:
                self.max_mem = float(self.max_mem)          # float -> %
            except ValueError:
                self.max_mem = human2bytes(self.max_mem)    # int -> absolute
        self.min_cpu = config.get("min_cpu")
        if self.min_cpu is not None:
            self.min_cpu = float(self.min_cpu)              # in %
        self.min_mem = config.get("min_mem")
        if self.min_mem is not None:
            try:
                self.max_mem = float(self.min_mem)          # float -> %
            except ValueError:
                self.max_mem = human2bytes(self.min_mem)    # int -> absolute

        # Other parameters
        self.health_threshold = float(config.get("health_threshold",
                                      75))                  # in %
        self.max_count = int(config.get("max_count", 3))
        self.action = config.get("action", 'restart').lower()
        if self.action not in VALID_ACTIONS:
            raise ValueError(self.action)
        self.per_process = bool(config.get("per_process", False))
        if self.per_process and self.action in ['reload', 'restart']:
            raise NotImplementedError("You can't restart or reload a process.")
        if not self.per_process and self.action not in ['reload', 'restart']:
            raise NotImplementedError("You can't send a signal to a watcher.")

        self._monitors = {}

    def look_after(self):
        stats = self.collect_stats()
        if stats is None:
            return

        if self.per_process:
            self.manage_process_monitors(stats.keys())
            for pid, pstats in stats.iteritems():
                self._monitors[pid].process_stats(pstats)
        else:
            if self.watcher not in self._monitors:
                self._monitors[self.watcher] = WatcherMonitor(self)
            self._monitors[self.watcher].process_stats(stats[self.watcher])

    def manage_process_monitors(self, pids):
        # Make sure there is a process monitor for each process, not more
        for pid in pids:
            if pid not in self._monitors:
                self._monitors[pid] = ProcessMonitor(pid, self)
        for pid in self._monitors.keys():
            if pid not in pids:
                del self._monitors[pid]

    def collect_stats(self):
        stats = self.call("stats", name=self.watcher)
        if stats["status"] == "error":
            self.statsd.increment("_resource_watcher.%s.error" % self.watcher)
            logger.error("Can't get stats of %s" % self.watcher)
            return
        stats = dict((k, v) for k, v in six.iteritems(stats['info']) if
                     type(v) == dict)

        # Convert absolute memory to bytes
        for item in stats:
            if stats[item]['mem_info1'] == 'N/A':
                stats[item]['mem_abs'] = 'N/A'
            else:
                stats[item]['mem_abs'] = human2bytes(stats[item]['mem_info1'])

        # Compute watcher stats if not in per_process mode
        if not self.per_process:
            stats[self.watcher] = defaultdict(lambda: 'N/A')
            for item in filter(lambda x: x != self.watcher, stats):
                for k in ['cpu', 'mem', 'mem_abs']:
                    if stats[item][k] != 'N/A':
                        if stats[self.watcher][k] == 'N/A':
                            stats[self.watcher][k] = stats[item][k]
                        else:
                            stats[self.watcher][k] += stats[item][k]
        return stats


class Monitor(object):

    def __init__(self, resourceWatcher, *args, **config):
        super(Monitor, self).__init__(*args, **config)
        self.rw = resourceWatcher
        self._counters = defaultdict(int)
        self._lastAction = datetime.min

    def process_stats(self, stats):
        # Update CPU counters
        self.update_counter('max_cpu', stats['cpu'])
        self.update_counter('min_cpu', stats['cpu'])

        # Update memory counters
        if type(self.rw.max_mem) == int:                    # see max_mem doc
            self.update_counter('max_mem_abs', stats['mem_abs'])
        else:
            self.update_counter('max_mem', stats['mem'])
        if type(self.rw.min_mem) == int:                    # see min_mem doc
            self.update_counter('min_mem_abs', stats['mem_abs'])
        else:
            self.update_counter('min_mem', stats['mem'])

        # Update health counters
        self.update_health_counter(stats['cpu'], stats['mem'])

        # Process counters
        self.process_counters()

    def update_counter(self, metric, value):
        threshold = getattr(self.rw, metric.strip('_abs'))   # see max_mem doc
        if value == 'N/A' or threshold is None:
            return
        limit_type = metric[:3]
        if (limit_type == 'max' and value > threshold or
                limit_type == 'min' and value < threshold):
            statdsMsg0 = "under" if limit_type == 'min' else "over"
            metricType = metric.split('_')[1]
            statdsMsg1 = "memory" if metricType == 'mem' else metricType
            self.rw.statsd.increment("_resource_watcher.%s.%s_%s" %
                                     (self.rw.watcher,
                                      statdsMsg0,
                                      statdsMsg1))
            self._counters[metric] += 1
        else:
            self._counters[metric] = 0

    def update_health_counter(self, cpu, mem):
        if self.rw.health_threshold:
            health = 0
            if cpu != 'N/A':
                health += cpu
            if mem != 'N/A':
                health += mem
            if health/2.0 > self.rw.health_threshold:
                self.rw.statsd.increment("_resource_watcher.%s.over_health" %
                                         self.rw.watcher)
                self._counters['health'] += 1
            else:
                self._counters['health'] = 0

    def process_counters(self):
        if max(self._counters.values()) > self.rw.max_count:
            fiveSecAgo = datetime.now() - timedelta(seconds=5)
            if self._lastAction is not None and self._lastAction < fiveSecAgo:
                self.perform_action()
                self._lastAction = datetime.now()

    def perform_action():
        raise NotImplementedError()


class WatcherMonitor(Monitor):
    def perform_action(self):
        if self.rw.action in ['restart', 'reload']:
            logger.info("Sending %s to watcher %s",
                        self.rw.action, self.rw.watcher)
            self.rw.statsd.increment("_resource_watcher.%s.%sing" %
                                     (self.rw.watcher, self.rw.action))
            self.rw.cast(self.rw.action, name=self.rw.watcher)
        else:
            # Signale effective on processes, not on the watcher
            raise NotImplementedError("This case should not happend. \
                See config.")

        self._counters = defaultdict(int)


class ProcessMonitor(Monitor):

    def __init__(self, pid, *args, **config):
        super(ProcessMonitor, self).__init__(*args, **config)
        self.pid = int(pid)

    def perform_action(self):
        if self.rw.action in ['restart', 'reload']:
            # Restart and reload are effective on the watcher not on processes
            raise NotImplementedError("This case should not happend. \
                See config.")
        else:
            logger.info("Sending signal %s to proc %d of watcher %s ",
                        self.rw.action, self.pid, self.rw.watcher)
            self.rw.statsd.increment("_resource_watcher.%s.%s.signal.%s" %
                                     (self.rw.watcher,
                                      self.pid,
                                      self.rw.action))
            self.rw.cast("signal", name=self.rw.watcher, pid=self.pid,
                         signum=self.rw.action)

        self._counters = defaultdict(int)
