import warnings
from circus.plugins.statsd import BaseObserver
from circus.util import human2bytes
from collections import defaultdict
import six


class ResourceWatcher(BaseObserver):

    def __init__(self, *args, **config):
        super(ResourceWatcher, self).__init__(*args, **config)
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
        self.health_threshold = float(config.get("health_threshold",
                                      75))                  # in %
        self.max_count = int(config.get("max_count", 3))
        self.use_reload = bool(config.get("use_reload"))
        self.per_process = bool(config.get("per_process", True))
        self._counters = defaultdict(lambda: defaultdict(int))

    def look_after(self):
        stats = self.collect_stats()
        if stats is None:
            return

        if self.per_process:
            for item in filter(lambda x: x != self.watcher, stats):
                self.update_counters(stats, item)
                self.process_counters(item)
        else:
            self.update_counters(stats, self.watcher)
            self.process_counters(self.watcher)

    def collect_stats(self):
        stats = self.call("stats", name=self.watcher)
        if stats["status"] == "error":
            self.statsd.increment("_resource_watcher.%s.error" % self.watcher)
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

    def update_counters(self, stats, item):
        self.update_counters_of_metric(stats, item, 'max_cpu')
        self.update_counters_of_metric(stats, item, 'min_cpu')
        if type(self.max_mem) == int:                       # see max_mem doc
            self.update_counters_of_metric(stats, item, 'max_mem_abs')
        else:
            self.update_counters_of_metric(stats, item, 'max_mem')
        if type(self.min_mem) == int:                       # see min_mem doc
            self.update_counters_of_metric(stats, item, 'min_mem_abs')
        else:
            self.update_counters_of_metric(stats, item, 'min_mem')
        self.update_counters_of_health(stats, item)

    def update_counters_of_metric(self, stats, item, metric):
        current_value = stats[item][metric[4:]]
        threshold = getattr(self, metric.strip('_abs'))     # see max_mem doc
        if current_value == 'N/A' or threshold is None:
            return
        limit_type = metric[:3]
        if (limit_type == 'max' and current_value > threshold or
                limit_type == 'min' and current_value < threshold):
            self.statsd.increment("_resource_watcher.%s.%s_%s" %
                                  (self.watcher,
                                   "under" if limit_type == 'min' else "over",
                                   metric.split('_')[1]))
            self._counters[item][metric] += 1
        else:
            self._counters[item][metric] = 0

    def update_counters_of_health(self, stats, item):
        if self.health_threshold:
            health = 0
            if stats[item]['cpu'] != 'N/A':
                health += stats[item]['cpu']
            if stats[item]['mem'] != 'N/A':
                health += stats[item]['mem']
            if health/2.0 > self.health_threshold:
                self.statsd.increment("_resource_watcher.%s.over_health" %
                                      self.watcher)
                self._counters[item]['health'] += 1
            else:
                self._counters[item]['health'] = 0

    def process_counters(self, item):
        # TODO: reload exceeding process but not the entire watcher when
        #       reload command will allow it.
        if max(self._counters[item].values()) > self.max_count:
            if self.use_reload:
                self.statsd.increment("_resource_watcher.%s.reloading" %
                                      self.watcher)
                self.cast("reload", name=self.watcher)
            else:
                self.statsd.increment("_resource_watcher.%s.restarting" %
                                      self.watcher)
                self.cast("restart", name=self.watcher)
            self._counters = defaultdict(lambda: defaultdict(int))
