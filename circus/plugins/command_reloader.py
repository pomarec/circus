import os
import time
import math
from circus.plugins import CircusPlugin
from circus import logger
from zmq.eventloop import ioloop
from collections import defaultdict

class CommandReloader(CircusPlugin):

    name = 'command_reloader'

    def __init__(self, *args, **config):
        super(CommandReloader, self).__init__(*args, **config)
        self.name = config.get('name')
        self.loop_rate = int(self.config.get('loop_rate', 1))
        self.watchers = self.config.get('watchers', '*')
        self.use_working_dir = bool(self.config.get('use_working_dir'))
        self.use_reload = bool(self.config.get('use_reload'))
        self.infos = defaultdict(dict)
        self.skip_dirs = self.config.get('skip_dirs', '*')

    def is_modified(self, watcher, previous_path):
        if watcher not in self.infos:
            return False
        if (previous_path is not None and
            self.infos[watcher]['path'] != previous_path):
            return True

        # Get age of the watcher (max age of its processes)
        infos = self.call('stats')['infos'][watcher].values()
        if infos:
            age = max(map(lambda x: x['age'], infos))
        else:
            return False
        if time.time() - age < self.infos[watcher]['mtime']:
            if 'last_restart' not in self.infos[watcher]:
                return True
            else:
                #verification for the case where processes didn't restarted yet,but reload command was already sent
                return self.infos[watcher]['last_restart'] <  self.infos[watcher]['mtime']     
        return False

    def mtime_of_path(self, path):
        """ If path is a dir, returns the max modified time of all files in path
            (and subpaths recursively) else returns modified time of path.
        """
        if self.skip_dirs != '*':
            skip_dirs = [dir for dir in self.skip_dirs.split(',')]
        else:
            skip_dirs = []
        if os.path.isdir(path):
            max_mtime = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for dir in skip_dirs:
                    try:
                        dirnames.remove(dir)
                    except ValueError:
                        pass
                for filename in filenames:
                    mtime = os.stat(os.path.join(dirpath, filename)).st_mtime
                    max_mtime = max(max_mtime, mtime)
                    max_path = filename
            return max_mtime
        else:
            return os.stat(path).st_mtime

    def look_after(self):
        # Get concerned watchers
        # Note: watchers and their configuration can change at any time
        watchers = self.call('list')['watchers']
        if self.watchers != '*':
            watchers = [w for w in watchers if w in self.watchers.split(',')]
        for watcher in list(self.infos.keys()):
            if watcher not in watchers:
                del self.infos[watcher]     # Clean obsolete watchers

        # Check modifications
        for w in watchers:
            # We make sure not to reload to often (wait at least 2secs)
            if self.infos[w].get('last_restart') < time.time() - 2:
                w_info = self.call('get', name=w, keys=['cmd', 'working_dir'])
                previous_path = self.infos[w].get('path')
                if self.use_working_dir:
                    current_path = w_info['options']['working_dir']
                else:
                    current_path = w_info['options']['cmd']
                self.infos[w]['path'] = current_path
                self.infos[w]['mtime'] = self.mtime_of_path(current_path)
                
                if self.is_modified(w, previous_path):
                    if self.use_reload:
                        logger.info('%s modified. Reloading.',
                                    self.infos[w]['path'])
                        self.call('reload', name=w)
                    else:
                        logger.info('%s modified. Reloading.',
                                    self.infos[w]['path'])
                        self.call('restart', name=w)
                    self.infos[w]['last_restart'] = time.time()


    def handle_init(self):
        self.period = ioloop.PeriodicCallback(self.look_after,
                                              self.loop_rate * 1000,
                                              self.loop)
        self.period.start()

    def handle_stop(self):
        self.period.stop()

    def handle_recv(self, data):
        pass

