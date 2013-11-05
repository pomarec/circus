import os

from circus.plugins import CircusPlugin
from circus import logger
from zmq.eventloop import ioloop


class CommandReloader(CircusPlugin):

    name = 'command_reloader'

    def __init__(self, *args, **config):
        super(CommandReloader, self).__init__(*args, **config)
        self.name = config.get('name')
        self.loop_rate = int(self.config.get('loop_rate', 1))
        self.watchers = self.config.get('watchers')
        self.use_working_dir = bool(self.config.get('use_working_dir'))
        self.cmd_files = {}

    def is_modified(self, watcher, current_mtime, current_path):
        if watcher not in self.cmd_files:
            return False
        if current_mtime != self.cmd_files[watcher]['mtime']:
            return True
        if current_path != self.cmd_files[watcher]['path']:
            return True
        return False

    def mtime_of_path(self, path):
        """ If path is a dir, returns the max modified time of all files in
            path (and subpaths recursively) else returns modified time of path.
        """
        if os.path.isdir(path):
            max_mtime = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    mtime = os.stat(os.path.join(dirpath, filename)).st_mtime
                    max_mtime = max(max_mtime, mtime)
            return max_mtime
        else:
            return os.stat(path).st_mtime

    def look_after(self):
        list_ = self.call('list')
        watchers = [watcher for watcher in list_['watchers']
                    if not watcher.startswith('plugin:')]

        if self.watchers and self.watchers != '*':
            watchers = [w for w in watchers if w in self.watchers.split(',')]

        for watcher in list(self.cmd_files.keys()):
            if watcher not in watchers:
                del self.cmd_files[watcher]

        for watcher in watchers:
            winfo = self.call('get', name=watcher, keys=['cmd', 'working_dir'])
            cmd = winfo['options']['cmd']
            working_dir = winfo['options']['working_dir']
            watched_path = working_dir if self.use_working_dir else cmd
            watched_path = os.path.realpath(watched_path)
            watched_path_mtime = self.mtime_of_path(watched_path)
            if self.is_modified(watcher, watched_path_mtime, watched_path):
                logger.info('%s modified. Restarting.', watched_path)
                self.call('restart', name=watcher)
            self.cmd_files[watcher] = {
                'path': watched_path,
                'mtime': watched_path_mtime,
            }

    def handle_init(self):
        self.period = ioloop.PeriodicCallback(self.look_after,
                                              self.loop_rate * 1000,
                                              self.loop)
        self.period.start()

    def handle_stop(self):
        self.period.stop()

    def handle_recv(self, data):
        pass
