#!/usr/bin/env python3
import logging
import os

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from pathlib import Path

class EventualFS(LoggingMixIn, Operations):
    'Write to memory, then sync to disk in background'

    def __init__(self):
        self.memory = '/dev/shm/eventual'
        self.final = '/var/eventual'
        os.mkdir(self.memory)
        os.mkdir(self.final)
        now = time()

    def do(self, command, path, *args):
        try:
            command(self.memory+path, *args)
        except IOError:
            command(self.final+path, *args)

    def chmod(self, path, mode):
        self.do(os.chmod, path, mode)

    def chown(self, path, uid, gid):
        self.do(os.chown, path, uid, gid)

    def create(self, path, mode):
        with open(self.memory + path, 'a') as f:
            return f.fileno()

    def getattr(self, path, fh=None):
        Path(path).stat()
            #raise FuseOSError(ENOENT)

    # def getxattr(self, path, name, position=0):
    #     attrs = self.files[path].get('attrs', {})

    #     try:
    #         return attrs[name]
    #     except KeyError:
    #         return ''       # Should return ENOATTR

    # def listxattr(self, path):
    #     attrs = self.files[path].get('attrs', {})
    #     return attrs.keys()

    def mkdir(self, path, mode):
        print('FUUUUUUUUUUUCK')
        print(path)
        self.files[path] = dict(
            st_mode=(S_IFDIR | mode),
            st_nlink=2,
            st_size=0,
            st_ctime=time(),
            st_mtime=time(),
            st_atime=time())

        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        return ['.', '..'] + [x[1:] for x in self.files if x != '/']

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.data[new] = self.data.pop(old)
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        # with multiple level support, need to raise ENOTEMPTY if contains any files
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(
            st_mode=(S_IFLNK | 0o777),
            st_nlink=1,
            st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
        # make sure extending the file fills in zero bytes
        self.data[path] = self.data[path][:length].ljust(
            length, '\x00'.encode('ascii'))
        self.files[path]['st_size'] = length

    def unlink(self, path):
        self.data.pop(path)
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        self.data[path] = (
            # make sure the data gets inserted at the right offset
            self.data[path][:offset].ljust(offset, '\x00'.encode('ascii'))
            + data
            # and only overwrites the bytes that data is replacing
            + self.data[path][offset + len(data):])
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(EventualFS(), args.mount, foreground=True, allow_other=True)
