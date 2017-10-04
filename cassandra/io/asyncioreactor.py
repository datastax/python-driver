from cassandra.connection import Connection

import asyncio
import logging
import os
import socket
from threading import Lock, Thread


log = logging.getLogger(__name__)


class AsyncioTimer(object):
    """
    An ``asyncioreactor``-specific Timer. Similar to :class:`.connection.Timer,
    but with a slightly different API due to limitations in the underlying
    ``call_later`` interface. Not meant to be used with a
    :class:`.connection.TimerManager`.
    """

    @property
    def end(self):
        raise NotImplementedError('{} is not compatible with TimerManager and '
                                  'does not implement .end()')

    def __init__(self, timeout, callback, loop):
        delayed = self._call_delayed_coro(timeout=timeout,
                                          callback=callback,
                                          loop=loop)
        self._handle = asyncio.run_coroutine_threadsafe(delayed, loop=loop)

    @staticmethod
    async def _call_delayed_coro(timeout, callback, loop):
        await asyncio.sleep(timeout, loop=loop)
        return callback()

    def __lt__(self, other):
        try:
            return self._handle < other._handle
        except AttributeError:
            raise NotImplemented

    def cancel(self):
        self._handle.cancel()

    def finish(self):
        # connection.Timer method not implemented here because we can't inspect
        # the Handle returned from call_later
        raise RuntimeError('finish method not implemented for asyncio reactor')


class AsyncioConnection(Connection):

    _loop = None
    _pid = os.getpid()

    _lock = Lock()
    _loop_thread = None

    _write_queue = None

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self._connect_socket()
        self._socket.setblocking(0)

        self._write_queue = asyncio.Queue(loop=self._loop)

        # see initialize_reactor -- loop is running in a separate thread, so we
        # have to use a threadsafe call
        self._read_watcher = asyncio.run_coroutine_threadsafe(
            self.handle_read(), loop=self._loop
        )
        self._write_watcher = asyncio.run_coroutine_threadsafe(
            self.handle_write(), loop=self._loop
        )
        self._send_options_message()

    @classmethod
    def initialize_reactor(cls):
        with cls._lock:
            if cls._pid != os.getpid():
                cls._loop = None
            if cls._loop is None:
                cls._loop = asyncio.get_event_loop()

            if not cls._loop_thread:
                # daemonize so the loop will be shut down on interpreter
                # shutdown
                cls._loop_thread = Thread(target=cls._loop.run_forever,
                                          daemon=True)
                cls._loop_thread.start()

    @classmethod
    def create_timer(cls, timeout, callback):
        return AsyncioTimer(timeout, callback, loop=cls._loop)

    def close(self):
        log.debug("Closing connection (%s) to %s" % (id(self), self.host))
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        self._write_watcher.cancel()
        self._read_watcher.cancel()

        self.connected_event.set()

    def push(self, data):
        buff_size = self.out_buffer_size
        if len(data) > buff_size:
            for i in range(0, len(data), buff_size):
                self._push_chunk(data[i:i + buff_size])
        else:
            self._push_chunk(data)

    def _push_chunk(self, chunk):
        fut = asyncio.run_coroutine_threadsafe(self._write_queue.put(chunk),
                                               loop=self._loop)
        asyncio.wait_for(fut,
                         timeout=None,
                         loop=self._loop)

    async def handle_write(self):
        while True:
            try:
                next_msg = await self._write_queue.get()
                if next_msg:
                    await self._loop.sock_sendall(self._socket, next_msg)
            except socket.error as err:
                log.debug("Exception in send for %s: %s", self, err)
                self.defunct(err)
                return

    async def handle_read(self):
        while True:
            try:
                buf = await self._loop.sock_recv(self._socket, self.in_buffer_size)
                self._iobuf.write(buf)
            except socket.error as err:
                log.debug("Exception during socket recv for %s: %s",
                          self, err)
                self.defunct(err)
                return  # leave the read loop

            if buf and self._iobuf.tell():
                self.process_io_buffer()
            else:
                log.debug("Connection %s closed by server", self)
                self.close()
                return
