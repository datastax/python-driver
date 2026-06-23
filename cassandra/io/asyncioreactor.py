from cassandra.connection import Connection, ConnectionShutdown

import atexit
import asyncio
import logging
import os
import socket
import ssl
from threading import Lock, Thread, get_ident


log = logging.getLogger(__name__)


def _cleanup():
    """
    Module-level cleanup called at interpreter shutdown via atexit.
    Stops the asyncio event loop and joins the loop thread.
    """
    loop = AsyncioConnection._loop
    thread = AsyncioConnection._loop_thread
    if loop is not None:
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            # loop may already be closed post-fork or during shutdown
            pass
    if thread is not None:
        thread.join(timeout=1.0)
        if thread.is_alive():
            log.warning(
                "Event loop thread could not be joined, so shutdown may not be clean. "
                "Please call Cluster.shutdown() to avoid this.")
        else:
            log.debug("Event loop thread was joined")


atexit.register(_cleanup)


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
                                          callback=callback)
        self._handle = asyncio.run_coroutine_threadsafe(delayed, loop=loop)

    @staticmethod
    async def _call_delayed_coro(timeout, callback):
        await asyncio.sleep(timeout)
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
        raise NotImplementedError('{} is not compatible with TimerManager and '
                                  'does not implement .finish()')


class AsyncioConnection(Connection):
    """
    An implementation of :class:`.Connection` that uses the ``asyncio``
    module in the Python standard library for its event loop.

    This is the preferred connection class on Python 3.12+ where the
    ``asyncore`` module has been removed. It is also used as a fallback
    when the libev C extension is not available.
    """

    _loop = None
    _pid = os.getpid()

    _lock = Lock()
    _loop_thread = None

    _write_queue = None
    _write_queue_lock = None

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self._connect_socket()
        self._socket.setblocking(0)

        self._write_queue = asyncio.Queue()
        self._write_queue_lock = asyncio.Lock()

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
                log.debug("Detected fork, clearing and reinitializing reactor state")
                cls.handle_fork()
            if cls._loop is None:
                cls._loop = asyncio.new_event_loop()

            if not cls._loop_thread or not cls._loop_thread.is_alive():
                # daemonize so the loop will be shut down on interpreter
                # shutdown
                cls._loop_thread = Thread(target=cls._loop.run_forever,
                                          daemon=True, name="asyncio_thread")
                cls._loop_thread.start()

    @classmethod
    def handle_fork(cls):
        """
        Called after a fork.  Cleans up any reactor state from the parent
        process so that a fresh event loop can be started in the child.
        """
        if cls._loop is not None:
            try:
                cls._loop.call_soon_threadsafe(cls._loop.stop)
            except RuntimeError:
                pass
        cls._loop = None
        cls._loop_thread = None
        cls._pid = os.getpid()

    @classmethod
    def create_timer(cls, timeout, callback):
        return AsyncioTimer(timeout, callback, loop=cls._loop)

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        # close from the loop thread to avoid races when removing file
        # descriptors
        asyncio.run_coroutine_threadsafe(
            self._close(), loop=self._loop
        )

    async def _close(self):
        log.debug("Closing connection (%s) to %s" % (id(self), self.endpoint))
        if self._write_watcher:
            self._write_watcher.cancel()
        if self._read_watcher:
            self._read_watcher.cancel()
        if self._socket:
            self._loop.remove_writer(self._socket.fileno())
            self._loop.remove_reader(self._socket.fileno())
            self._socket.close()

        log.debug("Closed socket to %s" % (self.endpoint,))

        if not self.is_defunct:
            self.error_all_requests(
                ConnectionShutdown("Connection to %s was closed" % self.endpoint))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def push(self, data):
        buff_size = self.out_buffer_size
        if len(data) > buff_size:
            chunks = []
            for i in range(0, len(data), buff_size):
                chunks.append(data[i:i + buff_size])
        else:
            chunks = [data]

        if self._loop_thread.ident != get_ident():
            asyncio.run_coroutine_threadsafe(
                self._push_msg(chunks),
                loop=self._loop
            )
        else:
            # avoid races/hangs by just scheduling this, not using threadsafe
            self._loop.create_task(self._push_msg(chunks))

    async def _push_msg(self, chunks):
        # This lock ensures all chunks of a message are sequential in the Queue
        async with self._write_queue_lock:
            for chunk in chunks:
                self._write_queue.put_nowait(chunk)


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
            except asyncio.CancelledError:
                return

    async def handle_read(self):
        while True:
            try:
                buf = await self._loop.sock_recv(self._socket, self.in_buffer_size)
                self._iobuf.write(buf)
            # sock_recv expects EWOULDBLOCK if socket provides no data, but
            # nonblocking ssl sockets raise these instead, so we handle them
            # ourselves by yielding to the event loop, where the socket will
            # get the reading/writing it "wants" before retrying
            except (ssl.SSLWantWriteError, ssl.SSLWantReadError):
                # Apparently the preferred way to yield to the event loop from within
                # a native coroutine based on https://github.com/python/asyncio/issues/284
                await asyncio.sleep(0)
                continue
            except socket.error as err:
                log.debug("Exception during socket recv for %s: %s",
                          self, err)
                self.defunct(err)
                return  # leave the read loop
            except asyncio.CancelledError:
                return

            if buf and self._iobuf.tell():
                self.process_io_buffer()
            else:
                log.debug("Connection %s closed by server", self)
                self.close()
                return
