"""Optional buffered writer for asynch 0.3.1, without modifying site-packages."""
import os

os.environ["ASYNCH_WRITER_MODE"] = "python"
from asynch.proto.streams.buffered import BufferedWriter
from asynch.proto.constants import BUFFER_SIZE
from _writer_core import WriterCore


class NativeWriter(BufferedWriter):
    def __init__(self, writer=None, max_buffer_size=BUFFER_SIZE):
        super().__init__(writer, max_buffer_size)
        self.core = WriterCore(max_buffer_size)

    async def flush(self):
        if self.writer:
            self.writer.write(self.core.take())
            self.position = 0
            await self.writer.drain()

    async def flush_if_full(self):
        self.position = self.core.position
        if self.position >= self.max_buffer_size:
            await self.flush()

    async def write_bytes(self, data):
        self.core.write(data)
        await self.flush_if_full()

    async def write_varint(self, data):
        self.core.write_varint(data)
        await self.flush_if_full()

    async def write_str(self, data):
        self.core.write_str(data)
        await self.flush_if_full()

    async def write_strings(self, data):
        self.core.write_strings(data)
        await self.flush_if_full()

    async def write_fixed_strings(self, data, length):
        self.core.write_fixed_strings(data, length)
        await self.flush_if_full()


def enable():
    from asynch.proto import connection
    connection.BufferedWriter = NativeWriter
