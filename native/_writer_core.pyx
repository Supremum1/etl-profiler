# Extracted from the local asynch experiment; nullable-column fast path removed.
# See THIRD_PARTY_LICENSE.txt for the upstream asynch license.
from cpython.bytes cimport (
    PyBytes_AS_STRING,
    PyBytes_Check,
    PyBytes_FromStringAndSize,
    PyBytes_GET_SIZE,
)
from cpython.mem cimport PyMem_Free, PyMem_Malloc, PyMem_Realloc
from libc.string cimport memcpy, memset


cdef class WriterCore:
    cdef char* buffer
    cdef public unsigned long long position
    cdef unsigned long long capacity

    def __cinit__(self, unsigned long long initial_size):
        if initial_size == 0:
            initial_size = 1
        self.buffer = <char*>PyMem_Malloc(initial_size)
        if not self.buffer:
            raise MemoryError()
        self.position = 0
        self.capacity = initial_size

    def __dealloc__(self):
        if self.buffer:
            PyMem_Free(self.buffer)
            self.buffer = NULL

    cdef void _ensure_capacity(self, unsigned long long extra):
        cdef unsigned long long required = self.position + extra
        cdef unsigned long long new_capacity
        cdef char* new_buffer

        if required <= self.capacity:
            return

        new_capacity = self.capacity
        while new_capacity < required:
            new_capacity *= 2

        new_buffer = <char*>PyMem_Realloc(self.buffer, new_capacity)
        if not new_buffer:
            raise MemoryError()
        self.buffer = new_buffer
        self.capacity = new_capacity

    cpdef write(self, data):
        cdef unsigned long long data_len
        cdef char* c_data

        if not PyBytes_Check(data):
            data = bytes(data)

        data_len = PyBytes_GET_SIZE(data)
        if data_len == 0:
            return

        self._ensure_capacity(data_len)
        c_data = PyBytes_AS_STRING(data)
        memcpy(&self.buffer[self.position], c_data, data_len)
        self.position += data_len

    cpdef write_varint(self, unsigned long long number):
        cdef unsigned char to_write
        cdef unsigned char i = 0
        cdef unsigned char num_buf[32]

        while True:
            to_write = number & 0x7f
            number >>= 7
            if number:
                num_buf[i] = to_write | 0x80
                i += 1
            else:
                num_buf[i] = to_write
                i += 1
                break

        self._ensure_capacity(i)
        memcpy(&self.buffer[self.position], <char*>num_buf, i)
        self.position += i

    cpdef write_str(self, value):
        cdef unsigned long long value_len

        if not PyBytes_Check(value):
            value = value.encode()

        value_len = PyBytes_GET_SIZE(value)
        self.write_varint(value_len)
        self.write(value)

    cpdef write_strings(self, items):
        for value in items:
            self.write_str(value)

    cpdef write_fixed_strings(self, items, Py_ssize_t length):
        cdef Py_ssize_t buf_pos = 0
        cdef Py_ssize_t value_len
        cdef Py_ssize_t items_buf_size = length * len(items)
        cdef char* c_value
        cdef char* items_buf

        if items_buf_size == 0:
            return

        items_buf = <char*>PyMem_Malloc(items_buf_size)
        if not items_buf:
            raise MemoryError()

        memset(items_buf, 0, items_buf_size)
        try:
            for value in items:
                if not PyBytes_Check(value):
                    value = value.encode()

                value_len = PyBytes_GET_SIZE(value)
                if length < value_len:
                    raise ValueError("Too large string size")

                c_value = PyBytes_AS_STRING(value)
                memcpy(&items_buf[buf_pos], c_value, value_len)
                buf_pos += length

            self._ensure_capacity(items_buf_size)
            memcpy(&self.buffer[self.position], items_buf, items_buf_size)
            self.position += items_buf_size
        finally:
            PyMem_Free(items_buf)

    cpdef bytes take(self):
        cdef bytes result = PyBytes_FromStringAndSize(self.buffer, self.position)
        self.position = 0
        return result
