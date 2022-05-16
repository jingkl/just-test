import threading
import time
from functools import partial


class StreamRead:
    def __init__(self, file_path: str, interval: int = 600, stop_read_flag: bool = False):
        self.file_path = file_path
        self.interval = interval
        self.stop_read_flag = stop_read_flag
        self.read_finished = False
        self.tick_read_flag = True

    def set_stop_read_flag(self, stop_flag: bool = True):
        """ Set the flag to True to stop streaming reading """
        self.stop_read_flag = stop_flag

    def set_read_finished(self, stop_flag: bool = True):
        """ Set the flag to True to stop reading """
        self.read_finished = stop_flag

    def streaming_read_incremental_file(self, file_path: str = ""):
        file_path = file_path or self.file_path

        with open(file_path) as fd:

            last_position = 0
            while True:
                incremental_content = fd.read()  # read incremental content

                current_position = fd.tell()  # record file current location
                if current_position != last_position:  # there is no new file if equal
                    fd.seek(current_position, 0)
                last_position = current_position  # Move the pointer to the current position

                yield incremental_content

    def tick_read_incremental_file(self, callable_object: callable = print):
        if self.stop_read_flag:
            return True

        self.tick_read_flag = False
        incremental_content = next(self.streaming_read_incremental_file())
        callable_object(incremental_content)
        self.tick_read_flag = True

        if not self.stop_read_flag:
            t = threading.Timer(self.interval, self.tick_read_incremental_file)
            t.start()

    def final_read_incremental_file(self, callable_object: callable = print):
        self.set_stop_read_flag()
        start = time.time()
        while time.time() - start > self.interval:
            if self.tick_read_flag:
                incremental_content = next(self.streaming_read_incremental_file())
                callable_object(incremental_content)
                break

    def streaming_read_file(self, file_path: str = "", block_size=65536):
        file_path = file_path or self.file_path

        # with open(file_path, 'rb') as f:
        #     for chunk in iter(partial(f.read, block_size), ''):
        #         if chunk.decode('utf-8') == '':
        #             self.read_finished = True
        #         yield chunk.decode('utf-8')

        with open(file_path, 'rb') as f:
            while True:
                block = f.readlines(block_size)
                if block:
                    yield [b.decode('utf-8').strip('\n') for b in block]
                else:
                    self.read_finished = True
                    yield ''
