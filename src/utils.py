import json
import logging
import logging.config
from collections import OrderedDict
from enum import Enum

import jsonpickle

import config as cfg
import hybrid_dfs_pb2


class Status:
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message


class ChunkStatus(Enum):
    TEMPORARY = 0
    FINISHED = 1
    DELETED = 2


class Chunk:
    def __init__(self, chunk_handle: str, chunk_locs):
        self.handle = chunk_handle
        self.locs = chunk_locs
        self.status = ChunkStatus.TEMPORARY

    def __repr__(self):
        res = self.handle + ": " + str(self.locs)
        return res

    def __str__(self):
        return self.__repr__()


class FileStatus(Enum):
    DELETING = 0
    WRITING = 1
    COMMITTED = 2


class File:
    def __init__(self, file_path: str, creation_time):
        self.path = file_path
        self.creation_time = creation_time
        self.chunks = OrderedDict()
        self.status = FileStatus.WRITING

    def __repr__(self):
        res = f"[file_path: {self.path}"
        res += f", creation_time: {self.creation_time}"
        res += f", chunks: {self.chunks}"
        res += f", status: {self.status}]"
        return res

    def display(self):
        res = '{:>12} {:>12} {:>12} {:>12}'.format(self.status.name, len(self.chunks), self.creation_time, self.path)
        return res

    def __str__(self):
        return self.__repr__()


def filter_maker(level):
    level = getattr(logging, level)

    def filter(record):
        return record.levelno <= level

    return filter


class Logger:
    def __init__(self, log_file):
        try:
            logging.config.dictConfig(json.loads(cfg.LOGGER_CONFIG))
            self.log = logging.getLogger("DFS_master")
            print(f"Master server started. Logging to {log_file}")
        except EnvironmentError:
            print("Error: Failed to open log file")

    def add_file(self, file: File):
        self.log.info(f"add_file {file.path} {str(file.creation_time)}")

    def add_chunk(self, file_path: str, chunk_handle: str):
        self.log.info(f"add_chunk {file_path} {chunk_handle}")

    def change_chunk_locs(self, file_path: str, chunk_handle: str, new_locs):
        self.log.info(f"change_chunk_locs {file_path} {chunk_handle} {jsonpickle.encode(new_locs)}")

    def commit_chunk(self, file_path: str, chunk_handle: str):
        self.log.info(f"commit_chunk {file_path} {chunk_handle}")

    def commit_file(self, file_path: str):
        self.log.info(f"commit_file {file_path}")

    def delete_file(self, file_path: str):
        self.log.info(f"delete_file {file_path}")


def stream_list(arr):
    for i in arr:
        yield hybrid_dfs_pb2.String(str=str(i))
