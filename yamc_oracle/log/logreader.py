# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas.vitvar@oracle.com

import os
import re
from datetime import datetime
from typing import Iterator, Any, Type, List, Callable
import yaml

from abc import ABC, abstractmethod


class FieldRules:
    def __init__(self, config_file) -> None:
        config = yaml.load(open(config_file, "r", encoding="utf-8"), Loader=yaml.FullLoader)
        self.rules = config.get("rules")
        if self.rules is None:
            raise Exception("Invalid config file. The top level field 'rules' is missing.")
        if type(self.rules) is not list:
            raise Exception("Invalid config file. The top level field 'rules' must be a list.")

        for v in self.rules:
            if v.get("component") is None:
                raise Exception("Invalid config file. The component field is missing.")
            if v.get("fields") is None:
                raise Exception("Invalid config file. The fields field is missing.")
            if type(v["fields"]) is not list:
                raise Exception("Invalid config file. The fields field must be a list.")

            v["__component"] = re.compile(v["component"])
            for v2 in v["fields"]:
                if v2.get("pattern") is None:
                    raise Exception("Invalid config file. The pattern field is missing.")
                if v2.get("field") is None:
                    raise Exception("Invalid config file. The field field is missing.")
                v2["__pattern"] = re.compile(v2["pattern"])

    def eval_rules(self, component: str, payload: str) -> dict:
        fields = {}
        for v in self.rules:
            if v["component"] == ".*" or v["__component"].search(component):
                for v2 in v["fields"]:
                    if fields.get(v2["field"]) is None:
                        m = v2["__pattern"].search(payload, re.DOTALL)
                        if m and v2.get("value") is None:
                            fields[v2["field"]] = m.group()
                        elif m and v2.get("value") is not None:
                            fields[v2["field"]] = v2["value"]
                        else:
                            fields[v2["field"]] = None
        return fields


class LogEntry(ABC):
    """
    A class representing a single log entry.
    """

    def __init__(self, datetime_format: str) -> None:
        """
        Create a new log entry.
        """
        self.datetime_format = datetime_format
        self.time = None
        self._payload = None
        self._message = None
        self.lines = []

    def add_line(self, line: str) -> None:
        """
        Add a line to the log entry.
        """
        self.lines.append(line)
        self._payload = None
        self._message = None

    def finish(self) -> None:
        """
        This method is called when the log entry is complete.
        """
        pass

    @property
    def payload(self):
        """
        Return the payload of the log entry. The payload is the message without the header.
        """
        if self._payload is None:
            self._payload = self.message
            if len(self._payload) > self.startinx_payload:
                self._payload = self._payload[self.startinx_payload :]
        return self._payload

    @property
    def message(self):
        """
        Return the message of the log entry. The message is the entire log entry as the orginal representation
        retrieved from the log file.
        """
        if self._message is None:
            self._message = "\n".join(self.lines)
        return self._message

    @abstractmethod
    def line_parser(self, line: str) -> Iterator[str]:
        pass

    def parse_datetime(self, line) -> datetime:
        """
        Parse the datetime from the log entry.
        """
        try:
            return datetime.strptime(next(self.line_parser(line)), self.datetime_format)
        except ValueError or StopIteration:
            return None

    @abstractmethod
    def parse_header(self, line) -> bool:
        """
        Parse the header of the log entry.
        """
        pass


class LogReader(ABC):
    """
    A class for reading WLS out logs.
    """

    def __init__(self, logfile: str, datetime_format: str = None, logentry_class: Type[LogEntry] = None) -> None:
        self.handler = None
        self.logentry_class = logentry_class
        self.logfile = logfile
        self.datetime_format = datetime_format

    def open(self, reopen=False):
        """
        Open the log file for reading.
        """
        if reopen and self.handler is not None:
            self.close()
            self.handler = None
        if self.handler is None:
            self.handler = open(self.logfile, "rb")

    def close(self):
        """
        Close the log file.
        """
        if self.handler is not None:
            self.handler.close()
            self.handler = None

    def __del__(self):
        """
        Destructor. Close the log file if it is open.
        """
        self.close()

    def create_entry(self) -> LogEntry:
        """
        Create a new log entry.
        """
        return self.logentry_class(self.datetime_format)

    def find(self, time: datetime, chunk_size: int = 1024) -> int:
        """
        Find the pos of the entry in the log file where the time of the entry is equal or greater than `time`.
        When the log entry is not found, -1 is returned.
        """

        self.open()
        start = 0
        dt_pos = -1
        end = os.path.getsize(self.logfile)
        _entry = self.create_entry()

        # use binary search to find the first log entry that matches the specified date and time.
        # Since not every line has a datetime, we need to read more lines to find the first one
        # that matches the specified datetime. We read the lines in chunks of chunk_size bytes.
        while (end - start) > 70:
            pos = start + (end - start) // 2
            self.handler.seek(pos)

            first_pos, second_pos = None, None
            chunk_pos = pos

            # read chunks to find the first and second datetime when the first time read is less
            # than time_from and the second is greater than time_from we have found the first position
            count = 0
            while count < 2:
                # read a chunk of data; 70 is the minimum number of bytes to read to get a datetime
                chunk = self.handler.read(min(chunk_size, end - start)).decode("utf-8")
                lines = chunk.split("\n")
                current_bytes = 0
                for l in lines:
                    # parse the datetime from the line
                    dt = _entry.parse_datetime(l)
                    if dt is not None:
                        dt_pos = chunk_pos + current_bytes
                        count += 1
                        if time <= dt:
                            first_pos = chunk_pos
                        if time > dt or chunk_pos == 0:
                            second_pos = chunk_pos
                    if first_pos is not None and second_pos is not None:
                        break
                    current_bytes += len(l) + 1
                chunk_pos += len(chunk)
                if chunk_pos >= end:
                    break

            if (first_pos is not None and second_pos is None) or (first_pos is None and second_pos is None):
                end = pos
            elif second_pos is not None and first_pos is None:
                start = chunk_pos
            else:
                break
        return dt_pos

    def read(
        self,
        time_from: datetime = None,
        time_to: datetime = None,
        count: int = None,
        chunk_size: int = 1024,
    ) -> Iterator[LogEntry]:
        """
        Read the log file and return an iterator of log entries. The log entries are returned in the order
        they appear in the log file. The iterator can be limited by the time_from and time_to parameters.
        The count parameter can be used to limit the number of log entries returned.
        """

        if time_from is not None:
            start_pos = self.find(time_from, chunk_size)
        else:
            start_pos = 0
        if start_pos < 0:
            return
        entry, dt = None, None
        reminder = ""
        _count = 0
        self.open()
        self.handler.seek(start_pos)
        while dt is None or (time_to is None or dt <= time_to):
            chunk = self.handler.read(chunk_size).decode("utf-8")
            if len(chunk) == 0:
                break
            lines = chunk.split("\n")
            lines[0] = reminder + lines[0]
            has_reminder = chunk[-1] != "\n"
            for inx, l in enumerate(lines[0:-1] if has_reminder else lines[0:]):
                _entry = self.create_entry()
                dt = _entry.time if _entry.parse_header(l) else None
                if dt is not None:
                    if entry is not None:
                        _count += 1
                        entry.finish()
                        yield entry
                        entry = None
                        if count is not None and _count >= count:
                            break
                    if time_to is not None and dt > time_to:
                        break
                    entry = _entry
                elif entry is not None:
                    entry.add_line(l)
            reminder = lines[-1] if has_reminder else ""
        if entry is not None:
            yield entry


class LogStorage:
    def __init__(self, dir) -> None:
        self.dir = dir
        self.entries = []

    def add_entry(self, entry: LogEntry):
        self.entries.append(entry)

    def store(self):
        for entry in self.entries:
            entry.store(self.dir)
