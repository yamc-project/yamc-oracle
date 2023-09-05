# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import re
from .logreader import LogEntry

from typing import Iterator, Tuple

from datetime import datetime, timedelta


DEFAULT_DATETIME_FORMAT = "%b %d, %Y %I:%M:%S,%f %p UTC"

FLOW_ID_PATTERN = re.compile(r"FlowId[:=]\s*(\d+)")
EXCEPTION_PATTERN = re.compile(r"\b([a-zA-Z\.0-9]+?\.[a-zA-Z0-9]+?Exception)(?!\()\b", re.MULTILINE)


class OutLogEntry(LogEntry):
    """
    A class representing a single log entry.
    """

    def __init__(self, datetime_format: str = DEFAULT_DATETIME_FORMAT) -> None:
        """
        Create a new log entry.
        """
        super().__init__(datetime_format)
        self.type = None
        self.component = None
        self.bea_code = None
        self.startinx_payload = 0
        self.exception = None

    def line_parser(self, line: str) -> Iterator[Tuple[str, int]]:
        pos = 0
        while pos < len(line):
            pos1 = line.find("<", pos)
            if pos1 != -1:
                pos2 = line.find(">", pos1 + 1)
                if pos2 != -1:
                    pos = pos2 + 1
                    yield line[pos1 + 1 : pos2], pos
                else:
                    break
            else:
                break

    def parse_datetime(self, line) -> datetime:
        """
        Parse the datetime from the log entry.
        """
        try:
            return datetime.strptime(next(self.line_parser(line))[0], self.datetime_format)
        except ValueError:
            return None
        except StopIteration:
            return None

    def parse_header(self, line) -> bool:
        """
        Parse the header of the log entry.
        """
        try:
            parser = self.line_parser(line)
            self.time = datetime.strptime(next(parser)[0], self.datetime_format)
            self.type = next(parser)[0]
            self.component = next(parser)[0]
            self.bea_code, self.startinx_payload = next(parser)
            self.add_line(line)
            return True
        except ValueError:
            return False
        except StopIteration:
            return False

    def finish(self) -> None:
        """
        This method is called when the log entry is complete.
        """
        exs = []
        m = re.finditer(EXCEPTION_PATTERN, self.payload)
        for match in m:
            exs.append(match.group(1).split(".")[-1])
        if len(exs) > 0:
            self.exception = ",".join(set(exs))


class SOAOutLogEntry(OutLogEntry):
    def __init__(self, datetime_format: str = DEFAULT_DATETIME_FORMAT) -> None:
        """
        Create a new SOA log entry.
        """
        super().__init__(datetime_format)
        self.flow_id = None

    def finish(self) -> None:
        """
        This method is called when the log entry is complete.
        """
        super().finish()
        m = FLOW_ID_PATTERN.search(self.payload)
        if m is not None:
            self.flow_id = m.group(1)


class EntryGroup:
    """
    A class representing a group of entries. The group is created by grouping entries by a specific condition.
    """

    def __init__(self) -> None:
        self.entries = []
        self.first_time = None
        self.last_time = None

    def add_entry(self, entry: OutLogEntry) -> None:
        if self.first_time is None or self.first_time > entry.time:
            self.first_time = entry.time
        if self.last_time is None or self.last_time < entry.time:
            self.last_time = entry.time
        self.entries.append(entry)

    def has_component(self, component: str) -> bool:
        return component in [x.component for x in self.entries]

    # def group_by(
    #     self,
    #     group_match: function,
    #     entry_types: list = ["Error"],
    #     time_from: datetime = None,
    #     time_to: datetime = None,
    #     entrygroup_class: type = EntryGroup,
    #     count: int = None,
    #     chunk_size: int = 1024,
    # ):
    #     """
    #     Return the list of groups of entries based on group by criteria time_delta and entry_types.
    #     """
    #     _groups = []
    #     for entry in self.read(time_from=time_from, time_to=time_to, count=count, chunk_size=chunk_size):
    #         if entry.type in entry_types:
    #             group = None
    #             for g in _groups:
    #                 if not g.has_component(entry.component) and group_match(g, entry):
    #                     group = g
    #                     break
    #             if group is None:
    #                 group = entrygroup_class()
    #                 _groups.append(group)
    #             group.add_entry(entry)
    #     return _groups

    # if self.rules is not None:
    #     ex_fields = self.rules.eval_rules(entry.component, entry.payload)
    #     if ex_fields is not None and len(ex_fields) > 0:
    #         entry.__dict__.update(ex_fields)
