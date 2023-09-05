# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import re
import os

import datetime
import threading

from yamc.providers import PerformanceProvider, perf_checker
from ..log import LogReader, SOAOutLogEntry

from yamc.utils import Map

component_re = r"ComponentDN: ([\w]+)\/([\w]+)\!([0-9\.]+).*?\/([\w]+)"
seconds_re = r"seconds since begin=([0-9]+).+seconds left=([0-9]+)"


def round_time_minutes(time, minutes):
    rounded_minutes = (time.minute // minutes) * minutes
    rounded_time = time.replace(minute=rounded_minutes, second=0, microsecond=0)
    return rounded_time


class GroupEntry:
    def __init__(self, entry) -> None:
        self.entries = []
        self.first_time = None
        self.last_time = None
        self.modified = False
        self.add_entry(entry)
        self._dn = None
        self._seconds = None

    def add_entry(self, entry) -> bool:
        if len(self.entries) == 0 or self.entries[0].flow_id == entry.flow_id:
            if self.first_time is None or self.first_time > entry.time:
                self.first_time = entry.time
            if self.last_time is None or self.last_time < entry.time:
                self.last_time = entry.time
            self.entries.append(entry)
            self.modified = True
            return True
        else:
            return False

    def _parse_dn(self):
        if self._dn is None:
            for e in self.entries:
                try:
                    match = next(re.finditer(component_re, e.payload, re.MULTILINE))
                    self._dn = Map(
                        partition=match.group(1),
                        composite=match.group(2),
                        version=match.group(3),
                        component=match.group(4),
                    )
                    return
                except StopIteration:
                    continue

    def parse_seconds(self):
        if self._seconds is None:
            for e in self.entries:
                try:
                    match = next(re.finditer(seconds_re, e.payload, re.MULTILINE))
                    self._seconds = Map(
                        begin=int(match.group(1)),
                        left=int(match.group(2)),
                    )
                    return
                except StopIteration:
                    continue

    @property
    def composite(self):
        self._parse_dn()
        return self._dn.composite if self._dn is not None else None

    @property
    def partition(self):
        self._parse_dn()
        return self._dn.partition if self._dn is not None else None

    @property
    def version(self):
        self._parse_dn()
        return self._dn.version if self._dn is not None else None

    @property
    def component(self):
        self._parse_dn()
        return self._dn.component if self._dn is not None else None

    @property
    def seconds_begin(self):
        self.parse_seconds()
        return self._seconds.begin if self._seconds is not None else None

    @property
    def seconds_left(self):
        self.parse_seconds()
        return self._seconds.left if self._seconds is not None else None

    @property
    def time(self):
        return self.entries[0].time

    @property
    def flow_id(self):
        return self.entries[0].flow_id

    @property
    def timespan(self):
        return self.last_time - self.first_time

    def to_dict(self):
        return Map(
            time=self.time,
            flow_id=self.flow_id,
            timespan=self.timespan,
            num_entries=len(self.entries),
            composite=self.composite,
            version=self.version,
            component=self.component,
            seconds_begin=self.seconds_begin,
            seconds_left=self.seconds_left,
        )


class SOAOutLogProvider(PerformanceProvider):
    """
    A yamc provider for Oracle SOA out logs.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.soaout_log = self.config.value_str("soaout_log", required=True)
        self.datetime_format = self.config.value("datetime_format", default="%b %d, %Y %I:%M:%S,%f %p UTC")
        self.buffer_minutes = self.config.value("buffer_minutes", default=2)
        self.simulated_time = self.config.value("simulated_time.start", default=None)
        self.simulated_time_delta = self.config.value("simulated_time.delta", default=1)
        self.simulated_time_format = self.config.value("simulated_time.format", default="%Y-%m-%d %H:%M:%S")
        self._time = None
        self.reader = LogReader(self.soaout_log, self.datetime_format, SOAOutLogEntry)
        self.lock = threading.Lock()
        self.groups = []
        self.data = []

    @property
    def source(self):
        return self.soaout_log

    def time(self):
        if self.simulated_time is not None:
            if self._time is None:
                self._time = datetime.datetime.strptime(self.simulated_time, self.simulated_time_format)
            else:
                self._time += datetime.timedelta(minutes=self.simulated_time_delta)
        else:
            self._time = datetime.datetime.now()
        return self._time

    def update(self, time_delta=1):
        with self.lock:
            _now = self.time()
            time_from = round_time_minutes(_now - datetime.timedelta(minutes=time_delta), time_delta)
            time_to = round_time_minutes(_now, time_delta)
            self.log.debug(
                "Reading data from %s to %s. There are %s groups in the buffer.", time_from, time_to, len(self.groups)
            )

            for g in self.groups:
                g.modified = False

            self.reader.open()
            try:
                for entry in self.reader.read(time_from=time_from, time_to=time_to):
                    if entry.flow_id is not None:
                        group = None
                        for g in self.groups:
                            if g.add_entry(entry):
                                group = g
                                break
                        if group is None:
                            self.groups.append(GroupEntry(entry))
            finally:
                self.reader.close()

            self.log.debug("Reading finished. There are %s groups in the buffer.", len(self.groups))

            time_emit = round_time_minutes(_now - datetime.timedelta(minutes=self.buffer_minutes), time_delta)
            self.log.debug("Creating groups to emit. The emit time is %s", time_emit)
            self.data = []
            for g in self.groups:
                if g.time < time_emit:
                    if not g.modified:
                        self.data.append(g.to_dict())
                    else:
                        self.log.debug(
                            "The group to be emited was modified in the last collector iteration: %s", g.to_dict()
                        )
            self.log.debug("Emiting %s groups", len(self.data))
            self.groups = [g for g in self.groups if g.time >= time_emit]
            self.log.debug("There are %s groups in the buffer after emit.", len(self.groups))

    @perf_checker()
    def soaerrors(self):
        self.update()
        return self.data
