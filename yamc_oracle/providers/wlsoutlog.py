# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import re
import os

import datetime

from yamc.providers import PerformanceProvider

from yamc.utils import Map, perf_counter


def round_time_minutes(time, minutes):
    rounded_minutes = (time.minute // minutes) * minutes
    rounded_time = time.replace(minute=rounded_minutes, second=0, microsecond=0)
    return rounded_time


class WLSOutLogProvider(PerformanceProvider):
    """
    A yamc provider for Oracle WLS out logs.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.out_log = self.config.value_str("outlog", required=True)
        self.datetime_format = self.config.value("datetime_format", default="%b %d, %Y %I:%M:%S,%f %p UTC")
        self.simulated_time = self.config.value("simulated_time.start", default=None)
        self.simulated_time_delta = self.config.value("simulated_time.delta", default=1)
        self.simulated_time_format = self.config.value("simulated_time.format", default="%Y-%m-%d %H:%M:%S")
        self._time = None

    def time(self):
        if self.simulated_time is not None:
            if self._time is None:
                self._time = datetime.datetime.strptime(self.simulated_time, self.simulated_time_format)
            else:
                self._time += datetime.timedelta(minutes=self.simulated_time_delta)
        else:
            self._time = datetime.datetime.now()
        return self._time

    def update(self, id=None, time_delta=1):
        _time = self.time()
        if id is None:
            id = self.component_id
        if self.data is None:
            self.data = Map()
        data = self.data.get(id, Map(data=None, time_from=None, time_to=None))
        if data.data is None or _time > data.time_to:
            data.time_from = round_time_minutes(_time - datetime.timedelta(minutes=time_delta), time_delta)
            data.time_to = round_time_minutes(_time, time_delta)
            # entries = find_entries(self.out_log, self.datetime_format, data.time_from, data.time_to)
            entries = read_entries(self.out_log, 0, self.datetime_format)
            print(entries[0]["time"], entries[-1]["time"], len(entries))
            for e in entries:
                if e["type"] == "Error":
                    print(e["time"], ":", e["lines"][0][e["startinx_msg"] : 300])
                # elif clazz is None:
                # print(e["clazz"], e["lines"][0])

            # for e in entries:
            #     print("time: ", e["time"], ", type: ", e["type"], ", clazz: ", e["clazz"], ", count: ", len(e["lines"]))
            #     print("      ", e["lines"][0])
            #     if len(e["lines"]) > 1:
            #         print("      ", e["lines"][1])

        return entries

    def test(self):
        self.update()
        return {"hello": "world"}
