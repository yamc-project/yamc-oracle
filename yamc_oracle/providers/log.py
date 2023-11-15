# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import re
import os

import datetime
import threading
import yaml
import pandas as pd

from yamc.providers import PerformanceProvider, perf_checker
from yamc.utils import import_class

from wls_analytics import config as wlsa_config
from wls_analytics.log import LabelParser


def round_time_minutes(time, minutes):
    rounded_minutes = (time.minute // minutes) * minutes
    rounded_time = time.replace(minute=rounded_minutes, second=0, microsecond=0)
    return rounded_time


class LogProvider(PerformanceProvider):
    """
    A yamc provider for Oracle logs.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.datetime_format = self.config.value("datetime_format", default="%b %d, %Y %I:%M:%S,%f %p UTC")
        self.logfile = self.config.value_str("logfile", required=True)
        self.group = self.config.value("group", default=[])
        self.wlsa_config = wlsa_config.Config(self.config.value_str("wlsa.config", required=True))
        self.wlsa_set = self.config.value_str("wlsa.set", required=True)
        self.wlsa_readerclass = import_class(self.wlsa_config(f"sets.{self.wlsa_set}.reader", required=True))
        self.wlsa_reader = self.wlsa_readerclass(self.logfile, self.datetime_format)
        self.wlsa_labelparser = LabelParser(self.wlsa_config("parsers"), [self.wlsa_set])

        self.buffer_minutes = self.config.value("buffer_minutes", default=2)
        self.simulated_time = self.config.value("simulated_time.start", default=None)
        self.simulated_time_delta = self.config.value("simulated_time.delta", default=1)
        self.simulated_time_format = self.config.value("simulated_time.format", default="%Y-%m-%d %H:%M:%S")
        self._time = None
        self.lock = threading.Lock()

        data_file = self.config.value_str("flows_dict", required=False, default=None)
        print(data_file)
        if data_file:
            self.read_flows_dict(data_file)

    @property
    def source(self):
        return self.logfile

    def read_flows_dict(self, data_file):
        self.flows_dict = {}
        if not os.path.exists(data_file):
            raise Exception(f"The flows dictionary file '{data_file}' does not exist.")
        with open(data_file, "r") as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        for flow in data.get("flows", []):
            flow_id = flow.get("flow_id")
            composites = flow.get("composites", [])
            if flow_id and composites:
                for c in composites:
                    if c not in self.flows_dict:
                        self.flows_dict[c] = []
                    if flow_id not in self.flows_dict[c]:
                        self.flows_dict[c].append(flow_id)
                    else:
                        self.log.warning("Flow %s already in composite %s.", flow_id, c)
            else:
                self.log.warning("Flow %s has no flow_id or composites.", flow.get("name"))

    def time(self):
        if self.simulated_time is not None:
            if self._time is None:
                self._time = datetime.datetime.strptime(self.simulated_time, self.simulated_time_format)
            else:
                self._time += datetime.timedelta(minutes=self.simulated_time_delta)
        else:
            self._time = datetime.datetime.now()
        return self._time

    @perf_checker()
    def out(self, max_str_len=60):
        with self.lock:
            _now = self.time()
            time_from = round_time_minutes(_now - datetime.timedelta(minutes=2), 1)
            time_to = round_time_minutes(_now - datetime.timedelta(minutes=1), 1)
            self.log.debug("Reading data from %s to %s.", time_from, time_to)
            data = []

            self.wlsa_reader.open()
            try:
                for entry in self.wlsa_reader.read_entries(time_from=time_from, time_to=time_to):
                    d = entry.to_dict(self.wlsa_labelparser)
                    for k, v in d.items():
                        if isinstance(v, str):
                            d[k] = v[:max_str_len]
                    data.append(d)
            finally:
                self.wlsa_reader.close()

                # add flows to data
                if self.flows_dict is not None:
                    _data = []
                    for d in data:
                        for f in self.flows_dict.get(d["composite"], ["Unknown"]):
                            d["flow"] = f
                            _data.append(d.copy())
                    data = _data

                # group and calc stats
                if len(data) > 0:
                    df = pd.DataFrame(data)
                    df["time"] = pd.to_datetime(df["time"]).dt.floor("min")
                    _group = ["time"] + list(set([x.lower() for x in self.group if x in df.columns]))
                    result = df.groupby(_group).size().reset_index(name="count")
                    result["epoch_time"] = (result["time"] - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)
                    data = result.to_dict(orient="records")

                return data
