# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import datetime
import re

from yamc.providers import PerformanceProvider, perf_checker, OperationalError

from yamc.utils import merge

from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.transports import Transport


def get_dict(pattern, line, **types):
    match = re.compile(pattern).match(line)
    if match:
        return {k: types.get(k, str)(v) for k, v in match.groupdict().items()}, True
    else:
        return {}, False


def parse_sentinel_date(v):
    return datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f")  # .replace(tzinfo=platformw_config().timezone)


def parse_spm_status(raw_status, token_blocked_time):
    """
    Parse Session Pool Manager status retrieved from the status SOAP service.
    """
    props = {
        "status": "OK",
        "CountSessionsInUse": "-",
        "SessionsInUse": [],
        "CountAvailableSessions": "-",
        "AvailableSessions": [],
    }

    block1, block2 = False, False
    for line in raw_status.splitlines():
        if line.find("NOT INITIALIZED") > 0:
            props["status"] = "DOWN"
            break
        props = merge(props, get_dict(r".*Pool Max Size:(?P<PoolMaxSize>[0-9]+).*", line, PoolMaxSize=int)[0])
        props = merge(
            props, get_dict(r".*Current Pool Size: (?P<CurrentPoolSize>[0-9]+).*", line, CurrentPoolSize=int)[0]
        )
        props = merge(props, get_dict(r".*Available Now: (?P<TokensAvailable>[0-9]+).*", line, TokensAvailable=int)[0])
        props = merge(props, get_dict(r".*Currently in Use: (?P<TokensInUse>[0-9]+).*", line, TokensInUse=int)[0])
        props = merge(
            props,
            get_dict(
                r".*Sentinel.Running since: (?P<SentinelStartupTime>[0-9\- \:\.]+).*",
                line,
                SentinelStartupTime=parse_sentinel_date,
            )[0],
        )
        props = merge(
            props,
            get_dict(r".*Sentinel.+Active: (?P<SentinelActive>[true|false]).*", line, SentinelActive=bool)[0],
        )
        props = merge(
            props,
            get_dict(
                r".*Sentinel.+Next awake:[ ]*(?P<SentinelNextAwake>[0-9\- \:\.]+).*",
                line,
                SentinelNextAwake=parse_sentinel_date,
            )[0],
        )
        props = merge(
            props,
            get_dict(
                r".*Min pool size to keep:[ ]*(?P<SentinelMinPoolSizeToKeep>[0-9]+).*",
                line,
                SentinelMinPoolSizeToKeep=int,
            )[0],
        )
        props = merge(
            props,
            get_dict(
                r".*Max times to renew a session token:[ ]*(?P<SentinelMaxTimesToRenewToken>[0-9]+).*",
                line,
                SentinelMaxTimesToRenewToken=int,
            )[0],
        )

        if block1:
            siu, block1 = get_dict(
                r".*BPELInstanceID\[(?P<InstanceId>[0-9]+)\].*Milliseconds being assigned (?P<Assigned>[0-9]+).*",
                line,
                Assigned=int,
            )
            if block1:
                props["SessionsInUse"].append(siu)

        if block2:
            avs, block2 = get_dict(r".*Age:(?P<Age>[0-9]+) Idle:(?P<Idle>[0-9]+).*", line, Age=int, Idle=int)
            if block2:
                props["AvailableSessions"].append(avs)

        p, found = get_dict(r".*Sessions In Use\((?P<CountSessionsInUse>[0-9]+)\).*", line, CountSessionsInUse=int)
        if found:
            props = merge(props, p)
            block1 = True

        p, found = get_dict(
            r".*Available Sessions \((?P<CountAvailableSessions>[0-9]+)\).*", line, CountAvailableSessions=int
        )
        if found:
            props = merge(props, p)
            block2 = True

    blocked = 0
    for s in props["SessionsInUse"]:
        if int(s["Assigned"]) / 1000 > token_blocked_time:
            s["Blocked"] = True
            blocked += 1
        else:
            s["Blocked"] = False
    props["NumBlocked"] = blocked

    return props


class SPMProvider(PerformanceProvider):
    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self._client = None
        self.data = {}
        self.max_age = self.config.value("max_age", default=60)
        self.token_blocked_time = self.config.value_int("token_blocked_time", default=400)
        self.wsdl = self.config.value_str("wsdl", required=True)
        self.read_timeout = self.config.value_int("read_timeout", default=30)
        self.username = self.config.value_str("username", required=False)
        self.password = self.config.value_str("password", required=False)

    @property
    def client(self):
        if self._client is None:
            transport = Transport(operation_timeout=self.read_timeout)
            if self.username is None or self.password is None:
                self._client = Client(self.wsdl, transport=transport)
            else:
                self._client = Client(
                    self.wsdl,
                    wsse=UsernameToken(self.username, self.password),
                    transport=transport,
                )
        return self._client

    def update(self, **kwargs):
        hostid = kwargs.get("hostid")
        if hostid is None:
            raise Exception("hostid is required")
        if self.data.get(hostid) is None or time.time() - self._updated_time > self.max_age:
            raw_status = self.client.service.Status(HostId=hostid)
            self.data[hostid] = parse_spm_status(raw_status, self.token_blocked_time)
            self._updated_time = time.time()

    @perf_checker(id_arg="hostid")
    def status(self, hostid):
        try:
            self.update(hostid=hostid)
            return self.data[hostid]
        except OperationalError as e:
            raise OperationalError(f"Error occurred when retrieving SPM status: {e}", e)
