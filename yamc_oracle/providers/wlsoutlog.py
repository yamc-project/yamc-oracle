# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time

from yamc.providers import PerformanceProvider

from yamc.utils import Map, perf_counter


class WLSOutLogProvider(PerformanceProvider):
    """
    A yamc provider for Oracle WLS out logs.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.outlog = None
