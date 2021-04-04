import logging
import math
from pathlib import Path
import random

import pandas as pd

#from producer import Producer
from producers.models.producer import Producer


logger = logging.getLogger(__name__)

    
class TurnstileHardware:
    curve_df = None
    seed_df = None

    def __init__(self, station):
        """Create the Turnstile"""
        self.station = station
        TurnstileHardware._load_data()
        self.metrics_df = TurnstileHardware.seed_df[
            TurnstileHardware.seed_df["station_id"] == station.station_id
        ]
        self.weekday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_weekday_rides"])
        )
        self.saturday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_saturday_rides"])
        )
        self.sunday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_sunday-holiday_rides"])
        )
