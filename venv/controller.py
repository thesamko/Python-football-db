import time
from full_incremental_load import datesData

class Controller:
    def __init__(self):
        self.dates_data = datesData.DatesData()


    def incremental_load(self):
        dd_start_time = time.time()
        self.dates_data.incremental_load()
        dd_end_time = time.time()
        print("Incremental load for datesData tables completed in {:.0f} seconds".format(dd_end_time-dd_start_time))


    def full_load(self):
        dd_start_time = time.time()
        self.dates_data.full_load()
        dd_end_time = time.time()
        print("Full load for datesData tables completed in {:.0f} seconds".format(dd_end_time-dd_start_time))

