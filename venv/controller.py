import time
from full_incremental_load import datesData, teamsData

class Controller:
    def __init__(self):
        self.dates_data = datesData.DatesData()
        self.teams_data = teamsData.TeamsData()


    def incremental_load(self):
        #DatesData Load
        #dd_start_time = time.time()
        #self.dates_data.incremental_load()
        #dd_end_time = time.time()
        #print("Incremental load for datesData tables completed in {:.0f} seconds".format(dd_end_time-dd_start_time))

        #TeamsData Load
        td_start_time = time.time()
        self.teams_data.incremental_load()
        td_end_time = time.time()
        print("Incremental load for teamsData tables completed in {:.0f} seconds".format(td_end_time - td_start_time))


    def full_load(self):
        # DatesData load
        dd_start_time = time.time()
        self.dates_data.full_load()
        dd_end_time = time.time()
        print("Full load for datesData tables completed in {:.0f} seconds".format(dd_end_time-dd_start_time))

