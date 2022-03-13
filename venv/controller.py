import time
from full_incremental_load import datesData, teamsData, rostersData

class Controller:
    def __init__(self):
        self.dates_data = datesData.DatesData()
        self.teams_data = teamsData.TeamsData()
        self.rosters_data = rostersData.RostersData()


    def incremental_load(self):
        #DatesData Load
        #dd_start_time = time.time()
        #self.dates_data.incremental_load()
        #dd_end_time = time.time()
        #print("Incremental load for datesData tables completed in {:.0f} seconds".format(dd_end_time-dd_start_time))

        #TeamsData Load
        #td_start_time = time.time()
        #self.teams_data.incremental_load()
        #td_end_time = time.time()
        #print("Incremental load for teamsData tables completed in {:.0f} seconds".format(td_end_time - td_start_time))

        #RostersData
        rd_start_time = time.time()
        self.rosters_data.incremental_load()
        rd_end_time = time.time()
        print("Incremental load for rostersData tables completed in {:.0f} seconds".format(rd_end_time - rd_start_time))

    def full_load(self):
        #DatesData
        dd_start_time = time.time()
        self.dates_data.full_load()
        dd_end_time = time.time()
        print("Full load for datesData tables completed in {:.0f} seconds".format(dd_end_time-dd_start_time))

        #TeamsData
        td_start_time = time.time()
        self.teams_data.full_load()
        td_end_time = time.time()
        print("Full load for teamsData tables completed in {:.0f} seconds".format(td_end_time - td_start_time))

        #RostersData
        rd_start_time = time.time()
        self.rosters_data.full_load()
        rd_end_time = time.time()
        print("Full load for rostersData tables completed in {:.0f} seconds".format(rd_end_time - rd_start_time))

