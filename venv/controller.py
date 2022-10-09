import time
from full_incremental_load import datesData, teamsData, rostersData, groupsData, minMaxPlayerStats, shotsData, \
    playerData, statisticsData

class Controller:
    def __init__(self):
        self.dates_data = datesData.DatesData()
        self.teams_data = teamsData.TeamsData()
        self.rosters_data = rostersData.RostersData()
        self.groups_data = groupsData.GroupsData()
        self.min_max_data = minMaxPlayerStats.MinMaxPlayerStats()
        self.shots_data = shotsData.ShotsData()
        self.players_data = playerData.PlayerData()
        self.statistics_data = statisticsData.StatisticsData()


    def incremental_load(self):
        #DatesData Load
        dd_start_time = time.time()
        self.dates_data.incremental_load()
        dd_end_time = time.time()
        print("Incremental load for datesData tables completed in {:.0f} seconds".format(dd_end_time-dd_start_time))

        #TeamsData Load
        td_start_time = time.time()
        self.teams_data.incremental_load()
        td_end_time = time.time()
        print("Incremental load for teamsData tables completed in {:.0f} seconds".format(td_end_time - td_start_time))

        #RostersData
        rd_start_time = time.time()
        self.rosters_data.incremental_load()
        rd_end_time = time.time()
        print("Incremental load for rostersData tables completed in {:.0f} seconds".format(rd_end_time - rd_start_time))

        # PlayersData
        pd_start_time = time.time()
        self.players_data.incremental_load()
        pd_end_time = time.time()
        print("Incremental load for playersData tables completed in {:.0f} seconds".format(pd_end_time - pd_start_time))

        #GroupsData
        gd_start_time = time.time()
        self.groups_data.incremental_load()
        gd_end_time = time.time()
        print("Incremental load for groupsData tables completed in {:.0f} seconds".format(gd_end_time - gd_start_time))

        #MinMaxData
        mm_start_time = time.time()
        self.min_max_data.incremental_load()
        mm_end_time = time.time()
        print("Incremental load for minMaxData tables completed in {:.0f} seconds".format(mm_end_time - mm_start_time))

        #ShotsData
        sd_start_time = time.time()
        self.shots_data.incremental_load()
        sd_end_time = time.time()
        print("Incremental load for shotsData tables completed in {:.0f} seconds".format(sd_end_time - sd_start_time))

        #StatisticsData
        stat_start_time = time.time()
        self.statistics_data.incremental_load()
        stat_end_time = time.time()
        print("Incremental load for statsData tables completed in {:.0f} seconds".format(stat_end_time - stat_start_time))

    def full_load(self):
        #DatesData
        dates_start_time = time.time()
        self.dates_data.full_load()
        dates_end_time = time.time()
        print("Full load for datesData tables completed in {:.0f} seconds".format(dates_end_time-dates_start_time))

        #TeamsData
        teams_start_time = time.time()
        self.teams_data.full_load()
        teams_end_time = time.time()
        print("Full load for teamsData tables completed in {:.0f} seconds".format(teams_end_time - teams_start_time))

        #PlayersData
        pd_start_time = time.time()
        self.players_data.full_load()
        pd_end_time = time.time()
        print("Full load for playersData tables completed in {:.0f} seconds".format(pd_end_time - pd_start_time))

        #GroupsData
        groups_start_time = time.time()
        self.groups_data.full_load()
        groups_end_time = time.time()
        print("Full load for groupsData tables completed in {:.0f} seconds".format(groups_end_time - groups_start_time))

        #ShotsData
        shots_start_time = time.time()
        self.shots_data.full_load()
        shots_end_time = time.time()
        print("Full load for shotsData tables completed in {:.0f} seconds".format(shots_end_time - shots_start_time))

        #StatsData
        stats_start_time = time.time()
        self.statistics_data.full_load()
        stats_end_time = time.time()
        print("Full load for statsData tables completed in {:.0f} seconds".format(stats_end_time - stats_start_time))

        #MinMaxData
        min_max_start_time = time.time()
        self.min_max_data.full_load()
        min_max_end_time = time.time()
        print("Full load for minMaxData tables completed in {:.0f} seconds".
              format(min_max_end_time - min_max_start_time))

        #RostersData
        rosters_start_time = time.time()
        self.rosters_data.full_load()
        rosters_end_time = time.time()
        print("Full load for rostersData tables completed in {:.0f} seconds".
              format(rosters_end_time - rosters_start_time))
