import pyodbc

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=landingdb;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
leagues = ['EPL', 'RFPL', 'Bundesliga', 'LaLiga', 'Ligue1', 'SerieA']
table_name = ['[landing_teams_statAttackSpeedData]', '[landing_teams_statFormationData]', '[landing_teams_statGamePlayData]',
                      '[landing_teams_statGameStateData]', '[landing_teams_statShotTypeData]', '[landing_teams_statShotZoneData]',
                      '[landing_teams_statTimingData]']
for leag in leagues:
    for table in table_name:
        league = leag.lower()
        try:
            query = f"ALTER TABLE [landingdb].[{league}].{table} ADD [LastUpdated] datetime DEFAULT GETUTCDATE()"
            cursor.execute(query)
            cursor.commit()
        except:
            print(league + table)