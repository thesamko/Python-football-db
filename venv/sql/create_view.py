from sql import connector

cursor = connector.Connection('landingdb')

leagues = ['RFPL', 'EPL', 'Bundesliga', 'LaLiga', 'Ligue1', 'SerieA']
for leag in leagues:
    league = leag.lower()

    cursor.execute(f'''CREATE VIEW {league}.factTeamTiming AS (
SELECT [team_id]
      ,[timing]
      ,[shots_for]
      ,[goals_for]
      ,[xG_for]
      ,[shots_against]
      ,[goals_against]
      ,[xG_against]
      ,[SEASON]
  FROM [landingdb].[{league}].[landing_teams_statTimingData]
)
''')
    cursor.commit()