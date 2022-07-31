import pyodbc
from sqlalchemy import create_engine



class Connection:

    def __init__(self,db, driver = 'SQL Server', server = '.', tc = 'yes'):
        self.conn = pyodbc.connect('Driver={%s};'
                      'Server=%s;'
                      'Database=%s;'
                      'Trusted_Connection=%s;' %(driver, server, db, tc))
        self.cursor = self.conn.cursor()
        self.driver = driver
        self.database = db

    def create_alchemy_engine(self):
        alchemy_connection = create_engine('mssql+pyodbc://LAPTOP-8V6UPAAI/landingdb?driver=SQL+Server')
        return alchemy_connection




