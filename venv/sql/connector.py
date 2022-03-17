import pyodbc


class Connection:

    def __init__(self,db, driver = 'SQL Server', server = '.', tc = 'yes'):
        self.conn = pyodbc.connect('Driver={%s};'
                      'Server=%s;'
                      'Database=%s;'
                      'Trusted_Connection=%s;' %(driver, server, db, tc))
        self.cursor = self.conn.cursor()




