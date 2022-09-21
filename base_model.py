
class BaseModel:
    def __init__(self):
        self.host = self.properties['hostname']
        self.port = self.properties['port']
        self.database = self.properties['database']
        self.url = f'jdbc:sqlserver://{self.host}:{self.port};databasename={self.database}'
        #self.url = f'jdbc:sqlserver://{self.host}:{self.port};databasename={self.database};user={self.username};password={self.password}'


    'The queries must be like this: (select columns from table where...) alias'
    def get_dataframe(self, query:str):
        self.credentials = {
            'username': self.properties['username'],
            'password': self.properties['password']
        }
        return spark.read.jdbc(url=self.url, table=query, properties=self.credentials)