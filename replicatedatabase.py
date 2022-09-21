from .base_model import BaseModel

class ReplicateDatabase(BaseModel):
    def __init__(self, schemas:dict, schemas_for_queries:dict, properties:dict):
        if type(schemas) != dict:
            raise TypeError('Schemas must be dict.')
        else:
            self.schemas = schemas
        
        self.schemas_for_queries = schemas_for_queries
        self.properties = properties
        BaseModel.__init__(self)


    def create_schemas(self):
        for schema in self.schemas:
            schemaquery = f'''
                create schema if not exists {schema}
            '''
            spark.sql(schemaquery)


    def create_tables(self, format:str='delta', partitions:dict = {}):
        for schema in self.schemas:
            for table in self.schemas[schema]:
                syntax = []

                for column, info in self.schemas[schema][table].items():
                    syntax.append(' '.join([column,info]))
                
                structure = ', '.join(syntax)

                if schema in partitions and table in partitions[schema]:
                    createquery = f'''create table if not exists {schema}.{table} ({structure})
                        partitioned by ({partitions[schema][table]})'''
                else:
                    createquery = f'''create table if not exists {schema}.{table} ({structure})
                        using {format}'''
                
                spark.sql(createquery)


    def reset_databricks(self, is_sure:bool=False):
        if is_sure:
            for schema in self.schemas:
                dropquery = f'drop schema {schema} cascade'

                spark.sql(dropquery)


    def finish_migration(self):
        if len(self.properties['mount']) < 1:
            raise Exception('Must provide a mount or target directory')

        for schema in self.schemas:
            if self.properties['mount'][-1] != '/':
                destination = self.properties['mount']+'/'+schema
            else:
                destination = self.properties['mount']+schema

            schema = schema.lower()
            schema_path = 'user/hive/warehouse/'+schema+'.db'

            dbutils.fs.cp(schema_path, destination, recurse=True)


    def query_and_insert_data(self):
        for schema in self.schemas_for_queries:
            for table in self.schemas_for_queries[schema]:
                columns_to_query = ', '.join(self.schemas_for_queries[schema][table].keys())

                query = f'''(select {columns_to_query}
                    from {schema}.{table}) query
                '''

                data = BaseModel.get_dataframe(self, query)

                data.write.insertInto(f'{schema}.{table}', overwrite=True)