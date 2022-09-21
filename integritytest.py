from .base_model import BaseModel
import random, json
from math import *

class Unitest(BaseModel):
    def __init__(self, schemas_for_queries:dict, schemas:dict, limit:int, properties:dict):
        self.schemas = schemas
        self.schemas_for_queries = schemas_for_queries
        self.properties = properties
        self.limit = limit/100
        BaseModel.__init__(self)


    def set_schemas_limit(self):
        if len(self.schemas.keys()) < 2:
            schemas_limit = 1
            return schemas_limit
        
        if len(self.schemas.keys()) > 1:
            schemas_limit = ceil(len(self.schemas.keys()) * self.limit)
            return schemas_limit


    def set_tables_limit(self, schema):
        if len(self.schemas[schema]) < 2:
            tables_limit = 1
            return tables_limit

        if len(self.schemas[schema].keys()) > 1:
            tables_limit = ceil(len(self.schemas[schema]) * self.limit)
            return tables_limit


    def get_random_schemas(self):
        self.test_schemas = {}
        schemas_limit = self.set_schemas_limit()

        for i in range(schemas_limit):
            chosen_schema = random.choice(list(self.schemas_for_queries.keys()))

            while chosen_schema in self.test_schemas:
                chosen_schema = random.choice(list(self.schemas_for_queries.keys()))

            self.test_schemas[chosen_schema] = {}


    def get_random_tables(self):
        for schema in self.test_schemas.keys():
            tables_limit = self.set_tables_limit(schema)

            for i in range(tables_limit):
                chosen_table = random.choice(list(self.schemas_for_queries[schema].keys()))

                while chosen_table in self.test_schemas[schema]:
                    chosen_table = random.choice(list(self.schemas_for_queries[schema].keys()))

                self.test_schemas[schema][chosen_table.lower()] = chosen_table


    def get_data_in_source(self):
        self.data_in_source = {}

        for schema in self.test_schemas:
            self.data_in_source[schema] = {}

            for table, og_table in self.test_schemas[schema].items():

                columns_to_query = ', '.join(self.schemas_for_queries[schema][og_table].keys())

                query = f'(select {columns_to_query} from {schema}.{table}) query'

                data = BaseModel.get_dataframe(self, query)

                self.data_in_source[schema][table] = data.count()


    def get_data_in_destination(self):
        self.data_in_destination = {}

        for schema in self.data_in_source:
            self.data_in_destination[schema] = {}

            for table in self.data_in_source[schema]:

                path = mount+f'{schema}/{table}'
                data = spark.read.format('delta').load(path)

                self.data_in_destination[schema][table] = data.count()


    def show_integrity(self):
        self.count_destination = 0
        self.count_source = 0

        for schema in self.data_in_destination:
            for table, value in self.data_in_destination[schema].items():
                print(f'Se encontraron {value} registros para la tabla {schema}.{table} de {self.data_in_source[schema][table]} en el origen.')

                if value != self.data_in_source[schema][table]:
                    missing = abs(value - self.data_in_source)
                    print(f'\nHay {missing} registros que no se copiaron.')

                self.count_destination += self.data_in_destination[schema][table]
                self.count_source += self.data_in_source[schema][table]

        print(f'Se encontraron {self.count_destination} registros copiados de {self.count_source} presentes en origen.')


    def show_progress_schemas(schemas):
        count_schemas = len(schemas.keys())

        print(f'Se identificaron {count_schemas} schemas.')
        print(list(schemas.keys()), '\n')


    def show_progress_tables(schemas):
        count_tables = 0

        for schema in schemas:
            count_tables += len(schemas[schema].keys())
        
        print(f'Se identificaron {count_tables} tablas.')

        for schema in schemas:
            print(schema, ':\n', list(schemas[schema].keys()), '\n')


    def pass_data(self):
        dbutils.notebook.exit(
            json.dumps(
                {
                    'source_data': self.data_in_source,
                    'destination_data': self.data_in_destination,
                }
            )
        )