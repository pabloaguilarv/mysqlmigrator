from .base_model import BaseModel
from .integritytest import Unitest
from .replicatedatabase import ReplicateDatabase

class QueryDatabase(ReplicateDatabase, Unitest, BaseModel):

    def __init__(self, properties:dict) -> None:
        self.properties = properties
        BaseModel.__init__(self)
    
    
    'create schemas dict, with the following format.'
    '''format:
        schemas = {
            'schema1': {
                'table1': {
                    'column1': datatype+constraints,
                    'column2': datatype+constraints,
                    'column3': datatype+constraints,
                },
                'table2': {
                    'column1': datatype+constraints,
                    'column2': datatype+constraints,
                    'column3': datatype+constraints,
                },
                ...
            },
            'schema2': {
                'table1': {
                    'column1': datatype+constraints,
                    'column2': datatype+constraints,
                    'column3': datatype+constraints,
                },
                'table2': {
                    'column1': datatype+constraints,
                    'column2': datatype+constraints,
                    'column3': datatype+constraints,
                },
                ...
            },
            etc...
        }
    '''
    def get_schemas(self, needed_schemas:tuple = (), condition:str = 'in') -> None:
        """
        > This function retrieves all the schemas from the database and stores them in a dictionary
        
        :param needed_schemas: tuple = ()
        :type needed_schemas: tuple
        :param condition: str = 'in', defaults to in
        :type condition: str (optional)
        """
        if len(needed_schemas) < 1:
            initquery = '''(
                select distinct table_schema
                from information_schema.tables
            ) initquery'''
        else:
            if type(needed_schemas)!=str:
                initquery = f'''(
                    select distinct table_schema
                    from information_schema.tables
                    where table_schema {condition} {needed_schemas}
                ) initquery'''
            else:
                initquery = f'''(
                    select distinct table_schema
                    from information_schema.tables
                    where table_schema {condition} '{needed_schemas}'
                ) initquery'''
        
        schemas_df = BaseModel.get_dataframe(self, initquery)
        
        self.schemas_for_queries = {schema[0]: {} for schema in schemas_df.collect()}
        self.schemas = {schema[0]: {} for schema in schemas_df.collect()}

        Unitest.show_progress_schemas(self.schemas); 'Shows how many schemas were retrieved.'


    def get_empty_tables(self) -> None:
        """
        It gets the empty tables from the database and stores them in a dictionary.
        """
        empty_query = f'''(
            select t.name table_name,
                s.name schema_name
            from sys.tables t
            join sys.schemas s
                on (t.schema_id = s.schema_id)
            join sys.partitions p
                on (t.object_id = p.object_id)
            where p.index_id in (0,1)
            group by t.name,s.name
            having sum(p.rows) = 0
        ) emptyquery'''

        empty_tables_df = BaseModel.get_dataframe(self, empty_query)

        '{schema: [tables]}'
        '{data[1]: [data[0]]} orden de acuerdo al query.'
        self.schemas_empty_tables = {data[1]: {} for data in empty_tables_df.collect() if data[1] in self.schemas}
        'Si se ignoró un schema al escanear la base de datos, acá se ignorará también con el if.'

        if len(self.schemas_empty_tables) > 0:
            for data in empty_tables_df.collect():
                self.schemas_empty_tables[data[1]][data[0]] = None

        print('Tablas vacías:')
        Unitest.show_progress_tables(self.schemas_empty_tables)
    
    
    def disc_empty_tables(self) -> None:
        if len(self.schemas_empty_tables) > 0:
            for schema in self.schemas_empty_tables:
                for table in self.schemas_empty_tables[schema]:

                    self.schemas_for_queries[schema].pop(table)
                    self.schemas[schema].pop(table)


    '''Here we specify which tables are needed. The method does not support the case when
    more than one schema shares the same name for a table but that table is not required in one or more of the schemas.'''
    def get_tables(self, needed_tables:tuple = (), condition:str = 'in') -> None:
        """
        It gets all the tables in the database and stores them in a dictionary
        
        :param needed_tables: tuple = ()
        :type needed_tables: tuple
        :param condition: str = 'in', defaults to in
        :type condition: str (optional)
        """
        for schema in self.schemas:
            if len(needed_tables) < 1:
                tablequery = f'''(
                    select table_name
                    from information_schema.tables
                    where table_schema = '{schema}'
                ) tquery'''
            else:
                if type(needed_tables)!=str:
                    tablequery = f'''(
                        select table_name
                        from information_schema.tables
                        where table_schema = '{schema}'
                        and table_name {condition} {needed_tables}
                    ) tquery'''
                else:
                    tablequery = f'''(
                        select table_name
                        from information_schema.tables
                        where table_schema = '{schema}'
                        and table_name {condition} '{needed_tables}'
                    ) tquery'''

            tables_df = BaseModel.get_dataframe(self, tablequery)
            
            self.schemas_for_queries[schema] = {table[0]: {} for table in tables_df.collect()}
            self.schemas[schema] = {table[0]: {} for table in tables_df.collect()}

        Unitest.show_progress_tables(self.schemas); 'Shows retrieved tables.'


    'Populates values of the table key, value pair. The dict containing the columns and info is the value'
    def get_columns_info(self, unnecesary_dtypes:tuple=(), condition:str = 'not in') -> None:
        for schema in self.schemas:
            for table in self.schemas[schema]:
                if len(unnecesary_dtypes) < 1:
                    colquery = f'''(
                        select column_name,
                            data_type,
                            case is_nullable
                                when 'NO'
                                    then 'not null'
                            end as is_null,
                            character_maximum_length,
                            numeric_precision,
                            numeric_scale
                        from information_schema.columns
                        where table_name = '{table}'
                        and table_schema = '{schema}'
                    ) colquery'''
                else:
                    if type(unnecesary_dtypes)!=str:
                        colquery = f'''(
                            select column_name,
                                data_type,
                                case is_nullable
                                    when 'NO'
                                        then 'not null'
                                end as is_null,
                                character_maximum_length,
                                numeric_precision,
                                numeric_scale
                            from information_schema.columns
                            where table_name = '{table}'
                            and table_schema = '{schema}'
                            and data_type {condition} {unnecesary_dtypes}
                        ) colquery'''
                    else:
                        colquery = f'''(
                            select column_name,
                                data_type,
                                case is_nullable
                                    when 'NO'
                                        then 'not null'
                                end as is_null,
                                character_maximum_length,
                                numeric_precision,
                                numeric_scale
                            from information_schema.columns
                            where table_name = '{table}'
                            and table_schema = '{schema}'
                            and data_type {condition} '{unnecesary_dtypes}'
                        ) colquery'''

                columns_info_df = BaseModel.get_dataframe(self, colquery)
                
                self.schemas_for_queries[schema][table] = {data[0]: list(map(str,data[1:])) for data in columns_info_df.collect()}
                self.schemas[schema][table] = {self.organize_column_name(data[0]): list(map(str,data[1:])) for data in columns_info_df.collect()}

    
    def organize_column_name(self, column_name:str, replacements:dict = {}):
        """
        It takes a string and a dictionary as arguments, and returns a string
        
        :param column_name: The name of the column to be organized
        :type column_name: str
        :param replacements: a dictionary of characters to replace
        :type replacements: dict
        :return: the column name.
        """
        column_name = column_name.lower()

        try:
            if type(int(column_name)) == int:
                return f'C_{column_name}'
        except:
            if len(replacements) < 1 or type(replacements) != dict:
                replacements = {
                    'á': 'a',
                    'é': 'e',
                    'í': 'i',
                    'ó': 'o',
                    'ú': 'u',
                    'ä': 'a',
                    'ë': 'e',
                    'ï': 'i',
                    'ö': 'o',
                    'ü': 'u',
                    'ñ': 'n'
                }

            corrected = column_name
            for k, v in replacements.items():
                if k in corrected:
                    corrected = corrected.replace(k, v)
            
            if corrected == column_name:
                return column_name
            return corrected


    def organize_column_info(self):
        """
        It takes the information from the SQL Server database and organizes it into a format that can be
        used to create the tables in the PostgreSQL database
        """
        'dict structure:'
        ''' self.schemas[schema][table]={
            column_name:
            info[0] = data_type,
            info[1] = is_nullable,
            info[2] = character_maximum_length,
            info[3] = numeric_precision,
            info[4] = numeric_scale
        }'''

        'numeric and decimal need precision and scale'
        'varbinary needs one position'

        'DB Homologues are added'

        for schema in self.schemas:
            for table in self.schemas[schema]:
                for column, info in self.schemas[schema][table].items():

                    'dtype(max_length) not null'
                    if info[0] in ['char', 'varchar', 'nvarchar'] and info[1] == 'not null':
                        self.schemas[schema][table][column] = 'string not null'
                    
                    'dtype(max_length)'
                    if info[0] in ['char', 'varchar', 'nvarchar'] and info[1] != 'not null':
                        self.schemas[schema][table][column] = 'string'

                    'dtype not null'
                    if info[0] in ['bit', 'bigint', 'tinyint', 'smallint', 'date', 'datetime', 'float', 'int', 'varbinary'] and info[1] == 'not null':
                        if info[0] == 'bit':
                            self.schemas[schema][table][column] = 'boolean not null'
                        elif info[0] == 'datetime':
                            self.schemas[schema][table][column] = 'timestamp not null'
                        elif info[0] == 'varbinary':
                            self.schemas[schema][table][column] = 'binary not null'
                        else:
                            self.schemas[schema][table][column] = info[0]+' not null'

                    'dtype'
                    if info[0] in ['bit', 'bigint', 'tinyint', 'smallint', 'date', 'datetime', 'float', 'int', 'varbinary'] and info[1] != 'not null':
                        if info[0] == 'bit':
                            self.schemas[schema][table][column] = 'boolean'
                        elif info[0] == 'datetime':
                            self.schemas[schema][table][column] = 'timestamp'
                        elif info[0] == 'varbinary':
                            self.schemas[schema][table][column] = 'binary'
                        else:
                            self.schemas[schema][table][column] = info[0]
                    
                    'dtype(precision,scale) not null'
                    if info[0] in ['numeric', 'decimal'] and info[1] == 'not null':
                        self.schemas[schema][table][column] = info[0]+'('+info[3]+','+info[4]+')'+' not null'
                    
                    'dtype(precision,scale)'
                    if info[0] in ['numeric', 'decimal'] and info[1] != 'not null':
                        self.schemas[schema][table][column] = info[0]+'('+info[3]+','+info[4]+')'


    def check_new_schemas(self):
        """
        It checks if there are new schemas in the mount point and if there are, it removes them from the
        schemas dictionary
        :return: The number of new schemas found.
        """
        self.stored_schemas = [file.name[:-1] for file in dbutils.fs.ls(self.properties['mount'])]

        for schema in self.stored_schemas:
            self.schemas.pop(schema)
            self.schemas_for_queries.pop(schema)

        return print(f'Se encontraron {len(self.schemas)} schemas nuevos.')


    def check_new_tables(self):
        """
        It checks if there are new tables in the schema and removes them from the dictionary
        """
        for schema in self.schemas_for_queries:
            if self.properties['mount'][-1] != '/':
                path = self.properties['mount']+'/'+schema
            else:
                path = self.properties['mount']+schema
            
            stored_tables = [table[:-1] for table in dbutils.fs.ls(path)]

            for table in stored_tables:
                self.schemas_for_queries[schema].pop(table)
                self.schemas[schema].pop(table)

            print(f'Se encontraron {len(self.schemas[schema])} tablas nuevas en el schema {schema}.')

