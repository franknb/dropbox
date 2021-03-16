import csv
from os import path
import json
import glob

# items to update
SourceName = 'Liveramp_Inbound'
data_dictionary_path = 'PaidSearch/RawSheets/*.csv'
table_dictionary_path = 'PaidSearch/TableDictionary.csv'

WriteStoredProcedure = False
WriteTables = False
WriteS3Metadata = False
WriteDBMetadata = True
WriteDBViews = False


# automated code
basepath = path.dirname(__file__)
DatabaseFilePath = path.abspath(path.join(basepath, "..", "database/"+SourceName+'/'))
S3MetadataFilePath = path.abspath(path.join(basepath, "..", "ETL-Pipeline/"+SourceName+'/Metadata/'))
DBMetadataFilePath = path.abspath(path.join(basepath, "..", "database/"+SourceName+'/Metadata/'))


ColumnTypeMap = {
    'int': {'RS DataType': 'bigint', 'DataType': 'bigint', 'ETL DataType': 'int'}
}


if WriteStoredProcedure:
    Master_SQL_File = open(DatabaseFilePath+'/Stored Procedures/'+SourceName+'_Procedures.sql', 'w')

if WriteTables:
    Master_StageTable_SQL_File = open(DatabaseFilePath+'/Stage Tables/'+SourceName+'_Master.sql', 'w')
    Master_TargetTable_SQL_File = open(DatabaseFilePath+'/Target Tables/'+SourceName+'_Master.sql', 'w')


if WriteDBViews:
    Master_View_SQL_File = open(DatabaseFilePath+'/Views/'+SourceName+'_Master.sql', 'w')

DBMetadata = []


col_list = ['Target Column Name', 'Source Column Name', 'DataType', 'Natural ID', 'Column Ordinal', 'Cleansing Rule', 'Transformation']

Results = []

Layers = [
    {'LayerName': 'Stage Table', 'storage_layer': 'stage', 'Target Schema Name': 'stage','Source Schema Name': ''},
    {'LayerName': 'Cleansed Table', 'storage_layer': 'cleansed', 'Target Schema Name': 'cleansed', 'Source Schema Name': 'stage'},
    {'LayerName': 'RS Stage Table', 'Target Schema Name': 'ca_stg', 'Source Schema Name': 'cleansed'},
    {'LayerName': 'RS Target Table', 'Target Schema Name': 'ca', 'Source Schema Name': 'RS Stage Table'},

]


TransformMap = {
    'Column Ordinal': 1
}

TableDict = {}
data_dictionary_path

DataDict_CSV = glob.glob(data_dictionary_path) # updated to a dynamic path


with open(table_dictionary_path, 'r') as f:
    count = 0
    csvreader = csv.reader(f, delimiter=",")
    for Line in csvreader:

        if count == 0:
            headerRow = Line

        else:
            dataRow = Line

            DataObject = {}

            CellCount = 0
            for Cell in dataRow:
                HeaderCell = headerRow[CellCount]
                if HeaderCell in TransformMap:
                    Cell = type(TransformMap[HeaderCell])(Cell)

                DataObject[HeaderCell] = Cell

                CellCount +=1

            for HeaderCell in ['Stage Table',	'Cleansed Table',	'RS Stage Table',	'RS Target Table']:
                TableName = DataObject[HeaderCell]
                ObjectName = f'{HeaderCell} - {TableName}'


                TableDict[ObjectName] = {
                                  'ColumnList': [],
                                  'Keys': [],
                                  'SortKeys': [],
                                  'DistributionKeys': [],
                                  'Table Name': TableName,
                                  'Table Classification': DataObject['Type'],
                                  'sequence': DataObject['Sequence'],
                                  'Analyze Statement Stage Table': DataObject['Analyze Statement Stage Table'],
                                  'Target Table Load Strategy': DataObject['Target Table Load Strategy'],
                                  'Analyze Statement Target Table': DataObject['Analyze Statement Target Table'],
                                  'Target table vaccum style': DataObject['Target table vaccum style'],
                                  'Staging Table Vaccum Style': DataObject['Staging Table Vaccum Style'],
                                  'compressed': 'false',
                                  "file_format": "parquet",
                                  "parameters": "{\"classification\":\"parquet\"}",
                                  "serde_lib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                  "serde_params": {},
                                  "PartitionList": [],
                                  "storage_layer": "cleansed",
                                  "source_name": SourceName,
                                  "input_table": TableName,
                                  "input_path": "s3://lly-tsp-etl-dev/stage/trulicity/"+SourceName,
                                  "output_path": "s3://lly-tsp-agr-dev/landing/trulicity/"+SourceName+"/"+TableName,
                                  "metadata_path": "s3://lly-tsp-etl-dev/metadata/cleansed/processed/"+SourceName+"/"+TableName+".json",
                        }


        count +=1

#print(json.dumps(TableDict, indent=2))

TDKeys = [k for k in TableDict]
for CsvFileName in DataDict_CSV:
    #print(CsvFileName, 'CsvFileName')
    with open(CsvFileName, 'r') as f:
        count = 0
        csvreader = csv.reader(f, delimiter=",")
        for Line in csvreader:

            if count == 0:
                headerRow = Line

            else:
                dataRow = Line
                #print(Line)
                DataObject = {}
                CellCount = 0
                for Cell in dataRow:
                    HeaderCell = headerRow[CellCount]
                    if HeaderCell in TransformMap:
                        Cell = type(TransformMap[HeaderCell])(Cell)

                    DataObject[HeaderCell] = Cell
                    CellCount +=1


                LayerName = ''

                for L in Layers:
                    if DataObject['Target Schema'] == L['Target Schema Name']:
                        LayerName = L['LayerName']
                #print(DataObject)
                TableName = f'{LayerName} - {DataObject["Target Table"]}'
                #print('TableNamwe', TableName)

                DataObject['RS DataType'] = DataObject['Data Type']
                DataObject['ETL DataType'] = DataObject['Data Type']

                if DataObject['Data Type'] == 'varchar':
                    Length = DataObject['Length']
                    DataObject['Data Type'] = f'varchar({Length})'
                    DataObject['ETL DataType'] = 'string'
                elif DataObject['Data Type'].lower() == 'int':
                    DataObject['Data Type'] = 'bigint'
                    DataObject['RS DataType'] = 'bigint'
                    DataObject['ETL DataType'] = 'int'
                elif DataObject['Data Type'].lower() == 'double':
                    DataObject['Data Type'] = 'double precision'
                    DataObject['RS DataType'] = 'double'
                    DataObject['ETL DataType'] = 'double'



                if TableName in TableDict:
                    #print(CsvFileName, json.dumps(DataObject, indent=2))
                    #if DataObject['Target Field Name'] not in [DO['Target Field Name'] for DO in TableDict[TableName]['ColumnList']]:
                    TableDict[TableName]['ColumnList'].append(DataObject)
                else:

                    TableDict[TableName] = {
                                      'ColumnList': [DataObject],
                                      'Keys': [],
                                      'SortKeys': [],
                                      'DistributionKeys': [],
                                      'Table Name': TableName,
                                      'Table Classification': None,
                                      'sequence': 10001,
                                      'Analyze Statement Stage Table': None,
                                      'Target Table Load Strategy': None,
                                      'Analyze Statement Target Table': None,
                                      'Target table vaccum style': None,
                                      'Staging Table Vaccum Style': None,
                                      'compressed': 'false',
                                      "file_format": "parquet",
                                      "parameters": "{\"classification\":\"parquet\"}",
                                      "serde_lib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                      "serde_params": {},
                                      "PartitionList": [],
                                      "storage_layer": "cleansed",
                                      "source_name": SourceName,
                                      "input_table": TableName,
                                      "input_path": "s3://lly-tsp-etl-dev/stage/trulicity/"+SourceName,
                                      "output_path": "s3://lly-tsp-agr-dev/landing/trulicity/"+SourceName+"/"+TableName,
                                      "metadata_path": "s3://lly-tsp-etl-dev/metadata/cleansed/processed/"+SourceName+"/"+TableName+".json",
                            }
                    #print('-----------------------')
                    #print('COULD NOT FIND TABLE NAME', TableName)
                    #RS Target Table - gps_searchquerystats

            count +=1




print(json.dumps(TableDict['RS Target Table - gps_accountbasicstats'], indent=2))




for Table in TableDict:

    Columns = sorted(TableDict[Table]['ColumnList'], key=lambda c: int(c['Column Ordinal']) )
    Keys = TableDict[Table]['Keys']
    SortKeys = TableDict[Table]['SortKeys']
    DistributionKeys = TableDict[Table]['DistributionKeys']



    CreateMetadataForTable = False

    DataObject = TableDict[Table]

    if 'RS Stage Table' not in Table and 'RS Target Table' not in Table:
        CreateMetadataForTable = True
    ObjectName = Table
    TableName = Table.replace('RS Stage Table - ', '').replace('RS Target Table - ', '').replace('Cleansed Table - ', '').replace('Stage Table - ', '')

    for Column in Columns:
        #print('Target Column Name', Column)
        if (Column['PrimaryKey'] == True or len(Column['PrimaryKey']) > 0) and Column['Target Field Name'] not in Keys:
            Keys.append(Column['Target Field Name'])

        if (Column['DistributionKey'] == True or len(Column['DistributionKey']) > 0) and Column['Target Field Name'] not in DistributionKeys :
            DistributionKeys.append(Column['Target Field Name'])

        if (Column['SortKey'] == True or len(Column['SortKey']) > 0) and Column['Target Field Name'] not in SortKeys :
            SortKeys.append(Column['Target Field Name'])

        #print('Column', Column)
        Column['RS DataType'] = Column['Data Type']
        Column['ETL DataType'] = Column['Data Type']

        Column['Identity Definition'] = ' IDENTITY(1,1)' if Column['Identity Column'] == 'Y' else ''


        Column['InsertValue'] = Column['Target Field Name']
        Column['WhereValue'] = Column['Target Field Name']


        if len(Column['Transformation']) > 0:
            if Column['Transformation'].lower() == 'convert to date':
                Column['InsertValue'] = f"to_date({Column['Target Field Name']},'YYYY-MM-DD')"
                Column['WhereValue'] = f"to_date(ca_stg.{Column['Source Table']}.{Column['Target Field Name']},'YYYY-MM-DD')"

        if (Column['PrimaryKey'] == True or len(Column['PrimaryKey']) > 0):
            if 'varchar' in Column['Data Type'].lower():
                Column['InsertValue'] = f"COALESCE({Column['Target Field Name']}, 'NA')"
                Column['WhereValue'] = f"COALESCE(ca_stg.{Column['Source Table']}.{Column['Target Field Name']}, 'NA')"

            elif 'int' in Column['Data Type'].lower():
                Column['InsertValue'] = f"COALESCE({Column['Target Field Name']}, 0)"
                Column['WhereValue'] = f"COALESCE(ca_stg.{Column['Source Table']}.{Column['Target Field Name']}, 0)"
            elif 'double' in Column['Data Type'].lower():
                Column['InsertValue'] = f"COALESCE({Column['Target Field Name']}, 0.0)"
                Column['WhereValue'] = f"COALESCE(ca_stg.{Column['Source Table']}.{Column['Target Field Name']}, 0.0)"
            elif 'bool' in Column['Data Type'].lower():
                Column['InsertValue'] = f"COALESCE({Column['Target Field Name']}, True)"
                Column['WhereValue'] = f"COALESCE(ca_stg.{Column['Source Table']}.{Column['Target Field Name']}, True)"

        if len(Column['Default Values']) > 0:
            if Column['Default Values'] == 'sysdate':
                Column['InsertValue'] = 'sysdate'
                Column['WhereValue'] = 'sysdate'
            else:
                Column['InsertValue'] = f"'{Column['Default Values']}'"
                Column['WhereValue'] = f"'{Column['Default Values']}'"

    IDColumn = 'ID'

    StgTableName = TableName

    if WriteStoredProcedure:
        if 'RS Target Table' in ObjectName:
            SourceTableName = list(set([c['Source Table'] for c in Columns if len(c['Source Table']) > 0]))

            if len(SourceTableName) > 1:
                print('Table', Table, SourceTableName )

            for StageTableName in SourceTableName:
                GPS_Source = ''
                if StageTableName.endswith('_dtc_hcp'):
                    GPS_Source = 'dtc_hcp'
                elif StageTableName.endswith('_hispanic_dtc'):
                    GPS_Source = 'hispanic_dtc'
                elif StageTableName.endswith('_medical'):
                    GPS_Source = 'medical'
                elif StageTableName.endswith('_hcp'):
                    GPS_Source = 'hcp'

                ProcedureName = 'sp_'+StageTableName

                StageTableColumns = [C for C in Columns if (C['Source Table'] == StageTableName or C['Source Table'] == '') and not len(C['Identity Column']) > 0 and (C['Target Field Name'] != 'source' or C['WhereValue'] == f"'{GPS_Source}'")]
                StageTableColumnNames = [C['Target Field Name'] for C in Columns if (C['Source Table'] == StageTableName or C['Source Table'] == '') and not len(C['Identity Column']) > 0]

                StageTableKeys = [K for K in StageTableColumns if K['Target Field Name'] in Keys]

                SQL_File = open(DatabaseFilePath+'/Stored Procedures/'+ProcedureName+'.sql', 'w')
                DeleteString = ''
                InsertString = ''

                KeyStatement = '\n\t  AND '.join([f"ca.{TableName}.{C['Target Field Name']} = {C['WhereValue']}" for C in StageTableKeys])
                WhereStatement = '' if len(KeyStatement) == 0 else '\n\tWHERE {KeyStatement}'.format(KeyStatement=KeyStatement)


                DeleteString = f"DELETE FROM ca.{TableName}\
            \nWHERE EXISTS (\
            \n\tSELECT 'y'\
            \n\tFROM ca_stg.{StageTableName}\
            {WhereStatement}\
            \n\tLIMIT 1\
            \n)\
            \n;"



                StoredColumnList = []
                ColumnList = []

                for col in sorted(StageTableColumns, key=lambda c: c['Column Ordinal']):
                    if col['Target Field Name'] not in StoredColumnList:
                        StoredColumnList.append(col['Target Field Name'])
                        ColumnList.append({'Target Field Name': col['Target Field Name'], 'InsertValue':col['InsertValue'], 'Column Ordinal': col['Column Ordinal']})

                ValuesColumnList = '\t' + ',\n\t'.join([col['Target Field Name'] for col in ColumnList])
                InsertColumnList = '\t' + ',\n\t'.join([col['InsertValue'] for col in ColumnList])


                InsertString = f'INSERT INTO ca.{TableName} ( \
            \n{ValuesColumnList}\
            \n)\
            \nSELECT DISTINCT\
            \n{InsertColumnList}\
            \nFROM ca_stg.{StageTableName}\
            \n;'


                ProcedureString = f'CREATE OR REPLACE PROCEDURE ca.{ProcedureName}()'
                ProcedureString += '\nlanguage plpgsql'
                ProcedureString += '\nas'
                ProcedureString += '\n$$'
                ProcedureString += '\nDECLARE'
                ProcedureString += '\n\tprocess_id_calc       BIGINT;'
                ProcedureString += '\n\tprocess_start_dt_calc timestamp;'
                ProcedureString += '\n\tsource_count_calc     BIGINT;'
                ProcedureString += '\n\ttarget_count_calc     BIGINT;'
                ProcedureString += '\nBEGIN'
                ProcedureString += f"\n\tprocess_id_calc := (SELECT MAX(process_id)\n\t\tfrom ca.ca_processed_log\n\t\twhere source = '{SourceName}');"
                ProcedureString += '\n\tprocess_start_dt_calc := (select process_start_dt from ca.ca_processed_log where process_id = process_id_calc);'
                ProcedureString += f"\n\tsource_count_calc := (select count(*)\n\t\tfrom ca_stg.{StageTableName});"

                ProcedureString += f'\n\n{DeleteString}'
                ProcedureString += f'\n\n{InsertString}'

                ProcedureString += f"\n\n\ttarget_count_calc := (select count(*)\n\t\tfrom ca.{TableName}\n\t\twhere source = '{GPS_Source}'\n\t\tand load_date > process_start_dt_calc);"
                ProcedureString += "\n\n\tupdate ca.ca_processed_log\n\tset source_count=source_count_calc,\n\ttarget_count = target_count_calc,\n\trejected_count = 0,\n\t\tstatus ='Success',\n\t\tprocess_end_dt=sysdate\n\twhere process_id = process_id_calc;"

                ProcedureString += "\n\nEND ;\n$$;"


                SQL_File.write(ProcedureString)
                Master_SQL_File.write(ProcedureString)

                SQL_File.close()


    if WriteTables:


        StoredColumnList = []
        ColumnList = []

        for col in sorted(Columns, key=lambda c: c['Column Ordinal']):
            if col['Target Field Name'] not in StoredColumnList:
                StoredColumnList.append(col['Target Field Name'])
                ColumnList.append(col)


        PrimaryKeyStatement = ''
        if len(Keys) > 0:
            PrimaryKeyList = ','.join(Keys)
            PrimaryKeyStatement = f'\n\t, primary key ({PrimaryKeyList})'

        DistributionKeyStatement = ''
        print('Table Classification', TableDict[Table]['Table Classification'])
        if TableDict[Table]['Table Classification'] is None:
            if Table+'_hcp' in TableDict:
                TableDict[Table]['Table Classification'] = TableDict[Table+'_hcp']['Table Classification']
            elif Table+'_dtc_hcp' in TableDict:
                TableDict[Table]['Table Classification'] = TableDict[Table+'_dtc_hcp']['Table Classification']
            elif Table+'_hispanic_dtc' in TableDict:
                TableDict[Table]['Table Classification'] = TableDict[Table+'_hispanic_dtc']['Table Classification']
            elif Table+'_medical' in TableDict:
                TableDict[Table]['Table Classification'] = TableDict[Table+'_medical']['Table Classification']

        if len(DistributionKeys) > 0 and TableDict[Table]['Table Classification'].lower() == 'fact':
            DistributionKeyList = DistributionKeys[0]
            DistributionKeyStatement = f'\n\t distkey ({DistributionKeyList})'
        else:
            DistributionKeyStatement = f'\n\t diststyle all'
        SortKeyStatement = ''
        if len(SortKeys) > 0:
            SortKeyList = ','.join(SortKeys)
            SortKeyStatement = f'\n\tcompound sortkey ({SortKeyList})'

        ColumnListWithTypes = ',\n\t\t'.join([f'{C["Target Field Name"]}  {C["Data Type"]} encode zstd {C["Identity Definition"]}' for C in ColumnList])




        if 'RS Target Table' in ObjectName and len(ColumnList) > 0:
            DropString = f'DROP TABLE IF EXISTS ca.{TableName};\n\n'
            CreateString = f'CREATE TABLE IF NOT EXISTS ca.{TableName}\
        \n\t( \
        \n\t\t{ColumnListWithTypes} \
        {PrimaryKeyStatement}\
        \n\t)\
        {DistributionKeyStatement}\
        {SortKeyStatement}\
        \n;\n\n\n'
            TargetTable_SQL_File = open(DatabaseFilePath+'/Target Tables/ca.'+TableName+'.sql', 'w')

            TargetTable_SQL_File.write(DropString)
            TargetTable_SQL_File.write(CreateString)

            TargetTable_SQL_File.close()

            Master_TargetTable_SQL_File.write(DropString)
            Master_TargetTable_SQL_File.write(CreateString)

        elif 'RS Stage Table' in ObjectName:
            DropString = f'DROP TABLE IF EXISTS ca_stg.{TableName};\n\n'
            CreateString = f'CREATE TABLE IF NOT EXISTS ca_stg.{TableName}\
        \n\t( \
        \n\t\t{ColumnListWithTypes} \
        \n\t)\
        {DistributionKeyStatement}\
        {SortKeyStatement}\
        \n;\n\n\n'

            StageTable_SQL_File = open(DatabaseFilePath+'/Stage Tables/ca_stg.'+TableName+'.sql', 'w')

            StageTable_SQL_File.write(DropString)
            StageTable_SQL_File.write(CreateString)

            StageTable_SQL_File.close()

            Master_StageTable_SQL_File.write(DropString)
            Master_StageTable_SQL_File.write(CreateString)


    if WriteDBViews:

        StoredColumnList = []
        ColumnList = []

        for col in sorted(Columns, key=lambda c: c['Column Ordinal']):
            if col['Target Field Name'] not in StoredColumnList:
                StoredColumnList.append(col['Target Field Name'])
                ColumnList.append(col)

        ColumnListWithTypes = ',\n\t\t'.join([f'{C["Target Field Name"]}' for C in ColumnList if C["Target Field Name"] not in ['input_file_name', 'created_date', 'load_date']])

        if 'RS Target Table' in ObjectName and len(ColumnList) > 0:
            DropString = f'DROP VIEW IF EXISTS ca_view.dlv_{TableName};\n\n'
            CreateString = f'CREATE VIEW IF NOT EXISTS ca_view.dlv_{TableName}\
        \n\t( \
        \n\t\t{ColumnListWithTypes} \
        \n\t)\
        \n\tAS SELECT\
        \n{ColumnListWithTypes}\
        \nFROM ca.{TableName}\
        \nwith no schema binding;\
        \nGRANT SELECT ON ca_view.dlv_{TableName} TO GROUP g_ca_view_ro;\
        \nGRANT SELECT, INSERT, UPDATE, DELETE ON ca_view.dlv_{TableName} TO GROUP g_ca_view_rw;\
        \n\n'
            View_SQL_File = open(DatabaseFilePath+'/Views/ca_view.'+TableName+'.sql', 'w')

            View_SQL_File.write(DropString)
            View_SQL_File.write(CreateString)

            View_SQL_File.close()

            Master_View_SQL_File.write(DropString)
            Master_View_SQL_File.write(CreateString)



    if WriteS3Metadata:

        if 'RS Target Table' not in ObjectName and 'RS Stage Table' not in ObjectName:

            if 'Stage Table' in ObjectName:
                LayerName = 'stage'

            elif 'Cleansed Table' in ObjectName:
                LayerName = 'cleansed'

            S3Metadata_File = open(S3MetadataFilePath+'/'+LayerName+'/'+TableName+'.json', 'w')
            #print('S3Metadata_File', S3Metadata_File)
            FileMetadataObj = {   'Table Name': TableName,
                              'Table Classification': 'Dim',
                              'compressed': 'false',
                              "file_format": "parquet",
                              "parameters": "{\"classification\":\"parquet\"}",
                              "serde_lib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                              "serde_params": {},
                              "PartitionList": [],
                              "storage_layer": "cleansed",
                              "source_name": SourceName,
                              "input_table": TableName,
                              "input_path": "s3://lly-tsp-etl-dev/stage/trulicity/"+SourceName,
                              "output_path": "s3://lly-tsp-agr-dev/landing/trulicity/"+SourceName+"/"+TableName,
                              "metadata_path": "s3://lly-tsp-etl-dev/metadata/cleansed/processed/"+SourceName+"/"+TableName+".json",
                           }


            MetadataColumnList = []
            for Column in Columns:
                MetadataColumnList.append(Column)


            FileMetadataObj['ColumnList'] = MetadataColumnList

            json.dump(FileMetadataObj, S3Metadata_File, indent=2)

            S3Metadata_File.close()

    if WriteDBMetadata:
        if 'RS Target Table' in ObjectName:
            FilePrefix = TableName.replace('gsp_', '').replace('googlepaidsearch_', '')
            Classification = DataObject["Table Classification"]
            Sequence = DataObject["sequence"]
            StageVacuumStyle = DataObject["Staging Table Vaccum Style"]
            StageAnalyze = DataObject["Analyze Statement Stage Table"]
            TargetLoadStrat = DataObject["Target Table Load Strategy"]
            TargetVacuum = DataObject["Target table vaccum style"]
            TargetAnalyze = DataObject["Analyze Statement Target Table"]
            DBMetadataLine = f"\nSELECT '{SourceName}' as source, '{FilePrefix}' as file_prefix, '{Classification}' as file_type, 'csv' as file_extension, '.gz' as compression_format, CAST({Sequence} AS BIGINT) as order_id, 's3://lly-tsp-agr-dev/processed/trulicity/google_paid_search/lilly_trulicity_medical' as raw_location, 's3://lly-tsp-etl-dev/processed/trulicity/google_paid_search' as stg_s3_path, 'stage.{TableName}' as stg_catalog_tbl, 's3://lly-tsp-agr-dev/processed/trulicity/{SourceName}/{TableName}' as cleansed_s3_path, 'cleansed.{SourceName}_{TableName}' as cleansed_catalog_tbl, 'ca_stg.{SourceName}_{TableName}' as stg_tbl, '{StageVacuumStyle}' as stg_tbl_vacuum_style, '{StageAnalyze}' as stg_tbl_analyze_perc, '{TargetLoadStrat}' as tgt_tabl_load_strategy,'{TargetLoadStrat}' as tgt_tbl_load, 'ca.{SourceName}_{TableName}' as tgt_tbl, '{TargetVacuum}' as tgt_tbl_vacuum_style, '{TargetAnalyze}' as tgt_tbl_analyze_perc, '' as tgt_vw, 'Y' as active_flg, 'lly-tsp-etl-dev' as metadata_bkt ,'metadata/cleansed/processed/GooglePaidSearch/{TableName}.json' as metadata_key "

            DBMetadata.append(DBMetadataLine)


if WriteDBMetadata:

    LoadDetailsColumns = [
                            'source',
                            'file_prefix',
                            'file_type',
                            'file_extension',
                            'compression_format',
                            'order_id',
                            'raw_location',
                            'stg_s3_path',
                            'stg_catalog_tbl',
                            'cleansed_s3_path',
                            'cleansed_catalog_tbl',
                            'stg_tbl',
                            'stg_tbl_vacuum_style',
                            'stg_tbl_analyze_perc',
                            'tgt_tabl_load_strategy',
                            'tgt_tbl_load',
                            'tgt_tbl',
                            'tgt_tbl_vacuum_style',
                            'tgt_tbl_analyze_perc',
                            'tgt_vw',
                            'active_flg',
                            'metadata_bkt',
                            'metadata_key',
                            ]

    DBMetadata_SQL_File = open(DatabaseFilePath+'/Metadata/ngca_ca_ca_load_details.sql', 'w')



    DeleteStatement = f"DELETE FROM ca.ca_load_details WHERE source = '{SourceName}';\n\n"

    WithStatement = '\n UNION ALL'.join(DBMetadata)
    CloseStatement = '\n)SELECT '
    CloseStatement += ','.join(LoadDetailsColumns)
    CloseStatement += ' INTO TEMP TABLE metadata_data_temp FROM metadata_data;'

    CloseStatement=CloseStatement.replace('order_id', 'CAST(order_id AS BIGINT) as order_id')

    InsertStatement = '\n\nINSERT INTO ca.ca_load_details ('
    InsertStatement += ','.join(LoadDetailsColumns)
    InsertStatement += '\n)'
    InsertStatement += '\nSELECT '
    InsertStatement += ','.join(LoadDetailsColumns)
    InsertStatement += '\nFROM metadata_data_temp;'

    OpeningStatement = 'with metadata_data as (\n'

    DBMetadata_SQL_File.write(DeleteStatement)
    DBMetadata_SQL_File.write(OpeningStatement)
    DBMetadata_SQL_File.write(WithStatement)
    DBMetadata_SQL_File.write(CloseStatement)
    DBMetadata_SQL_File.write(InsertStatement)
    DBMetadata_SQL_File.close()
