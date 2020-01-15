from google.cloud import bigquery
from collections import Counter
import argparse
import csv


parser = argparse.ArgumentParser(description='BQ Schema Extractor utils')
parser.add_argument('project', type=str, help='The project that contains the BigQuery')
parser.add_argument('operation_mode', type=str, help='CSV or DDL operation')
parser.add_argument('file_path', type=str, help='Output dir for file')
parser.add_argument('count_incr', type=int, default=10, help='Log out every x tables. Choose an integer to use as a divisor', )
args = parser.parse_args()
project = args.project
file_path = args.file_path
count_incr = args.count_incr
operation_mode = args.operation_mode

# create bigquery connection obj
client = bigquery.Client(project=project)


def get_table_details(dataset_tablename):
    """
    Extract details using the BQ API
    """
    table = client.get_table(dataset_tablename)
    table_doc = {}
    type_list = list()
    table_schema = table.schema
    for i in table_schema:
        type_list.append(i.field_type)
    schema_type_count = dict(Counter(type_list))
    column_list = list()
    for i in table_schema:
        column_list.append(i.name)
    #column_list.sort()
    nullable_list = list()
    for i in table_schema:
        nullable_list.append(i.is_nullable)
    subfields_list = list()
    for i in table_schema:
        subfields_list.append(i.fields)
    table_doc['table_name'] = dataset_tablename
    table_doc['full_table_id'] = table.full_table_id
    table_doc['friendly_name'] = table.friendly_name
    table_doc['project'] = table.project
    table_doc['num_rows'] = table.num_rows
    table_doc['size_mb'] = int(table.num_bytes / 1000000)
    try:
        table_doc['avg_byte_per_row'] = round(table.num_bytes / table.num_rows, 2)
        table_doc['avg_kbyte_per_row'] = round(int(table.num_bytes / 1000) / table.num_rows, 2)
    except:
        table_doc['avg_byte_per_row'] = None
        table_doc['avg_kbyte_per_row'] = None
    table_doc['clustering_fields'] = table.clustering_fields
    table_doc['created'] = table.created.isoformat()
    table_doc['description'] = table.description
    table_doc['expires'] = table.expires
    table_doc['labels'] = table.labels
    table_doc['location'] = table.location
    table_doc['modified'] = table.modified.isoformat()
    table_doc['table_type'] = table.table_type
    table_doc['table_path'] = table.path
    table_doc['range_partitioning'] = table.range_partitioning
    table_doc['partitioning_type'] = table.partitioning_type
    table_doc['time_partitioning'] = table.time_partitioning
    table_doc['schema_type_count'] = schema_type_count
    table_doc['column_names'] = column_list
    table_doc['column_type_list'] = type_list
    table_doc['nullable_list'] = nullable_list
    table_doc['subfields_list'] = subfields_list
    return table_doc


def crawler(project):
    """
    Crawl all of the datasets and tables in the project
    """
    client.list_datasets(project=project)
    datasets = list(client.list_datasets())
    all_tables = []
    counter = 0
    for dataset in datasets:
        dataset_id = dataset.dataset_id
        tables_list = list(client.list_tables(dataset_id))
        for table in tables_list:
            dataset_table_name = dataset.dataset_id + '.' + table.table_id
            all_tables.append(dataset_table_name)
            counter += 1
            if counter % count_incr == 0:
                print(counter, 'tables crawled')
    print(counter, 'tables crawled')
    all_table_details = []
    for table in all_tables:
        table_details = get_table_details(table)
        all_table_details.append(table_details)
    return all_table_details


def write_to_csv(all_table_details):
    keys = all_table_details[0].keys()
    with open(file_path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(all_table_details)
    print('CSV saved to:', file_path)


def write_ddl(all_table_details):
    """
    Generate CREATE TABLE DDL
    @todo:
    Support for ARRAY data type
    Support for STRUCT more than 1 level
    Support for PARTITION BY TIMESTAMP and INGESTION TIME
    Support for PARTITION BY INTEGER RANGE
    Support for OPTIONS
    Support for AS
    Support for VIEWS
    """
    #TOKENS
    create_table_token_open = "CREATE OR REPLACE TABLE "
    values_token_open = " ("
    values_separator = " , "
    create_table_token_close = ")"
    blank_space = " "
    new_line = "\n"
    integer_token_read = "INTEGER"
    integer_token_write = "INT64"
    float_token_read = "FLOAT"
    float_token_write = "FLOAT64"
    record_token = "RECORD"
    struct_token_open = "STRUCT< "
    struct_token_close = " >"
    not_nullable_token = " NOT NULL"
    partition_by_token = " PARTITION BY "
    with open(file_path, 'w') as output_file:
        for table in all_table_details:
            output_file.write(create_table_token_open)
            output_file.write(table['table_name'])
            output_file.write(values_token_open)
            counter = 0
            for column in table['column_names']:
                output_file.write(column)
                output_file.write(blank_space)
                if (table['column_type_list'][counter] == integer_token_read):
                    output_file.write(integer_token_write)
                elif (table['column_type_list'][counter] == float_token_read):
                    output_file.write(float_token_write)
                elif (table['column_type_list'][counter] == record_token):
                    output_file.write(struct_token_open)
                    counter_2 = 0
                    for subfield in table['subfields_list'][counter]:
                        output_file.write(subfield.name)
                        output_file.write(blank_space)
                        output_file.write(subfield.field_type)
                        if(not subfield.is_nullable):
                            output_file.write(not_nullable_token)
                        if (( counter_2 +1 ) != len( table['subfields_list'][counter] ) ):
                            output_file.write(values_separator)
                        counter_2 += 1
                    output_file.write(struct_token_close)
                else:
                    output_file.write(table['column_type_list'][counter])
                if (not table['nullable_list'][counter]):
                    output_file.write(not_nullable_token)
                if (( counter +1 ) != len( table['column_type_list'] ) ):
                    output_file.write(values_separator)
                counter += 1
            output_file.write(create_table_token_close)
            if (table['time_partitioning'] is not None and table['time_partitioning'].field is not None):
                output_file.write(partition_by_token)
                output_file.write(table['time_partitioning'].field)
            output_file.write(new_line)
    print('File saved to:', file_path)


def main():
    print('Starting crawl')
    all_table_details = crawler(project)
    result_count = len(all_table_details)
    print(result_count, ' tables found')
    if(operation_mode == "csv"):
        write_to_csv(all_table_details)
    if(operation_mode == "ddl"):
        write_ddl(all_table_details)
    print('Crawl completed')


if __name__ == '__main__':
    main()