# gcp_data_utilities
Smaller utility apps that don't need their own repository
                                  
```python
usage: bq_crawler.py [-h] --project PROJECT [--csv_path CSV_PATH]
                     [--json_path JSON_PATH] [--sql_path SQL_PATH]
                     [--output_bq_table OUTPUT_BQ_TABLE]
                     [--count_incr COUNT_INCR] 
```

Crawl all datasets & tables in a project and save the table details

```python
optional arguments:
  -h, --help            show this help message and exit
  --project PROJECT     The project that contains the BigQuery
  --csv_path CSV_PATH   Output dir for CSV
  --json_path JSON_PATH
                        Output dir for JSON
  --sql_path SQL_PATH   Output dir for SQL
  --output_bq_table OUTPUT_BQ_TABLE
                        Table to write to in BigQuery. Ex: mydataset.mytable
  --count_incr COUNT_INCR
                        Log out every x tables. Choose an integer to use as a
                        divisor
```
