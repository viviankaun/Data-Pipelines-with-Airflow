## ETL Pipelines with Airflow
![This is an image](https://github.com/viviankaun/Data-Pipelines-with-Airflow/blob/main/img/GraphView01.png)

## Dataset 
- Log data:  s3://udacity-dend/log_data/ 
        's3://udacity-dend/log-data/2018/11/2018-11-01.JSON'
- Song data:  s3://udacity-dend/song_data/
       's3://udacity-dend/song_data/A/A/C/xxxx.JSON'

## Files 
| File Namee | Descriptions  | 
|----| ------------- |  
| create_tables.sql  | create tables   | 
| udac_example_dag.py  | main airflow DAG  |  
|- plugins\operators |  |  
| __init__.py  |  list of Operators     |  
| data_quality.py  | checking table records   |   
| load_dimension.py  | insert table from stage table |  
| load_fact.py  | insert table from stage table |   
| stage_redshift.py  | get data from s3 to redshift  |   
|- plugins\helper | |  
| __init__.py  | list of SQL class  |  
| sql_queries.py  |  SQL statement of insert table   |   

## COPY Statement from s3 to redshift
```
copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {} 
    """
```

## Variables, macros and filters can be used in templates

-DAG.py
s3_key = "log-data/{execution_date.year}/{execution_date.month:02d}",    

- Operator.py
template_fields = ("s3_key",)

rendered_key = self.s3_key.format(**context)
s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

 




