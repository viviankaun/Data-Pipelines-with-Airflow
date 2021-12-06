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
|- plugins\operators | ddd |  
| __init__.py  |  list of Operators     |  
| data_quality.py  | checking table records   |   
| load_dimension.py  | insert table from stage table |  
| load_fact.py  | insert table from stage table |   
| stage_redshift.py  | get data from s3 to redshift  |   
|- plugins\helper |ddd|  
| __init__.py  | dddd  |  
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

 




