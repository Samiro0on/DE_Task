from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pandas as pd
import math
print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

# while True:
#     try:
#         psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
#         break
#     except OperationalError:
#         sleep(0.1)
# print('Connection to PostgresSQL successful.')

# Write the solution here
def get_df(query,con):
    df = pd.read_sql(query,con)
    return df 
def write_df(df,con,table_name):
    df.to_sql(table_name, con, if_exists='append', index=False,chunksize=1000)

def get_distance(row):
    lat1 =  row.lat1
    lon1  =   row.lon1
    lat2   =  row.lat2
    lon2    =  row.lon2 
    distance = math.acos(math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)) * 6371
    return round(distance,3) 

def transform_data(df):
    df[["lat1","lat2","lon1","lon2"]] = df[["lat1","lat2","lon1","lon2"]].astype(float)

    df['distance']=df.apply(get_distance,axis=1)

    df = df.groupby(['device_id','hour_timestamp'],as_index=False).agg({"distance":sum})
    return df
if __name__ == "__main__":
    query_max_temp_per_device_per_hour = """

    with cte as 
    (select device_id,temperature,date_trunc('hour', to_timestamp(time::integer ) ) hour_timestamp from devices )
    select device_id,hour_timestamp,max(temperature)as max_temperature from cte group by device_id,hour_timestamp
    """

    psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
    mysql_engine = create_engine(environ["MYSQL_CS"])

    src_con = psql_engine.connect()
    out_con = mysql_engine.connect()
    df = get_df(query_max_temp_per_device_per_hour,src_con)
    write_df(df,out_con,"temperature_per_device_per_hour")

    query_points_count_per_device_per_hour="""
    with cte as 
    (select device_id,date_trunc('hour', to_timestamp(time::integer ) ) hour_timestamp from devices )
    select device_id,hour_timestamp,count(hour_timestamp) as points_count from cte group by device_id,hour_timestamp

    """
    df = get_df(query_points_count_per_device_per_hour,src_con)
    write_df(df,out_con,"points_per_device_per_hour")

    query="""

        with cte as 
        (select device_id,location,date_trunc('hour', to_timestamp(time::integer ) ) hour_timestamp from devices ),
        cte2 as
        (select device_id,location::json,hour_timestamp,lead (location) over(partition by device_id,hour_timestamp)::json as location2 from cte),
        cte3 as
        ( select device_id,hour_timestamp,
                            location->'latitude' as lat1,location->'longitude' as lon1,
                            location2->'latitude' as lat2,location2->'longitude' as lon2 
                            from cte2
                            where location2 is not Null
        )
        select * from cte3
        
    """
    df = get_df(query,src_con)
    df = transform_data(df)
    write_df(df,out_con,"distance_per_device_per_hour")
    src_con.close()
    out_con.close()