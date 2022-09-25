"""Automated data loading and staging - Pedestrian counting system.

This project demonstrates an end-to-end data science project from data engineering
(including automated data loading, wrangling, and staging) to visual analytics (interactive visualisation dashboards).
The datasets (Pedestrian Counting System) include locations of pedestrian sensor devices across Melbourne city
and hourly pedestrian counts from these sensors.

Author: Hoang Tam Vo

Version: 1.0
"""

# Import required libraries
import sqlite3 as sqldb  # light-weight sql database engine for demos
import pandas as pd  # data analysis and manipulation tool
from pandas_profiling import ProfileReport  # create profiling report for a dataframe

####################################################################################
## Define constant variables and configurations for the entire project
####################################################################################

# Location of staging database
SQLITE_DB_FILE_NAME = "./staged_pedestrian.db"
staged_db = sqldb.connect(SQLITE_DB_FILE_NAME)  # connect to staging database

# Data sources API endpoints
URL_SENSOR_LOCATION = "https://data.melbourne.vic.gov.au/resource/h57g-5234.csv"
URL_PEDESTRIAN_PER_HOUR = "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.csv"

# Melbourne 6 lockdown periods: https://www.platinumaccounting.com.au/melbourne-lockdown-dates/
LOCKDOWN_PERIODS = [('20200331', '20200512'), ('20200709', '20201027'), ('20210213', '20210217')
    , ('20210528', '20211006'), ('20210716', '20210727'), ('20210805', '20211021')]


####################################################################################
## Utility functions for accessing staged databases
####################################################################################

def stage_df_as_table(df, db_connection, table_name, if_exists="fail"):
    """Stages a dataframe.

    Stores a dataframe as a table in the staging database.

    Args:
        df:
            A dataframe to be stored into database.
        db_connection:
            Connection to the sql database
        if_exists: {"fail", "replace", "append"}, default "fail"
            How to behave if the table already exists.
            fail: Raise a ValueError.
            replace: Drop the table before inserting new values.
            append: Insert new values to the existing table.
    """
    df.to_sql(table_name, db_connection, if_exists=if_exists)


def query_database(db_connection, sql_query):
    """Fetches data from database.

    Retrieves data from the database with a provided SQL query.

    Args:
        db_connection:
            Connection to the sql database
        sql_query:
            SQL syntax to retrieve data from the database

    Returns:
        A dataframe resulted from the execution of the query in the database.
    """
    df = pd.read_sql(sql_query, db_connection)
    return df


####################################################################################
## Data loading, profiling, cleansing, enhancing
####################################################################################

def load_data(url):
    """Load data from API endpoint.

    Retrieves data from data sources with the provided API endpoint.

    Args:
        url:
            API endpoint

    Returns:
        A dataframe retrieved from the data source.
    """
    limit = 50000  # page size (number of records) to load for each request
    i = 0
    df = pd.DataFrame()
    while True:
        offset = i * limit
        endpoint = "{}?$limit={}&$offset={}".format(url, limit, offset)  # load a page from an offset
        page = pd.read_csv(endpoint)
        df = pd.concat([df, page])
        print(endpoint)
        i += 1
        if page.shape[0] < 50000:  # this is last page of the whole table
            break
    return df


def data_profiling(df, report_html_filename):
    """Profiles data of a dataframe

    Creates a data profiling report of a dataframe and write the report to an html file.
    """
    profile = ProfileReport(df)
    profile.to_file(report_html_filename)


def wrangle_sensor_location():
    """Wrangles sensor location dataset.

    Performs wrangling and cleansing sensor location dataset.
    """
    global df_sensor_location
    # remove special characters \n in front of the location string
    df_sensor_location["location"] = df_sensor_location["location"].str[5:]


def weekday_or_weekend(row):
    """Returns weekday or weekend value.

    Checks and returns whether it is weekday or weekend, given the row containing a "day" column.
    """
    if "Saturday" in row["day"] or "Sunday" in row["day"]:
        day_type = "Weekend"
    else:
        day_type = "Weekday"
    return day_type


def enhance_hourly_counts_data():
    """Enhances pedestrian hourly counts dataset.

    Performs wrangling, cleasing, and enhancing pedestrian hourly count dataset.
    Creates a date_key column, which is string following 'yyyymmdd' format.
    Creates a day_type column, whose value could be weekday or weekend depending on the day in the week.
    """
    global df_pedestrian_per_hour
    # derive date_key from existing date_time column
    df_pedestrian_per_hour["date_key"] = df_pedestrian_per_hour["date_time"].str[0:10]
    df_pedestrian_per_hour["date_key"] = df_pedestrian_per_hour["date_key"].str.replace("-", "")

    # create day_type flag as weekday or weekend
    df_pedestrian_per_hour['day_type'] = df_pedestrian_per_hour.apply(weekday_or_weekend, axis=1)


####################################################################################
## Extract statistics
####################################################################################

#### Stats 1 and 2: Top N (most pedestrians) locations by Day or Month

def calculate_stats_top_n(time_period):
    """Calculates top N locations (most pedestrians) by a time period (day or month).

    Steps:
    - Calculate average hourly counts by (1) time period (day or month) and (2) sensor id
    - Join with sensor location data to get location info
    - Calculate the rank within each time period group based on average hourly counts
    (rank 1 the most pedestrians)

    Args:
        time_period:
            "day" or "month" the required period for statistics

    Returns:
       Dataframe containing the calculated results (ranks of locations by time period)
    """
    # template sql query to perform the above steps
    sql_query = '''select count_stats.*
        , sensor.sensor_description, sensor.latitude, sensor.longitude, sensor.location 
    from 
        ( select {}, sensor_id, AVG(hourly_counts) as avg_hourly_counts
        from PEDESTRIAN_PER_HOUR
        group by {}, sensor_id
        ) count_stats
    join SENSOR on count_stats.sensor_id = sensor.sensor_id
    order by {}, avg_hourly_counts DESC'''.format(time_period, time_period, time_period)

    df_top_n_locations = query_database(staged_db, sql_query)

    # calculate rank using pandas (it could have been done in SQL, but current python/sqlite version not possible)
    df_top_n_locations["rank"] = df_top_n_locations.groupby(time_period)["avg_hourly_counts"].rank("dense",
                                                                                                   ascending=False)
    df_top_n_locations["rank"] = df_top_n_locations["rank"].astype("int")

    return df_top_n_locations


#### Stats 3: Location has shown most decline due to lockdowns

def calculate_avg_hourly_counts(filter_condition):
    """Calculates average hourly counts of locations subject to filter condition on data.

    Steps:
    - Calculate average hourly counts by sensor id on data that has been applied filter condition
    - Join with sensor location data to get location info

    Args:
        filter_condition:
            Condition to filter data for the analysis
            (e.g, based on date_key to determine precovid, lockdown, or after lockdown periods)

    Returns:
       Dataframe containing the calculated results (average hourly counts of locations)
    """
    # template sql query to perform the above steps
    sql_query = '''select count_stats.*
        , sensor.sensor_description, sensor.latitude, sensor.longitude, sensor.location 
    from 
        ( select sensor_id, AVG(hourly_counts) as avg_hourly_counts
        from PEDESTRIAN_PER_HOUR
        {}
        group by sensor_id
        ) count_stats
    join SENSOR on count_stats.sensor_id = sensor.sensor_id
    order by sensor_id'''.format(filter_condition)

    df_avg_hourly_counts = query_database(staged_db, sql_query)

    return df_avg_hourly_counts


def get_difference_in_hourly_counts(df1, rename_column1, df2, rename_column2, calculated_column, percent_column):
    """Calculates the difference in average hourly counts (e.g., between precovid and lockdown periods) for each location.

    Inputs: df1 and df2 contain data (e.g., average hourly counts for locations during precovid and lockdown periods)
    to calculate the difference

    Steps:
        - step 1: join the two dafaframe based on the location (sensor id)
        - step 2: calculate the difference in average hourly counts as a new calculated column
        - step 3: sort the dataframe based on the calculated difference (in descending sort)
        - step 4: calculate the percent change (decline or growth percent)

    Returns:
        Dataframe containing the calculated results.
    """
    # step 1
    df1.rename(columns={'avg_hourly_counts': rename_column1}, inplace=True)
    df2.rename(columns={'avg_hourly_counts': rename_column2}, inplace=True)
    df = pd.merge(left=df1, right=df2, how="inner")

    # step 2
    df[calculated_column] = df[rename_column1] - df[rename_column2]

    # step 3
    df = df.sort_values(calculated_column, ascending=False)

    # step 4
    if "growth" in percent_column:
        df[percent_column] = (df[calculated_column] / df[rename_column2]) * 100
    elif "decline" in percent_column:
        df[percent_column] = (df[calculated_column] / df[rename_column1]) * 100

    return df


def get_locations_most_decline():
    """Calculates the locations with most declined traffic during lockdowns (compared to precovid).

    Compares the average hourly counts for each location between precovid period and during lockdown period,
    and returns the location having most decline.
    """
    # 6 lockdown periods in Melbourne
    lockdown_list = [f' (date_key between "{start}" and "{end}") ' for (start, end) in LOCKDOWN_PERIODS]

    # sql filter condition for lockdown periods
    filter_lockdown_periods = "or".join(lockdown_list)

    # sql filter condition for precovid period
    filter_precovid_period = " date_key < " + LOCKDOWN_PERIODS[0][0]

    # calculate average hourly counts during lockdown periods
    df_avg_hourly_counts_lock_down = calculate_avg_hourly_counts(" where " + filter_lockdown_periods)

    # calculate average hourly counts during precovid period
    df_avg_hourly_counts_pre_covid = calculate_avg_hourly_counts(" where " + filter_precovid_period)

    # calculate the most declined locations
    df_hourly_counts_decline_lockdown = get_difference_in_hourly_counts(df_avg_hourly_counts_pre_covid,
                                                                        "precovid_avg_hourly_counts"
                                                                        , df_avg_hourly_counts_lock_down,
                                                                        "lockdown_avg_hourly_counts"
                                                                        , "declined_avg_hourly_counts",
                                                                        "percent_decline")

    return df_hourly_counts_decline_lockdown


#### Stats 4: Location has most growth after lockdowns

def get_locations_most_growth():
    """Calculates the locations with most growth traffic after lockdowns.

    Compares the average hourly counts for each location between lockdown period and after lockdown,
    and returns the location having most growth.
    """
    # 6 lockdown periods in Melbourne
    lockdown_list = [f' (date_key between "{start}" and "{end}") ' for (start, end) in LOCKDOWN_PERIODS]

    # sql filter condition for lockdown periods
    filter_lockdown_periods = "or".join(lockdown_list)

    # sql filter condition for after lockdown period
    filter_after_lockdown = " date_key > " + LOCKDOWN_PERIODS[-1][1]

    # calculate average hourly counts during lockdown periods
    df_avg_hourly_counts_lock_down = calculate_avg_hourly_counts(" where " + filter_lockdown_periods)

    # calculate average hourly counts after lockdown period
    df_avg_hourly_counts_after_lockdown = calculate_avg_hourly_counts(" where " + filter_after_lockdown)

    # calculate the most growth locations
    df_hourly_counts_growth_after_lockdown = get_difference_in_hourly_counts(df_avg_hourly_counts_after_lockdown,
                                                                             "after_lockdown_avg_hourly_counts"
                                                                             , df_avg_hourly_counts_lock_down,
                                                                             "lockdown_avg_hourly_counts"
                                                                             , "growth_avg_hourly_counts",
                                                                             "percent_growth")

    return df_hourly_counts_growth_after_lockdown


#### Stats 5: Peak hours traffic pattern analysis

def calculate_avg_hourly_counts_by_day_time():
    """Calculate average hourly by time (0-23 hours) in each day (Mon-Sun) in the week.

    For each location, calculate average hourly by time (0-23 hours) in each day in the week.
    This is useful for identifying peak hours in a day at a location.
    """
    sql_query = '''select count_stats.*
        , sensor.sensor_description, sensor.latitude, sensor.longitude, sensor.location 
    from 
        ( select sensor_id, day, time, AVG(hourly_counts) as avg_hourly_counts
        from PEDESTRIAN_PER_HOUR
        group by sensor_id, day, time
        ) count_stats
    join SENSOR on count_stats.sensor_id = sensor.sensor_id '''

    df = query_database(staged_db, sql_query)

    return df


#### Stats 6: Weekday vs. weekend traffic analysis

def calculate_avg_hourly_counts_weekday_weekend_time():
    """Calculate average hourly by time (0-23 hours) in weekday and weekend.

    For each location, calculate average hourly by time (0-23 hours) in weekday and weekend.
    This is useful for identifying peak hours during weekday and weekend at a location.
    """
    sql_query = '''select count_stats.*
        , sensor.sensor_description, sensor.latitude, sensor.longitude, sensor.location 
    from 
        ( select sensor_id, day_type, time, AVG(hourly_counts) as avg_hourly_counts
        from PEDESTRIAN_PER_HOUR
        group by sensor_id, day_type, time
        ) count_stats
    join SENSOR on count_stats.sensor_id = sensor.sensor_id '''

    df = query_database(staged_db, sql_query)

    return df


####################################################################################
## Execute the whole data pipeline
####################################################################################

if __name__ == '__main__':
    """Automated data loading and staging pipeline.
    
    The data pipeline architecture described in the report provides an ideal setup for a production environment 
    with commercial and scalable systems. 
    In this project a Python built-in SQL database system (SQLite) was used for demonstration purpose only, 
    and the entire data pipeline workflow was run using Python (version 3.7.1).
    """
    print("Automated data loading and staging - Pedestrian counting system!")

    # Task 1.1: Load pedestrian hourly counts from API endpoint
    df_pedestrian_per_hour = load_data(URL_PEDESTRIAN_PER_HOUR)

    # Task 1.2: Profile and QA hourly counts data
    data_profiling(df_pedestrian_per_hour, "data profiling pedestrian per hour.html")

    # Task 1.3: Cleanse, enrich and stage hourly counts dataset
    enhance_hourly_counts_data()
    stage_df_as_table(df_pedestrian_per_hour, staged_db, "PEDESTRIAN_PER_HOUR", "replace")

    # Task 2.1: Load sensor location from API endpoint
    df_sensor_location = load_data(URL_SENSOR_LOCATION)

    # Task 2.2: Profile and QA sensor location data
    data_profiling(df_sensor_location, "data profiling sensor location.html")

    # Task 2.3: Cleanse and stage sensor location into database
    wrangle_sensor_location()
    stage_df_as_table(df_sensor_location, staged_db, "SENSOR", "replace")

    # Task 3.1: Top N locations (most traffic) by Day
    df_top_n_locations_by_day = calculate_stats_top_n("day")
    stage_df_as_table(df_top_n_locations_by_day, staged_db, "TOP_N_LOCATIONS_BY_DAY", "replace")

    # Task 3.2: Top N locations (most traffic) by Month
    df_top_n_locations_by_month = calculate_stats_top_n("month")
    stage_df_as_table(df_top_n_locations_by_month, staged_db, "TOP_N_LOCATIONS_BY_MONTH", "replace")

    # Task 3.3: Locations most decline during lockdowns
    df_hourly_counts_decline_lockdown = get_locations_most_decline()
    stage_df_as_table(df_hourly_counts_decline_lockdown, staged_db, "HOURLY_COUNTS_DECLINE_LOCKDOWN", "replace")

    # Task 3.4: Locations most growth after lockdowns
    df_hourly_counts_growth_after_lockdown = get_locations_most_growth()
    stage_df_as_table(df_hourly_counts_growth_after_lockdown, staged_db, "HOURLY_COUNTS_GROWTH_AFTER_LOCKDOWN",
                      "replace")

    # Task 3.5: Peak hours traffic pattern analysis
    df_avg_hourly_counts_by_day_time = calculate_avg_hourly_counts_by_day_time()
    stage_df_as_table(df_avg_hourly_counts_by_day_time, staged_db, "AVG_HOURLY_COUNTS_BY_DAY_TIME", "replace")

    # Task 3.6: Weekday vs. weekend traffic analysis
    df_avg_hourly_counts_weekday_weekend_time = calculate_avg_hourly_counts_weekday_weekend_time()
    stage_df_as_table(df_avg_hourly_counts_weekday_weekend_time, staged_db, "AVG_HOURLY_COUNTS_WEEKDAY_WEEKEND",
                      "replace")
