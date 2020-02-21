# NiFi-GetGoogleAnalyticsData

 Apache NiFi data flow to extract data from Google Analytics every hour.


# Python Code Reference: 
https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/service-py

# Example call:
   ```python3.5 getGA.py file_path report_name dimensions metrics start_date end_date my_filter```

# Arguments:
    1. file_path = path where csv and json extract will be stored
    2. report_name = csv file name | target redshift table name(table will be loaded via Infa job)
    3. dimensions = dimensions for GA Query
    4. metrics = Metrics/measures for GA Query
    5. start_date = start date of extract
    6. end_date = till date when report will be generated
    7. my_filter = will apply a filer of type HH hour-1
# Sample call :
   python getGA.py \
      /nifi/nifi_staging/google/ \
      ga_traffic_channel_goal_metrics \
      ga:date,ga:channelGrouping \
      ga:sessions,ga:goal6Completions,ga:goal9Completions,ga:goal10Completions,ga:goal13Completions,ga:goal8Completions,ga:goal7Completions,ga:goal12Completions,ga:goal4Completions,ga:goal5Completions \
      yesterday \
      yesterday \
      hour
