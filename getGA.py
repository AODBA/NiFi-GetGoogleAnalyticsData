'''
Reference: https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/service-py
Example call:
    python3.5 getGA.py file_path report_name dimensions metrics start_date end_date my_filter
Arguments:
    1. file_path = path where csv and json extract will be stored
    2. report_name = csv file name | target redshift table name(table will be loaded via Infa job)
    3. dimensions = dimensions for GA Query
    4. metrics = Metrics/measures for GA Query
    5. start_date = start date of extract
    6. end_date = till date when report will be generated
    7. my_filter = will apply a filer of type HH hour-1
Sample call :
   python getGA.py
      /nifi/nifi_staging/google/
      ga_traffic_channel_goal_metrics
      ga:date,ga:channelGrouping
      ga:sessions,ga:goal6Completions,ga:goal9Completions,ga:goal10Completions,ga:goal13Completions,ga:goal8Completions,ga:goal7Completions,ga:goal12Completions,ga:goal4Completions,ga:goal5Completions
      yesterday
      yesterday
      hour

'''
import os
import sys
import json
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2

file_path = sys.argv[1]
report_name = sys.argv[2]
dimensions = sys.argv[3]
metrics = sys.argv[4]
start_date = sys.argv[5]
end_date = sys.argv[6]
my_filter = sys.argv[7]



## Some Hour cleanup
if len(str(int(my_filter) - 1)) == 1:
    my_filter = '0' + str(int(my_filter) - 1)
else:
    my_filter = str(int(my_filter) - 1)
my_filter = 'ga:hour==' + my_filter



key_file_location_json = '/opt/nifi/external_libs/gAnalytics/config.json'
service_email = 'service-acc-ganalytics@gserviceaccount.com'
scope = ['https://www.googleapis.com/auth/analytics.readonly']
api_name = 'analytics'
api_version = 'v3'


def create_folder(file_path):
        # Create Folder if does not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)




# (2) Create Credentials and build the Service Object
def get_service(key_file_location_json, service_email):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location_json, scopes=scope)
    # credentials = ServiceAccountCredentials.from_p12_keyfile(service_email, key_file, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    # Build the service object.
    service = build(api_name, api_version, http=http)
    return service


# (3) Use the Analytics service object to get the first profile id
def get_first_profile_id(service):
    # Get a list of all Google Analytics accounts for this user
    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')

        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
            accountId=account).execute()

        if properties.get('items'):
            # Get the first property id.
            property = properties.get('items')[0].get('id')

        # Get a list of all views (profiles) for the first property.
        profiles = service.management().profiles().list(
            accountId=account,
            webPropertyId=property).execute()

        if profiles.get('items'):
            # return the first view (profile) id.
            return profiles.get('items')[0].get('id')
    return None


# (4) Find the start-index for the next record
def find_next_index(url_string):
    next_link = url_string
    start_index_pos = next_link.find('start-index=')
    end_pos = next_link.find('&', start_index_pos)
    return int(next_link[start_index_pos + 12: end_pos])


# (5) Convert Result to csv
def create_table(results_list, file_path, header, report_name):
    csv_file = file_path + report_name + '.csv'
    f = open(csv_file, "w")
    f.write(header + '\n')
    for result in results_list:
        result_row = result["rows"]
        for i in range(len(result_row)):
            row_i = result_row[i]
            row_string = ''
            for j in range(len(row_i)):
                row_string += row_i[j] + ','

            f.write(row_string[:-1] + '\n')
    f.close()
    #print("{}.csv has been generated".format(report_name))


# (6) Get results, generate json file and return a list of results
def get_results(service, profile_id, start, end, my_filter, metrics, dimensions, report_name, file_path):
    counter = 0
    start_index = 1
    results_list = []

    while True:
        f_name = report_name  + '.json'
        json_file_path = file_path + f_name

        result = service.data().ga().get( \
            ids='ga:' + profile_id, start_date=start, end_date=end, \
            metrics=metrics, dimensions=dimensions, start_index=start_index, \
            filters=my_filter, max_results=10000).execute()
        #print("Total Record Number for the %s query result no.%s: %s" \
        #       % (report_name, str(counter), len(result['rows'])))

        results_list.append(result)
        # Create Json Files
        pretty = json.dumps(result, indent=1)
        f = open(json_file_path, "w")
        f.write(pretty)
        f.close()
        #print("{} has been successfully generated.".format(f_name))
        try:
            next_link = result["nextLink"]
            start_index = find_next_index(next_link)
            counter += 1
        except KeyError:
            #print("No more records to retrieve for {}".format(f_name))
            break
    return results_list


# (7) Create Header for csv file
def create_header(dimensions, metrics, report_name):
    header = dimensions.replace('ga:', '') + ',' + metrics.replace('ga:', '')
    header = header.replace(' ', '')
 #   print("Table Name: {}".format(report_name))
 #   print(header)
    return header


# Main Method
def main():
    create_folder(file_path)
    # Authentication
    service = get_service(key_file_location_json, service_email)
    profile_id = get_first_profile_id(service)
    # print("Obtained Profile ID: " + profile_id)

    header = create_header(dimensions, metrics, report_name)
    results_list = get_results(service, profile_id, start_date, end_date, my_filter, metrics, dimensions, report_name, file_path)
    create_table(results_list, file_path, header, report_name)


# Execute
if __name__ == "__main__":
    main()
~
