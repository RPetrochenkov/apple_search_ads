import requests   # We need this for communication with Apple
from pandas.io.json import json_normalize  # this will convert Apple API Response to Table View
import pandas as pd  # This will help us to work with Tables

ORG_ID = "000000" # Your ORG_ID, you can find in Apple Search ads, cabinet in the top right menu
APPLE_CERT = (file_address_of_apple_pem, file_address_of_apple_key)

START_DATE = "2019-10-01"
END_DATE = "2019-10-10"


def main():
    # We call our main delivery-boy function to do all the work
    df = download_campaigns_report()

    # We rename some columns names to make them more beautiful
    new_columns = {x: x.replace("metadata.","") for x in df.columns}
    df = df.rename(columns=new_columns)

    # We fill empty rows with zeros
    df = df.fillna(0)

    # We print the table and save it as CSV
    print(df)
    df.to_csv("campaigns_data.csv") # You also can use .to_excel(...) method to save is xlsx


def download_campaigns_report():
    # URL, where we through out request
    url = "https://api.searchads.apple.com/api/v2/reports/campaigns"

    # We call our function, that creates a JSON request for us
    report = create_campaigns_report()

    # Now we construct everything together
    headers = {"Authorization": "orgId={org_id}".format(org_id=ORG_ID)}
    response = requests.post(url, cert=APPLE_CERT, json=report, headers=headers)
    response.encoding = "utf-8"

    # Id status code is not 200 - something went wrong. We stop the program and show exact mistake
    if response.status_code != 200:
        raise ValueError(response.content)

    # If we ger here - the status is 200 and response contains our report
    # So we need to get it from JSON and ask json_normalize() to convert it to the table
    data = response.json()['data']['reportingDataResponse']['row']
    data = json_normalize(data)
    return data


def create_campaigns_report():
    report = \
        {
            "startTime": START_DATE,  # WE USE OUR START DATE HERE
            "endTime": END_DATE,      # WE USE OUR END DATE HERE
            "selector": {
                "orderBy": [
                    {
                        "field": "countryOrRegion",
                        "sortOrder": "ASCENDING"
                    }
                ],
                "conditions": [
                    {
                        "field": "deleted",
                        "operator": "EQUALS",
                        "values": [
                            "false"
                        ]
                    },
                    {
                        "field": "campaignStatus",
                        "operator": "EQUALS",
                        "values": [
                            "ENABLED"
                        ]
                    }
                ],
                "pagination": {
                    "offset": 0,
                    "limit": 1000
                }
            },
            "groupBy": [
                "countryOrRegion"
            ],
            "timeZone": "UTC",
            "returnRecordsWithNoMetrics": True,
            "returnRowTotals": True,
            "returnGrandTotals": True
        }
    return report


if __name__ == "__main__":
    main()
