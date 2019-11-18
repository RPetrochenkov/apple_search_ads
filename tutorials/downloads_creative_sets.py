import requests
import pandas as pd
from pandas.io.json import json_normalize
import copy

ORG_ID = "000000" # Your ORG_ID, you can find in Apple Search ads, cabinet in the top right menu
APPLE_CERT = (file_address_of_apple_pem, file_address_of_apple_key)

CAMPAIGN_URL = "https://api.searchads.apple.com/api/v2/reports/campaigns"
ADGROUPS_URL = "https://api.searchads.apple.com/api/v2/reports/campaigns/{campaignId}/adgroups"
CREATIVES_URL = "https://api.searchads.apple.com/api/v2/reports/campaigns/{campaignId}/creativesets"

REPORT_ENCODING = "utf-8"
HEADERS = {"Authorization": "orgId={org_id}".format(org_id=ORG_ID)}

# Each report should have start and end day
START_DATE = "2019-11-01"  # Format YEAR-MONTH-DAY like "2019-11-01"
END_DATE = "2019-11-01"  # Format YEAR-MONTH-DAY like "2019-11-01"

# We need to define granularity of the report. I can be DAILY, WEEKLY or MONTHLY
GRANULARITY = "DAILY"
TIME_ZONE = "UTC"

def main():
    my_campaigns = download_campaigns_report()
    
    campaign_ids = my_campaigns["metadata.campaignId"].unique().tolist()
    my_ad_groups = download_ad_groups_report(campaign_ids)

    my_creative_sets = download_creative_sets(campaign_ids)

    # Delete data we don't need from campaign and adgroup report
    my_campaigns = my_campaigns[["metadata.campaignId", "metadata.campaignName"]]
    my_ad_groups = my_ad_groups[["metadata.campaignId", "metadata.adGroupId", "metadata.adGroupName"]]

    # Join all Campaigns, Ad Groups and Creative Sets together 
    total = pd.merge(my_creative_sets, my_campaigns, how="inner", on="metadata.campaignId")
    total = pd.merge(total, my_ad_groups, how="inner", on=["metadata.campaignId", "metadata.adGroupId"])

    print(total)
    total.to_excel("creative_sets.xlsx") # or to_csv("creative_sets.csv")


def download_campaigns_report():
    # We call our function, that creates a JSON request for us
    report = campaign_report_in_json()

    response = requests.post(CAMPAIGN_URL, cert=APPLE_CERT, json=report, headers=HEADERS)
    response.encoding = REPORT_ENCODING

    # Id status code is not 200 - something went wrong. We stop the program and show exact mistake
    if response.status_code != 200:
        raise ValueError(response.content)

    # If we get here - the status is 200 and response contains our report
    # So we need to get it from JSON and ask json_normalize() to convert it to the table
    data = response.json()['data']['reportingDataResponse']['row']
    data = json_normalize(data)
    # Converting ID from INT to STR will help us later
    data["metadata.campaignId"] = data["metadata.campaignId"].astype(str)
    return data


def download_ad_groups_report(campaigns):
    report = ad_group_report_in_json()
    # First we download each ad group and put them separately in the array
    ad_groups = list()
    for campaign in campaigns:
        url = copy.copy(ADGROUPS_URL).format(campaignId=campaign)
        response = requests.post(url, cert=APPLE_CERT, json=report, headers=HEADERS)
        response.encoding = REPORT_ENCODING

        if response.status_code != 200:
            raise ValueError(response.content)

        data = response.json()['data']['reportingDataResponse']['row']
        data = json_normalize(data)
        ad_groups.append(data)

    # After all downloads are finished we combine all tables in the array intro one big table
    ad_groups = pd.concat(ad_groups, ignore_index=True, sort=False).reset_index(drop=True)
    ad_groups["metadata.campaignId"] = ad_groups["metadata.campaignId"].astype(str)
    ad_groups["metadata.adGroupId"] = ad_groups["metadata.adGroupId"].astype(str)
    return ad_groups


def download_creative_sets(campaigns):
    creative_sets = list()
    for campaign in campaigns:
        report = creative_sets_in_json()
        url = copy.copy(CREATIVES_URL).format(campaignId=campaign)

        response = requests.post(url, cert=APPLE_CERT, json=report, headers=HEADERS)
        response.encoding = REPORT_ENCODING

        if response.status_code != 200:
            raise ValueError(response.content)

        data = response.json()['data']['reportingDataResponse']['row']
        
        # Following lines are going to unpack compressed JSON data into full table
        data_frames = list()
        for row in data:
            df = json_normalize(data=row['granularity'])
            df = df.assign(**row['metadata'])
            data_frames.append(df)
            
        # Go next if there was nothing in the data 
        if not len(data_frames):
            continue
            
        data = pd.concat(data_frames, ignore_index=False, sort=False).reset_index(drop=True)

        data["metadata.campaignId"] = campaign
        creative_sets.append(data)
        
    creative_sets = pd.concat(creative_sets, ignore_index=True, sort=False).reset_index(drop=True)
    creative_sets = creative_sets.rename(columns={"adGroupId": "metadata.adGroupId"})

    creative_sets["metadata.campaignId"] = creative_sets["metadata.campaignId"].astype(str)
    creative_sets["metadata.adGroupId"] = creative_sets["metadata.adGroupId"].astype(str)
    return creative_sets


def campaign_report_in_json():
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
            "timeZone": TIME_ZONE,
            "returnRecordsWithNoMetrics": True,
            "returnRowTotals": True,
            "returnGrandTotals": False
        }
    return report


def ad_group_report_in_json():
    report = \
        {
            "startTime": START_DATE,
            "endTime": END_DATE,
            "selector": {
                "orderBy": [
                    {
                        "field": "adGroupId",
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
                    }
                ],
                "pagination": {
                    "offset": 0,
                    "limit": 1000
                }
            },
            "timeZone": TIME_ZONE,
            "returnRecordsWithNoMetrics": True,
            "returnRowTotals": True,
            "returnGrandTotals": False
        }
    return report


def creative_sets_in_json():
    report = \
        {
            "startTime": START_DATE,
            "endTime": END_DATE,
            "selector": {
                "orderBy": [
                    {
                        "field": "creativeSetId",
                        "sortOrder": "ASCENDING"
                    }
                ],
                "conditions": [
                ],
                "pagination": {
                    "offset": 0,
                    "limit": 1000
                }
            },
            "groupBy": [],
            "timeZone": TIME_ZONE,
            "granularity": GRANULARITY,
            "returnRecordsWithNoMetrics": False,
            "returnRowTotals": False,
            "returnGrandTotals": False
        }
    return report


if __name__ == "__main__":
    main()
