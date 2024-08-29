import sys
import requests
import json
import boto3
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

def save_json_to_s3(url):
    response = requests.get(json_url)
    json_data = response.json()

    json_string = json.dumps(json_data)

    s3_bucket = 'cc4tha'
    s3_key = 'source/restaurant_data.json'

    s3.put_object(Body=json_string, Bucket=s3_bucket, Key=s3_key)
    
def preprocess_country_codes():
    country_code_df = pd.read_excel('s3://cc4tha/source/Country-Code.xlsx', sheet_name='Sheet1')
    return country_code_df
    
def append_restaurant_details(restaurant, restaurant_ids, restaurant_names, restaurant_countrycodes, restaurant_cities, restaurant_votes, restaurant_user_ratings, restaurant_user_ratings_text, restaurant_cuisines):
    restaurant_ids.append(restaurant.get("R").get("res_id"))
    restaurant_names.append(restaurant.get("name"))
    restaurant_countrycodes.append(restaurant.get("location").get("country_id"))
    restaurant_cities.append(restaurant.get("location").get("city"))
    restaurant_votes.append(restaurant.get("user_rating").get("votes"))
    restaurant_user_ratings_text.append(restaurant.get("user_rating").get("rating_text"))
    restaurant_user_ratings.append(float(restaurant.get("user_rating").get("aggregate_rating")))
    restaurant_cuisines.append(restaurant.get("cuisines"))

def preprocess_restaurant_data():
    bucket_name = 'cc4tha'
    file_key = 'source/restaurant_data.json'
    
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    
    data = json.loads(data)

    restaurant_ids = []
    restaurant_names = []
    restaurant_countrycodes = []
    restaurant_cities = []
    restaurant_votes = []
    restaurant_user_ratings = []
    restaurant_user_ratings_text = []   
    restaurant_cuisines = []
    restaurant_eventids = []
    restaurant_photourls = []
    restaurant_eventtitles = []
    restaurant_eventstartdates = []
    restaurant_eventenddates = []

    for result in data:
        for r in result["restaurants"]:
            restaurant = r["restaurant"]

            append_restaurant_details(restaurant, restaurant_ids, restaurant_names, restaurant_countrycodes, restaurant_cities, restaurant_votes, restaurant_user_ratings, restaurant_user_ratings_text, restaurant_cuisines)

            event_results = restaurant.get("zomato_events", "NA")

            if event_results == "NA" :
                [lst.append("NA") for lst in [restaurant_eventids, restaurant_photourls, restaurant_eventstartdates, restaurant_eventenddates, restaurant_eventtitles]]
            
            else:
                for i, e in enumerate(event_results):
                    if i != 0:
                        append_restaurant_details(restaurant, restaurant_ids, restaurant_names, restaurant_countrycodes, restaurant_cities, restaurant_votes, restaurant_user_ratings, restaurant_user_ratings_text, restaurant_cuisines)
                    event = e.get("event")
                    restaurant_eventids.append(event.get("event_id", "NA"))
                    if "photos" in event and len(event.get("photos")) != 0:
                            restaurant_photourls.append(event.get("photos")[0].get("photo").get("url", "NA"))
                    else:
                        restaurant_photourls.append("NA")
                    restaurant_eventstartdates.append(event.get("start_date", "NA"))
                    restaurant_eventenddates.append(event.get("end_date", "NA"))
                    restaurant_eventtitles.append(event.get("title", "NA"))

    data = {"Restaurant Id" : restaurant_ids, 
            "Restaurant Name" : restaurant_names, 
            "Country Code" : restaurant_countrycodes,
            "City" : restaurant_cities,
            "User Rating Votes" : restaurant_votes,
            "User Aggregate Rating" : restaurant_user_ratings,
            "User Rating Text" : restaurant_user_ratings_text,
            "Cuisines" : restaurant_cuisines,
            "Event Id" : restaurant_eventids,
            "Photo URL" : restaurant_photourls,
            "Event Title" : restaurant_eventtitles,
            "Event Start Date" : restaurant_eventstartdates,
            "Event End Date" : restaurant_eventenddates
    }

    df = pd.DataFrame(data)
    df.to_csv("s3://cc4tha/output/complete_restaurant_data.csv", index=False)
    return df


def output_restaurant_data(restaurant_data_df, country_code_df):
    merged_df = restaurant_data_df.merge(country_code_df, on = "Country Code", how = "left")
    merged_df = merged_df.drop(columns=["Country Code"])
    merged_df = merged_df[["Restaurant Id", "Restaurant Name", "Country", "City", "User Rating Votes", "User Aggregate Rating", "Cuisines"]]
    merged_df_deduped = merged_df.drop_duplicates(keep='first') # duplicates arising combined preprocessing function, a restaurant may have multiple events, and as a result have repeated restaurant details with different event details
    merged_df_deduped.to_csv("s3://cc4tha/output/restaurant_data.csv", index = False)
    
def output_restaurant_events(restaurant_data_df):
    filtered_df = restaurant_data_df[restaurant_data_df["Event Start Date"] != "NA"]
    filtered_df["converted_eventstartdt"] = pd.to_datetime(filtered_df["Event Start Date"])
    filtered_df1 = filtered_df.loc[lambda x : (x["converted_eventstartdt"].dt.year == 2019) & (x["converted_eventstartdt"].dt.month == 4)]
    filtered_df2 = filtered_df1[["Event Id", "Restaurant Id", "Restaurant Name", "Photo URL", "Event Title", "Event Start Date", "Event End Date"]]
    filtered_df2.to_csv("s3://cc4tha/output/restaurant_events.csv", index = False)


if __name__ == '__main__':
    
    # can uncomment the below function to upload new json file
    # save_json_to_s3(url) 
    
    s3 = boto3.client('s3')
    json_url = 'https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json'
    
    restaurant_data_df = preprocess_restaurant_data()
    country_code_df = preprocess_country_codes()
    output_restaurant_data(restaurant_data_df, country_code_df)
    output_restaurant_events(restaurant_data_df)
    
    # requirement 3
    restaurant_data_df = restaurant_data_df.loc[lambda x : x["User Rating Text"].isin(["Excellent", "Very Good", "Good", "Average", "Poor"])]
    thresholds = restaurant_data_df.groupby("User Rating Text").agg(
        min_rating=("User Aggregate Rating", "min"),
        max_rating=("User Aggregate Rating", "max"),
        avg_rating=("User Aggregate Rating", "mean")
    )
    
    thresholds.sort_values(by = "avg_rating", inplace = True)
    
    print(thresholds)



