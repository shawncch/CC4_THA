import pandas as pd
import json
import requests

# run this once to write json file

url = "https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json"
response = requests.get(url)
with open('restaurant_data.json', 'w') as json_file:  
    json.dump(response.json(), json_file, indent=4) 


def initialize_data():  #initialize json data and populate missing zomato_events keys
    with open("restaurant_data.json") as f:
        data = json.load(f)

    for i, result in enumerate(data):
        for j, restaurant in enumerate(result['restaurants']):
            if 'zomato_events' not in restaurant['restaurant']:
                data[i]["restaurants"][j]["restaurant"]["zomato_events"] = []
    
    return data


def preprocess_unique_restaurants(data):

    #time complexity of O(n*d), where n is total no. of restaurants and d is the maximum depth we retrieve our value from
    unique_restaurants_df = pd.json_normalize(data, record_path=['restaurants'])  

    unique_restaurants_df = unique_restaurants_df[[
                        "restaurant.R.res_id",
                        "restaurant.name", 
                        "restaurant.location.country_id", 
                        "restaurant.location.city",
                        "restaurant.user_rating.votes",
                        "restaurant.user_rating.aggregate_rating",
                        "restaurant.user_rating.rating_text",
                        "restaurant.cuisines"
                        ]]

    rename_dict = {"restaurant.name" : "Restaurant Name", 
                    "restaurant.R.res_id" : "Restaurant Id",
                    "restaurant.location.country_id" : "Country Code", 
                    "restaurant.location.city" : "City",
                    "restaurant.user_rating.votes" : "User Rating Votes",
                    "restaurant.user_rating.aggregate_rating" : "User Aggregate Rating",
                    "restaurant.user_rating.rating_text" : "User Rating Text",
                    "restaurant.cuisines" : "Cuisines"}

    unique_restaurants_df.rename(columns=rename_dict, inplace=True)

    unique_restaurants_df['User Aggregate Rating'] = unique_restaurants_df['User Aggregate Rating'].astype('float')
    

    return unique_restaurants_df


def preprocess_unique_events(data):
        unique_events_df = pd.json_normalize(data, 
                                                record_path=['restaurants', 'restaurant', 'zomato_events'], 
                                                meta = [["restaurants", "restaurant", "R", "res_id"], ["restaurants", "restaurant", "name"]])

        unique_events_df["photo_urls"] = unique_events_df["event.photos"].apply(lambda x: [photo["photo"]["url"] for photo in x])


        unique_events_df = unique_events_df[[
                                        "event.event_id",
                                        "restaurants.restaurant.R.res_id",
                                        "restaurants.restaurant.name",
                                        "photo_urls",
                                        "event.title",
                                        "event.start_date",
                                        "event.end_date",
                                        
                                ]]

        rename_dict = {
                        "event.event_id" : "Event Id",
                        "restaurants.restaurant.R.res_id" : "Restaurant Id",
                        "restaurants.restaurant.name" : "Restaurant Name",
                        "event.title" : "Event Title",
                        "event.start_date" : "Event Start Date",
                        "event.end_date" : "Event End Date",
                        "photo_urls" : "Photo URL List"
                        }

        unique_events_df.rename(columns=rename_dict, inplace=True)
        unique_events_df["Photo URL List"] = unique_events_df["Photo URL List"].apply(lambda x: "NA" if len(x) == 0 else x)
        unique_events_df["Restaurant Id"] = unique_events_df["Restaurant Id"].astype('int')
        

        return unique_events_df


def preprocess_country_codes():
    country_code_df = pd.read_excel('Country-Code.xlsx', sheet_name='Sheet1')
    return country_code_df


def output_restaurant_data(unique_restaurants_df, country_code_df):
    merged_df = unique_restaurants_df.merge(country_code_df, on = "Country Code", how = "inner")
    merged_df = merged_df[["Restaurant Id", "Restaurant Name", "Country", "City", "User Rating Votes", "User Aggregate Rating", "Cuisines"]]
    merged_df = merged_df.fillna("NA")
    return merged_df

def output_restaurant_data_with_events(merged_restaurants_df, unique_events_df): # restaurant details duplicated depending on no. of events
    unique_events_df.drop(columns = ["Restaurant Name"], inplace = True) #drop duplicate column
    merged_df = merged_restaurants_df.merge(unique_events_df, on = "Restaurant Id", how = "left")
    merged_df = merged_df.fillna("NA")

    # removing trailing zeros from event id, trailing zeros appeared due to the merge 
    # pandas automatically promotes the entire column’s data type to float to accommodate null values
    merged_df['Event Id'] = merged_df['Event Id'].apply(lambda x: "NA" if x == "NA" else int(x))

    return merged_df

def output_restaurant_events(restaurant_data_df):
    filtered_df = restaurant_data_df[(restaurant_data_df["Event Start Date"] != "NA") & (restaurant_data_df["Event End Date"] != "NA")]
    filtered_df["converted_eventstartdt"] = pd.to_datetime(filtered_df["Event Start Date"])
    filtered_df["converted_eventenddt"] = pd.to_datetime(filtered_df["Event End Date"])

    april_2019 = pd.to_datetime("2019-04-01")

    filtered_df1 = filtered_df[(filtered_df["converted_eventstartdt"] <= april_2019) & (filtered_df["converted_eventenddt"] >= april_2019)]
    filtered_df2 = filtered_df1[["Event Id", "Restaurant Id", "Restaurant Name", "Photo URL List", "Event Title", "Event Start Date", "Event End Date"]]
    
    return filtered_df2

def output_thresholds(data):
    restaurant_data_df = preprocess_unique_restaurants(data)
    restaurant_data_df = restaurant_data_df.loc[lambda x : x["User Rating Text"].isin(["Excellent", "Very Good", "Good", "Average", "Poor"])]
    thresholds = restaurant_data_df.groupby("User Rating Text").agg(
        min_rating=("User Aggregate Rating", "min"),
        max_rating=("User Aggregate Rating", "max"),
        avg_rating=("User Aggregate Rating", "mean")
    )

    thresholds.sort_values(by = "avg_rating", inplace = True)

    print(thresholds)

    thresholds.to_csv("thresholds.csv")

if __name__ == "__main__":
    data = initialize_data()
    unique_restaurants_df = preprocess_unique_restaurants(data)
    unique_events_df = preprocess_unique_events(data)
    country_code_df = preprocess_country_codes()
    
    merged_unique_restaurants_df = output_restaurant_data(unique_restaurants_df, country_code_df)
    filtered_unique_events_df = output_restaurant_events(unique_events_df)
    restaurants_with_events_df = output_restaurant_data_with_events(merged_unique_restaurants_df, unique_events_df)

    # Requirement 1
    merged_unique_restaurants_df.to_csv("restaurant_details.csv", index = False)
    restaurants_with_events_df.to_csv("restaurant_withevents.csv", index = False)
    
    # Requirement 2
    filtered_unique_events_df.to_csv("restaurant_events.csv", index = False)

    # Requirement 3: Ratings threshold
    output_thresholds(data)