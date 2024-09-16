import unittest
import pandas as pd
from main_revised import *

with open("test.json") as f:
    data = json.load(f)

class TestPreprocessUniqueRestaurants(unittest.TestCase):
    def test_preprocess_unique_restaurants(self):
        print("Running test_preprocess_unique_restaurants")
        
        expected_df = pd.DataFrame({
            "Restaurant Id": [743838, 743839],
            "Restaurant Name": ["Shawn's Pizza", "Shawn's Ramen"],
            "Country Code": [184, 14],
            "City": ["Punggol", "Melbourne"],
            "User Rating Votes": [250, 320],
            "User Aggregate Rating": [4.5, 4.2],
            "User Rating Text": ["Excellent", "Very Good"],
            "Cuisines": ["Italian, Pizza", "Ramen, Japanese"]
        })

        result_df = preprocess_unique_restaurants(data)
        pd.testing.assert_frame_equal(result_df, expected_df)
        print(result_df)

    def test_preprocess_unique_events(self):
        print("Running test_preprocess_unique_events")

        expected_df = pd.DataFrame({
            "Event Id": [123456, 123458, 123457],
            "Restaurant Id": [743838, 743838, 743839],
            "Restaurant Name": ["Shawn's Pizza", "Shawn's Pizza", "Shawn's Ramen"],
            "Photo URL List": [["https://www.zomato.com/singapore/pizza-festival", "https://www.zomato.com/singapore/pizza-festival-2"], ["https://www.zomato.com/singapore/pasta-festival"], ["https://www.zomato.com/melbourne/ramen-festival"]],
            "Event Title": ["Pizza Festival", "Pasta Festival", "Ramen Festival"],
            "Event Start Date": ["2019-02-01", "2021-04-01", "2019-05-01"],
            "Event End Date": ["2019-05-31", "2021-04-30", "2019-05-30"]
        })

        result_df = preprocess_unique_events(data)
        pd.testing.assert_frame_equal(result_df, expected_df)


    def test_output_unique_restaurants(self):
        print("Running test_output_unique_restaurants")

        country_code_df = preprocess_country_codes()
        unique_restaurants_df = preprocess_unique_restaurants(data)
        

        expected_df = pd.DataFrame({
            "Restaurant Id": [743838, 743839],
            "Restaurant Name": ["Shawn's Pizza", "Shawn's Ramen"],
            "Country": ["Singapore", "Australia"],
            "City": ["Punggol", "Melbourne"],
            "User Rating Votes": [250, 320],
            "User Aggregate Rating": [4.5, 4.2],
            "Cuisines": ["Italian, Pizza", "Ramen, Japanese"]
            })

        result_df = output_restaurant_data(unique_restaurants_df, country_code_df)
        pd.testing.assert_frame_equal(result_df, expected_df)
        print(result_df)

    def test_output_restaurants_with_events(self):
        print("Running test_output_restaurants_with_events")

        country_code_df = preprocess_country_codes()
        unique_restaurants_df = preprocess_unique_restaurants(data)
        merged_restaurants_df = output_restaurant_data(unique_restaurants_df, country_code_df)
        unique_events_df = preprocess_unique_events(data)
        

        expected_df = pd.DataFrame({
            "Restaurant Id": [743838, 743838, 743839],
            "Restaurant Name": ["Shawn's Pizza", "Shawn's Pizza", "Shawn's Ramen"],
            "Country": ["Singapore", "Singapore", "Australia"],
            "City": ["Punggol", "Punggol", "Melbourne"],
            "User Rating Votes": [250, 250, 320],
            "User Aggregate Rating": [4.5, 4.5, 4.2],
            "Cuisines": ["Italian, Pizza", "Italian, Pizza", "Ramen, Japanese"],
            "Event Id": [123456, 123458, 123457],
            "Photo URL List": [["https://www.zomato.com/singapore/pizza-festival", "https://www.zomato.com/singapore/pizza-festival-2"], ["https://www.zomato.com/singapore/pasta-festival"], ["https://www.zomato.com/melbourne/ramen-festival"]],
            "Event Title": ["Pizza Festival", "Pasta Festival", "Ramen Festival"],
            "Event Start Date": ["2019-02-01", "2021-04-01", "2019-05-01"],
            "Event End Date": ["2019-05-31", "2021-04-30", "2019-05-30"]
        })

        result_df = output_restaurant_data_with_events(merged_restaurants_df, unique_events_df)
        pd.testing.assert_frame_equal(result_df, expected_df)
        print(result_df)

    def test_filter_events_by_april_2019(self):
        unique_events_df = preprocess_unique_events(data)

        expected_df = pd.DataFrame({
            "Event Id": [123456],
            "Restaurant Id": [743838],
            "Restaurant Name": ["Shawn's Pizza"],
            "Photo URL List": [["https://www.zomato.com/singapore/pizza-festival", "https://www.zomato.com/singapore/pizza-festival-2"]],
            "Event Title": ["Pizza Festival"],
            "Event Start Date": ["2019-02-01"],
            "Event End Date": ["2019-05-31"]
        })


if __name__ == '__main__':
    unittest.main()