Case Scenario: Steven is a travel blogger that intends to create a travel food series. He is looking at data from Zomato for inspiration. He wants to find restaurants that have good user ratings and interesting past events.

**Assumptions or interpretations made about the requirements**
  1) Event Start Dates are used to filter for events that took place in April 2019, regardless of the Event End Date
     e.g. An event can start in 14 April 2019 and end in 18 May and still be considered an event that took place in 2019
  2) The Photo URL data required for restaurants_events.csv is retrieved from Each Restaurant -> Each Event -> Photo -> Photo URL


**Design Considerations**
Since the desired output CSV files for the first and second requirement required the iteration of the json file to extract the values required, I have decided to have one main preprocessing function to preprocess all the data required for both the requirements, instead of having two separate preprocessing functions.
However, as a result of this combined preprocessing, there was a need to deduplicate rows for unique restaurant details in the first requirement(restaurants.csv). E.g. A restaurant with 5 events will be populated with five rows with duplicated restaurant details (res_id, name etc), but with different event details.

I have also decided to store the data in a Pandas Dataframe instead of using a Spark Dataframe, considering that the data is still relatively small (< 2000).

**Instructions on how to run the source code**
## How to Run the Jupyter Notebook (.ipynb)

To run the Jupyter Notebook provided in this repository, please follow these steps:

### 1. Install Jupyter Notebook or JupyterLab
If you don't have Jupyter installed, you can install it using Anaconda or pip.

**Using Anaconda:**
```bash
conda install -c conda-forge notebook






