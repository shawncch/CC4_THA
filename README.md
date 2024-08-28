# Travel Food Series Data Analysis

## Case Scenario
**Steven**, a travel blogger, intends to create a travel food series. He is analyzing data from Zomato for inspiration and aims to find restaurants with good user ratings and interesting past events.

### Assumptions or Interpretations
1. **Event Filtering**: Event Start Dates are used to filter for events that took place in April 2019, regardless of the Event End Date. For example, an event that starts on 14 April 2019 and ends on 18 May 2019 is still considered an event that took place in April 2019.
2. **Photo URL Retrieval**: The Photo URL data required for `restaurants_events.csv` is retrieved from:
   - Each Restaurant -> Each Event -> Photo -> Photo URL.

### Design Considerations
- **Unified Preprocessing Function**: 
  - Since the desired output CSV files for the first and second requirements require iterating through the JSON file to extract the necessary values, a single main preprocessing function is used to handle data processing for both requirements.
  - This approach avoids redundancy but necessitates deduplication of rows for unique restaurant details in the first requirement (`restaurants.csv`). For example, a restaurant with 5 events would initially have five rows with duplicated restaurant details (e.g., `res_id`, `name`) but different event details.

- **Data Handling**:
  - A **Pandas DataFrame** is used for data storage and processing instead of a Spark DataFrame, considering the data size is relatively small (< 2000 records).

## Instructions on How to Run the Source Code

### 1. Install Jupyter Notebook or JupyterLab
If you don't have Jupyter installed, you can install it using Anaconda or pip.

**Using Anaconda:**
```bash
conda install -c conda-forge notebook
```

### 2. Install required Python Packages

**Using Pip:**
```bash
pip install pandas json requests openpyxl
```

### 3. Run the Notebook

Open the Notebook and click on Run All
