# Travel Food Series Data Analysis

## Case Scenario
**Steven**, a travel blogger, intends to create a travel food series. He is analyzing data from Zomato for inspiration and aims to find restaurants with good user ratings and interesting past events.

### Assumptions and Interpretations
1. **Photo URL data**: Photo URLs are aggregated into a list for each event, to avoid duplicated records of same event
2. **Photo URL Retrieval**: The Photo URL data required for `restaurants_events.csv` is retrieved from:
   - Each Restaurant -> Each Event -> Photo -> Photo URL.

### Design Considerations
- ** Preprocessing Function**: 
  - Use of Pandas's json_normalize to flatten nested json objects into a dataframe, rather than iterating through the json object to extract data.
 
- **Local and Cloud adaptations**:
   - There are two different adaptations for the solution, one which can be run on the local machine using the notebook (`main.ipynb`), and the other which was run in my AWS Glue job (`main_glue.py`) 
 
## Architecture and Cloud Deployment Considerations

To deploy this solution, I have utilised `AWS Glue` or `Lambda` jobs to run the source code or script. Making use of `AWS S3`, the job will read source files from a specific bucket and write output files to the desired folder. In order to get this working, there is a need to create an IAM Role in AWS that has both read and write access to S3, as well as Glue Service Access.

S3 will be configured with a bucket that contains two folders, `Source` and `Output`, where the Source folder will contain the source data files, and the Output folder will contain the output files such as restaurant.csv and restaurant_events.csv.

![Alt text](Screenshots%20and%20diagrams/S3_Folders.jpg)  
![Alt text](Screenshots%20and%20diagrams/S3_OutputFiles.jpg)
![Alt text](Screenshots%20and%20diagrams/AWS_GlueScript.jpg)

There is also a function in the script to export a json file to the Source folder from a JSON URL link.

With this setup, whenever there is a change in the restaurant data or country codes, we can replace the source files in the dedicated S3 bucket, and manually run the glue job again to overwrite the output files in Output folder. 

One consideration of deploying this solution on AWS is the seamless process of generating the requirements.
By utilising `S3 Event Notifications` and `AWS Lambda` to detect any changes in the Source Data Files and automatically invoke the start of the glue job, making the generation of these output files a much more seamless process.
Alternatively, in the case where source data is uploaded or amended on regular intervals, we can consider the use of Amazon EventBridge to schedule the invocation of the glue job periodically.

## Architecture Diagram
![Alt text](Screenshots%20and%20diagrams/AWS_Architecture_Diagram.jpg)
The diagram shows two possible paths we can trigger the glue job. The first approach uses `S3 Event Notifications` and `AWS Lambda`, whille the second approach relies on a scheduler with the help of `AWS EventBridge`. Furthermore, if source files were to be moved to a database, we can utitilise `RDS` and extract data from there, instead of the Excel and JSON files.

## Documents Overview  
`AWS_Architecture_Diagram.jpg` - Photo of AWS Architecture Diagram  
`AWS_GlueScript.jpg` - Screenshot of AWS Glue Script used to execute and produce required output files
`complete_restaurant_data.csv` - All restaurant and event details, restaurant details may be duplicated depending on number of events held  
`Country-Code.xlsx` - Source file containg Country Codes and corresponding countries  
`main_glue.py` - Main python script used in AWS Glue job  
`restaurant_data.json` - JSON file retrieved from JSON URL link  
`restaurant_events.csv` - List of events and their corresponding restaurant details  
`restaurants.csv` - Unique List of restaurants and their details  
`S3_Folders.jpg` - Screenshot of AWS S3 directory  
`S3_OutputFiles.jpg` - Screenshot of AWS S3 Output Folder  
`thresholds.csv` - List of Rating texts and their correspondig min, max and average values  

## Instructions on How to Run the Source Code

### 1. Install required Python Packages

**Using Pip:**
```bash
pip install pandas json requests openpyxl
```

### 2. Run the Notebook

Open the Notebook and click on `Run All`
