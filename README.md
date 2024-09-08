# Building Data Pipeline Using Luigi

## Requirements and Gathering
### Problems: 
- The Sales Team has sales data in the PostgreSQL Database. But there is still a lot of missing data and the data format is incorrect.
- The Product Team also has data on Electronic Product Pricing in .csv format, but the data format is still messy and there are many missing values.
- The Data Scientist Team wanted to do research on NLP (Natural Language Processing) modelers, but has no data to read. The team asked the Data Engineer Team to gather the data through Web Scraping process from a website.
### Solutions:
- Collect data required from Web Scrapping Process, .csv file, and database then transform them into nice formatted data.
- Make Data Pipeline and its infrastructure to transform the data
- Store the transformed data into a data warehouse in PostgreSQL 
### Data Source
- [Sales Data](https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db) 
- [Marketing Data](https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view)
- [Web Scrapping (Kompas.com)](https://indeks.kompas.com/?site=all&page)
### Method
a. Extract and Validate Data
    - Collect the data
    - Understanding the data
    - Check data types and values
    - Check for missing value, duplicate, and inconcistent data
b. Transform
    - Cleaning the data
    - Filter unusable column
    - Change column's name
    - Formating the table
c. Load
    - Save the transformed data into data warehouse in PostgreSQL
d. Scheduling
    - Make scheduled pipeline
    - Using crontab to run pipleline at a certain period
e. Testing  
    - Scenario 1 : making sure the pipeline is running and the data has moved into the same database
    - Scenario 2 : making sure the pipeline has been scheduled
    

## Designing ETL Pipeline
