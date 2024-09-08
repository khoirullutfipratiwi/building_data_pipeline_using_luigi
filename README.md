# Building Data Pipeline Using Luigi

## Requirements and Gathering
### Problems
- The Sales Team has sales data in the PostgreSQL Database. But there is still a lot of missing data and the data format is incorrect.
- The Product Team also has data on Electronic Product Pricing in `.csv` format, but the data format is still messy and there are many missing values.
- The Data Scientist Team wanted to do research on NLP (Natural Language Processing) modelers, but has no data to read. The team asked the Data Engineer Team to gather the data through Web Scraping process from a website.
### Solutions
- Collect data required from Web Scrapping Process, `.csv` file, and database then transform them into nice formatted data.
- Make Data Pipeline and its infrastructure to transform the data
- Store the transformed data into a data warehouse in PostgreSQL 
### Data Sources
- [Sales Data](https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db) 
- [Marketing Data](https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view)
- [Web Scrapping (Kompas.com)](https://indeks.kompas.com/?site=all&page)
  > _Desclaimer:__The__used website is allowed to perform web scrapping_
### Methods
1. Extract and Validate Data
    - [x] Collect the data
    - [x] Understanding the data
    - [x] Check data types and values
    - [x] Check for missing value, duplicate, and inconcistent data
2. Transform
    - [x] Cleaning the data
    - [x] Filter unusable column
    - [x] Change column's name
    - [x] Formating the table
3. Load
    - [x] Save the transformed data into data warehouse in PostgreSQL
4. Scheduling
    - [x] Make scheduled pipeline
    - [x] Using crontab to run pipleline at a certain period
5. Testing  
    - [x] Scenario 1 : making sure the pipeline is running and the data has moved into the same database
    - [x] Scenario 2 : making sure the pipeline has been scheduled
    

## Designing ETL Pipeline
