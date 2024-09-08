# Building Data Pipeline Using Luigi

## Requirements and Gathering
### Problems
- The Sales Team has sales data in the PostgreSQL database. But there is still a lot of missing data and the data format is incorrect.
- The Product Team also has data on electronic eroduct pricing in `.csv` format, but the data format is still messy and there are many missing values.
- The Data Scientist Team wanted to do research on NLP (Natural Language Processing) modelers, but has no data to read. The team asked the Data Engineer Team to gather the data through Web Scraping process from a website.
### Solutions
- Collect data required from Web Scrapping Process, `.csv` file, and database then transform them into nice formatted data.
- Make Data Pipeline and its infrastructure to transform the data
- Store the transformed data into a data warehouse in PostgreSQL 
### Data Sources
- [Sales Data](https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db) 
- [Marketing Data](https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view)
- [Web Scrapping (Kompas.com)](https://indeks.kompas.com/?site=all&page)
  > _Desclaimer: This website is allowed to perform web scrapping_
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
![pipelineku](https://github.com/user-attachments/assets/6f82efa0-a0c6-4c6e-8630-94da05136bbc "ETL Pipeline Design")
  ETL (Extract, Transform, Load) design involves planning and structuring the data pipeline to efficiently move and transform data from source systems to a target system. It requires careful consideration of factors such as data sources, target systems, transformation requirements, performance considerations, and scalability. We first extract the three files from our data source to validate the data. And then we also extract them to transform into formatted data. After that, we load the three transformed file into a data warhouse in PostgreSQL database.

## Implementing ETL Pipeline
### Building ETL Pipeline
We build data papeline using ETL (Extract, Transform, Load) method which in each stage has a different approach and tools and affect how we design and build the pipeline. We use script-base ETL using programming language to build custom pipeline. 

### Tools
- Python
- Luigi
- BeautifulSoup
- Docker
- PostgreSQL
- DBeaver
