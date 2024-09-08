import pandas as pd
import luigi
import requests
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import function as func
from pangres import upsert

class ExtractSalesData(luigi.Task):
    
    def requires(self):
        pass
    
    def output(self):
        return luigi.LocalTarget("raw/extracted_sales_data.csv")
    
    def run(self):
        conn = create_engine("postgresql://postgres:password123@localhost:5480/etl_db")
        query = "SELECT * FROM public.amazon_sales_data"
        extract_sales_data = pd.read_sql(query, conn)
        
        extract_sales_data.to_csv(self.output().path, index = False)

class ExtractMarketingData(luigi.Task):
    
    def requires(self):
        pass
    
    def output(self):
        return luigi.LocalTarget("raw/extracted_marketing_data.csv")
    
    def run(self):
        extract_marketing_data = pd.read_csv("ElectronicsProductsPricingData.csv")
        
        extract_marketing_data.to_csv(self.output().path, index = False)

class ExtractAPIKompas(luigi.Task):
    def requires(self):
        pass
    
    def output(self):
        return luigi.LocalTarget("raw/extracted_kompas_news.csv")
    
    def run(self):
        # menyimpan iterasi link 
        links = []
        for page in tqdm(range(1, 6)): 
            get_link=f"https://indeks.kompas.com/?site=all&page={page}"
            links.append(get_link)
            time.sleep(0.25)
            
        news_kompas_link = pd.DataFrame(links, columns = ['url'])    
        news_kompas_link.to_csv("news_kompas_ini_lagi.csv", index = False)

        links = pd.read_csv("news_kompas_ini_lagi.csv")
        
        # melakukan proses ekstrak
        full_data = []
        for idx in tqdm(range(len(links))):
            try:
                get_link = links["url"].iloc[idx]
                resp = requests.get(get_link)
                soup = BeautifulSoup(resp.text, 'html.parser')
        
                articles = soup.find_all('div', class_='articleItem')
                for article in articles:
                    title = article.find('h2', class_="articleTitle").text.strip()
                    category = article.find('div', class_="articlePost-subtitle").text.strip()
                    date = article.find('div', class_="articlePost-date").text.strip().replace('/', '-')
                    url = article.find('a', class_="article-link")['href']
                    img = article.find('div', class_="articleItem-img").find('img')['src']
    
                    current_timestamp = pd.Timestamp.now()
                    formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    scrapped_at = formatted_timestamp
    
                    article_results = { 
                        'news_title' : title, 
                        'news_category' : category, 
                        'date_news' : date,
                        'scrapped_at' : scrapped_at,
                        'url' : url,
                        'image' : img
                    }
                    full_data.append(article_results)
                    time.sleep(0.5)
            except:
                raise Exception("There is some error")
                
        extract_kompas_news = pd.DataFrame(full_data)
        
        extract_kompas_news.to_csv(self.output().path, index = False)

class ValidateData(luigi.Task):
    def requires(self):
        return [ExtractSalesData(), 
                ExtractMarketingData(), 
                ExtractAPIKompas()]
    
    def output(self):
        pass
    
    def run(self):
        # read sales data
        validate_sales_data = pd.read_csv(self.input()[0].path)
        
        # read marketing data
        validate_marketing_data = pd.read_csv(self.input()[1].path)
        
        # read kompas news data
        validate_kompas_data = pd.read_csv(self.input()[2].path)
        
        # validate sales data
        func.validation_process(data = validate_sales_data,
                          table_name = "sales")
        # validate marketing data
        func.validation_process(data = validate_marketing_data,
                          table_name = "marketing")
        # validate kompas data
        func.validation_process(data = validate_kompas_data,
                          table_name = "kompas")

class TransformSalesData(luigi.Task):

    def requires(self):
        return ExtractSalesData()
    
    def output(self):
        return luigi.LocalTarget("transformed/transformed_sales_data.csv")
    
    def run(self):
        # read data from previous source
        sales_data = pd.read_csv(self.input().path)

        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].fillna(0)

        sales_data['currency'] = sales_data['actual_price'].iloc[0][:1]

        sales_data['actual_price'] = sales_data['actual_price'].str.replace('₹', '')
        sales_data['discount_price'] = sales_data['discount_price'].str.replace('₹', '')

        sales_data['actual_price'] = sales_data['actual_price'].str.replace(',', '')
        sales_data['discount_price'] = sales_data['discount_price'].str.replace(',', '')

        sales_data['actual_price'] = sales_data['actual_price'].replace('', float('nan'))
        sales_data['discount_price'] = sales_data['discount_price'].replace('', float('nan'))

        sales_data['actual_price'] = pd.to_numeric(sales_data['actual_price'], errors='coerce')
        sales_data['discount_price'] = pd.to_numeric(sales_data['discount_price'], errors='coerce')

        sales_data['ratings'] = sales_data['ratings'].str.replace(',', '')
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].str.replace(',', '')

        sales_data['ratings'] = sales_data['ratings'].replace('', float('nan'))
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].replace('', 0)

        sales_data['ratings'] = pd.to_numeric(sales_data['ratings'], errors='coerce')
        sales_data['no_of_ratings'] = pd.to_numeric(sales_data['no_of_ratings'], errors='coerce')

        sales_data = sales_data.drop(columns="Unnamed: 0")

        sales_data.to_csv(self.output().path, index = False)

class TransformMarketingData(luigi.Task):

    def requires(self):
        return ExtractMarketingData()
    
    def output(self):
        return luigi.LocalTarget("transformed/transformed_marketing_data.csv")
    
    def run(self):
        marketing_data = pd.read_csv(self.input().path)

        cols_under_constraint = []
        for column in marketing_data.columns:
            percent = marketing_data[column].isna().sum() / len(marketing_data[column]) * 100

            if percent > 50:
                cols_under_constraint.append(column)
                
        marketing_data = marketing_data.drop(columns=cols_under_constraint)
        marketing_data.reset_index()

        marketing_data['prices.shipping'] = marketing_data['prices.shipping'].fillna('Unknown')

        marketing_data['weight_unit'] = ''
        if marketing_data['weight'].str.contains('pounds').any():
            marketing_data['weight_unit'] = 'pounds'
        else:
            marketing_data['weight_unit'] = 'other'
    
        if len(marketing_data['weight_unit'].unique()) == 1:
            marketing_data['weight'] = marketing_data['weight'].str.replace('pounds', '')
            marketing_data['prices.amountMax'] = marketing_data['prices.amountMax'].astype('float')

        marketing_data['dateAdded'] = pd.to_datetime(marketing_data['dateAdded'])
        marketing_data['dateUpdated'] = pd.to_datetime(marketing_data['dateUpdated'])
  
        price_availability = {
                'Yes' : 'In Stock', 
                'In Stock' : 'In Stock', 
                'TRUE' : 'In Stock', 
                'undefined' : 'No Information', 
                'yes' : 'In Stock', 
                'Out Of Stock' : 'Out Of Stock', 
                'Special Order' : 'In Stock', 
                'No' : 'Out Of Stock', 
                'More on the Way' : 'In Stock', 
                'sold' : 'Out Of Stock', 
                'FALSE' : 'Out Of Stock', 
                'Retired' : 'Out Of Stock', 
                '32 available' : 'In Stock', 
                '7 available' : 'In Stock'
                }
        price_condition = {
                'New' : 'New', 
                'new' : 'New', 
                'Seller refurbished' : 'Refurbished', 
                'Used' : 'Used', 
                'pre-owned' : 'Used', 
                'Refurbished' : 'Refurbished', 
                'Manufacturer refurbished': 'Refurbished', 
                'New other (see details)': 'New'
                }
        marketing_data['prices.availability'] = marketing_data['prices.availability'].replace(price_availability)
        marketing_data['prices.condition'] = marketing_data['prices.condition'].replace(price_condition)

        marketing_data['prices.condition'] = marketing_data['prices.condition'].fillna('Unknown')
  
        marketing_data['prices.amountMin'] = marketing_data['prices.amountMin'].astype('float')
        marketing_data['prices.amountMax'] = marketing_data['prices.amountMax'].astype('float')

        RENAME_COLS = {
            "prices.amountMax" : "max_price_amount",
            "prices.amountMin" : "min_price_amount",
            "prices.availability" : "product_availability",
            "prices.condition" : "product_condition",
            "prices.currency" : "currency",
            "prices.dateSeen" : "date_seen",
            "prices.isSale" : "is_product_sale",
            "prices.merchant" : "product_merchant",
            "prices.shipping" : "product_shipping",
            "prices.sourceURLs" : "old_url",
            "asins" : "identification_number",
            "brand" : "brand",
            "categories" : "category",
            "dateAdded" : "created_at",
            "dateUpdated" : "date_updated",
            "imageURLs" : "image",
            "keys" : "key",
            "manufacturerNumber" : "manufacturer_number",
            "name" : "name",
            "primaryCategories" : "primary_category",
            "sourceURLs" : "new_url",
            "upc" : "upc",
            "weight" : "weight",
            "weight_unit" : "weight_unit"
             }
        marketing_data = marketing_data.rename(columns = RENAME_COLS)
        
        # save the output to csv
        marketing_data.to_csv(self.output().path, index = False)

class TransformAPIKompas(luigi.Task):

    def requires(self):
        return ExtractAPIKompas()
    
    def output(self):
        return luigi.LocalTarget("transformed/transformed_kompas_news.csv")
    
    def run(self):
        # read data from previous source
        kompas_data = pd.read_csv(self.input().path)

        #Convert Date Column
        kompas_data['date_news'] = pd.to_datetime(kompas_data['date_news'])
        # save the output to csv
        kompas_data.to_csv(self.output().path, index = False)

class LoadData(luigi.Task):

    def requires(self):
        return [TransformSalesData(),
                TransformMarketingData(),
                TransformAPIKompas()]
    
    def output(self):
        return [luigi.LocalTarget("loaded/loaded_sales_data.csv"),
                luigi.LocalTarget("loaded/loaded_marketing_data.csv"),
                luigi.LocalTarget("loaded/loaded_kompas_data.csv"),]
    
    def run(self):
        # read data from previous task
        load_sales_data = pd.read_csv(self.input()[0].path)
        load_marketing_data = pd.read_csv(self.input()[1].path)
        load_kompas_data = pd.read_csv(self.input()[2].path)

        # init data warehouse engine
        dw_engine = create_engine("postgresql://etl_db_owner:IF30NeLyHWhR@ep-square-silence-a5pu220y.us-east-2.aws.neon.tech/etl_db?sslmode=require")

        dw_table_sales = "etl_sales_table"
        dw_table_marketing = "etl_marketing_table"
        dw_table_kompas = "etl_kompas_table"

        # insert data to data warehouse
        load_sales_data.to_sql(name = dw_table_sales,
                               con = dw_engine,
                               if_exists = "append",
                               index = False)
        
        load_marketing_data.to_sql(name = dw_table_marketing,
                               con = dw_engine,
                               if_exists = "append",
                               index = False)

        load_kompas_data.to_sql(name = dw_table_kompas,
                               con = dw_engine,
                               if_exists = "append",
                               index = False)
       
        load_sales_data.to_csv(self.output()[0].path, index = False)
        load_marketing_data.to_csv(self.output()[1].path, index = False)
        load_kompas_data.to_csv(self.output()[2].path, index = False)

if __name__ == "__main__":
    
    luigi.build([ExtractSalesData(),
                ExtractMarketingData(),
                ExtractAPIKompas(),
                ValidateData(),
                TransformSalesData(),
                TransformMarketingData(),
                TransformAPIKompas(),
                LoadData()], local_scheduler = True)
