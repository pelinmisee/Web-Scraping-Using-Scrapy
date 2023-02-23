import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import json
import csv
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, VARCHAR, select, Boolean
import pandas as pd
import subprocess
Base=declarative_base()


class Data(Base):
    #TODO: add column SOLD with default value False
    __tablename__ = 'data'
    link = Column(VARCHAR(100), primary_key=True)
    price = Column(String)
    wonen = Column(String)
    perceel = Column(String)
    bedroom_amount = Column(String)
    description = Column(VARCHAR(1000000))
    is_sold = Column(String, default=False)

class PostgreSQLStore():
    def __init__(self):
        USERNAME = read_config("config.json")["USERNAME"]
        PASSWORD = read_config("config.json")["PASSWORD"]
        HOST = read_config("config.json")["HOST"]
        DATABASE = read_config("config.json")["DATABASE"]
        self.engine = create_engine(f"postgresql://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

class LoadDataFromCSV():
    def __init__(self):
        self.session = PostgreSQLStore().Session()
        self.engine = PostgreSQLStore().engine

    def load(self):
        
        #get link column from data table
        df = pd.read_sql_query('select link from data', con=self.engine)
        #convert link column to list
        link_list = df['link'].tolist()

        with open("homes.csv", "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader) # skip the header row
            for row in reader:
                if row[0] not in link_list:
                    data = Data(link=row[0], price=row[1], wonen=row[2], perceel=row[3], bedroom_amount=row[4], description=row[5])
                    self.session.add(data)
                    self.session.commit()

    def update_sold(self):
        #get link column from data table
        df = pd.read_sql_query('select link from data', con=self.engine)
        #convert link column to list
        link_list = df['link'].tolist()
        with open("sold_list.json", "r", encoding="utf-8") as f:
            sold_list = json.load(f)
        for link in sold_list:
            if link["sold_house_link"] in link_list:
                self.engine.execute("UPDATE data SET is_sold = 'True' WHERE link = '{}'".format(link["sold_house_link"]))
                self.session.commit()
                
def read_config(config_file_path:str) -> dict:
    config_file = {}
    with open(config_file_path, "r") as config_file:
        config_file = json.load(config_file)
    return config_file

            
def parse_price(price_list):
    try:
        return (price_list[0])
    except IndexError:
        return "price not found"

def parse_wonen(square_meters):
    try:
        return square_meters[0]
    except IndexError:
        return "square meters not found"


def parse_perceel(square_meters):
    if len(square_meters) == 3:
        try:
            return square_meters[1]
        except IndexError:
            return "perceel not found"
    else:
        return "perceel not found"

def parse_bedroom(square_meters):
    if len(square_meters) == 3:
        try:
            return square_meters[2]
        except IndexError:
            return "bedroom not found"
    else:
        return square_meters[-1]

def parse_description(description):
    description=str(description)
    description=description.replace("['\\r\\n", "")
    description=description.replace("\\r\\n    ']", "")
    description=description.replace("[\"\\r\\n","")
    description=description.replace("'", "")
    description=description.replace(",","")
    description=description.replace("\\r\\n    \"]","")
    return description

def parse_links(list):
    new_list = []
    for link in list:
        link = "https://www.funda.nl" + link
        new_list.append(link)
    return new_list

def merge_list(list1,list2,list3):
    return set(list1 + list2 +list3)

class Q5MainSpider(scrapy.Spider):
    name = "q5_main_2"
    allowed_domains = ["www.funda.nl"]
    start_urls = ["https://www.funda.nl/koop/heel-nederland/"]

 
    def start_requests(self):
        with open("urls.json", 'r') as f:
            rooms = json.load(f)
        for room in rooms:
            yield scrapy.Request(url=room['link'], callback=self.parse)


    def parse(self, response):
        link = response.url
        price=response.xpath('//strong[@class="object-header__price"]/text()').getall()
        price=parse_price(price)
        square_meters=response.xpath('//span[@class="kenmerken-highlighted__value fd-text--nowrap"]/text()').getall()
        wonen=parse_wonen(square_meters)
        perceel=parse_perceel(square_meters)
        bedroom_amount=parse_bedroom(square_meters)
        description=response.xpath('//div[@class="object-description-body"]/text()').getall()
        description=parse_description(description)

        with open ("homes.csv","r+",encoding="utf-8", newline='') as f:
            if link in f.read() or link == "https://www.funda.nl/koop/heel-nederland/":
                pass
            else:
                writer = csv.writer(f)
                writer.writerow([link,price,wonen,perceel,bedroom_amount,description])


class Q5ScrapSpider(scrapy.Spider):
    name = "q5_main"
    allowed_domains = ["www.funda.nl"]
    #there are more than 4000 pages, I just scrape the first 100 pages
    start_urls = ["https://www.funda.nl/koop/heel-nederland/p{}/".format(i) for i in range(2,40)]

    def parse(self, response):
        link1 = response.xpath("//div[@class='search-content-output']/ul/li/div//a[@class='top-position-object-link top-position-object is-backgroundcover']//@href").getall()
        
        link2 = response.xpath("//div[@class='search-content-output']/ol/li/div/a/@href").getall()
        
        link3 = response.xpath("//li[@class='search-result']/div/div[@class='search-result-content']/div/div/div/a/@href").getall()

        merged = merge_list(link1 , link2 , link3)
        merged = parse_links(merged) 
        data = [{"link":each_data} for each_data in merged]

        with open('urls.json','w',encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=True, indent=4 )            
            

if __name__ == "__main__":
    """that will provide update for
       new data load to database 
       check for sold houses

    """
    Data()
    PostgreSQLStore()
    while True:
        command="scrapy crawl q5_main"
        subprocess.run(command, shell=True)
        command="scrapy crawl q5_main_2"
        subprocess.run(command, shell=True)
        LoadDataFromCSV().load()
        LoadDataFromCSV().update_sold()
        print("sleeping for 5 seconds")
        time.sleep(5)

