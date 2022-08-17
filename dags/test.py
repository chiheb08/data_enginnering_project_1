from asyncio import tasks
from os import path
import csv
import os 
import requests
import pandas as pd
import datetime as dt
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.mysql_operator import MySqlOperator
from bs4 import BeautifulSoup
from airflow.configuration import conf
import re
import random
import functools




            
#This function prepares the environment to start scraping the data from the website
def parse_bicycles(**context):
    path = '/usr/local/airflow/store_files_airflow/Data_sources/'
    with open(path + 'bicycles.txt', encoding='utf8') as f:
        entries = f.readlines()

    for entry in entries:
        r = requests.get( entry, headers={"Accept-Language":"de-de"} )
        if r.status_code == 200:
            soup = BeautifulSoup( r.content , 'html.parser')
            #Finding Frarhad Category
            category_tag = soup.findAll(class_="refinement-link cyc-padding_right-4")[0:10] 
            farrhad_category= {i.text:[i["href"]] for i in category_tag} 
            for i in farrhad_category:
                URL_cat = farrhad_category[i][0]
                page_cat = requests.get( URL_cat, headers={"Accept-Language":"de-de"} )
                soup_cat = BeautifulSoup( page_cat.content , 'html.parser')
                number_of_pages_tag = soup_cat.findAll( lambda tag:  tag.get("class")== ["pagination__listitem"])
                if number_of_pages_tag==[]:
                    farrhad_category[i].append(1)
                else:
                    number_of_pages =number_of_pages_tag[-1].text.replace("\n","")
                    farrhad_category[i].append(int(number_of_pages))
    bicycle_categories = context['task_instance']
    bicycle_categories.xcom_push(key="parse_bicycles", value=farrhad_category) 





#This function scrape data from the web site "https://www.fahrrad.de/fahrraeder/e-bikes/" 
def scrape_bicycles(**kwargs):
    ti = kwargs['ti']
    farrhad_category = ti.xcom_pull(key='parse_bicycles', task_ids='parse_bicycles')
    dict_result ={"id":[],"category":[],"brand_name":[], "model_name":[],"price":[],"variations":[],"is_available":[]}
    images={"brand_name":[],"model_name":[],"image_link":[]}
    for category in farrhad_category : 
        link = farrhad_category[category][0]
        number_of_pages = farrhad_category[category][1]
        for p in range(number_of_pages):
            URL_page=link+f"?page={p+1}&sz=48"
            print(URL_page)
            page_page = requests.get( URL_page, headers={"Accept-Language":"de-de"} )
            soup_page = BeautifulSoup( page_page.content , 'html.parser')
            tag_article = soup_page.findAll(class_= "product-tile-inner")
            for article in tag_article:
                dict_result["category"].append(category)
                dict_result["brand_name"].append(article.findAll(class_="cyc-typo_subheader cyc-color-text")[0].text.replace("\n","").strip())
                dict_result["model_name"].append(article.findAll(class_="product-name cyc-typo_body cyc-color-text_secondary")[0].text.replace("\n",""))
                dict_result["id"].append(article.findAll(class_="thumb-link is-absolute cyc-top_0 cyc-right_0 cyc-font-size-0 js_producttile-link")[0]["href"].split("=")[-2].replace("&cgid",""))
                dict_result["price"].append(article.findAll(class_="price-standard")[0].text.replace("\n","").replace("€ ","").replace(",00","").replace(".","").strip())
                variation_tag = article.findAll(class_="variations")
                if variation_tag ==[]:
                    dict_result["variations"].append("no variations")
                else:
                    dict_result["variations"].append(variation_tag[0].text.replace("\n",""))
                if article.findAll(class_= "c-product-notavailable is-hidden js-update-availability js-product-notavailable")==[]:
                    dict_result["is_available"].append(1)
                else:
                    dict_result["is_available"].append(0)
                images["brand_name"].append(article.findAll(class_="cyc-typo_subheader cyc-color-text")[0].text.replace("\n","").strip())
                images["model_name"].append(article.findAll(class_="product-name cyc-typo_body cyc-color-text_secondary")[0].text.replace("\n",""))
                images["image_link"].append(article.img['data-src'])
        

    data =pd.DataFrame.from_dict(dict_result)
    images_bicycles=pd.DataFrame.from_dict(images) 
    data["DATE"] = pd.to_datetime("today").strftime("%Y-%m-%d")
    images_bicycles["DATE"]=pd.to_datetime("today").strftime("%Y-%m-%d") 
    bicycle_data = kwargs['task_instance']
    bicycle_data.xcom_push(key="bicycle_row_data", value=data)
    bicycle_data.xcom_push(key="bicycle_data_images", value=images_bicycles)







#download image bicycle : 
def download_bicycle_data(**kwargs):
    ti = kwargs['ti'] 
    # download Bicycle row data : 
    data=ti.xcom_pull(key='bicycle_row_data', task_ids='scrape_bicycles')
    date_scraped = datetime.today().strftime('%m-%d-%Y')
    file_name='bicycle_data_'+date_scraped+'.csv'
    file_name=file_name.replace("-","_")
    data.to_csv(f'~/store_files_airflow/Row_data/{file_name}', index=False)
    ## download Bicycle Image Metadata : 
    image_data=ti.xcom_pull(key='bicycle_data_images', task_ids='scrape_bicycles')
    image_file_name='bicycle_data_image_'+date_scraped+'.csv'
    image_file_name=image_file_name.replace("-","_") 
    image_data.to_csv(f'~/store_files_airflow/Image_data/Metadata/{image_file_name}', index=False)
    bicycle_data = kwargs['task_instance']
    bicycle_data.xcom_push(key="bicycle_row_data", value=data)
    bicycle_data.xcom_push(key="bicycle_image_Metadata", value=image_data)



#This function Validates the file names located in our storage layer :  
def validate_row_data_files(**kwargs): 
    #file name should be : bicycle_data_**_**_****.csv (dd_mm_yyyy: the current date)
    path = '/usr/local/airflow/store_files_airflow/Row_data' # this is 
    files=os.listdir(path) 
    #this is the number 
    nb=0
    for f in files : 
        r=re.match(r'^bicycle_data_[0-9]{1,2}_[0-9]{1,2}_[0-9]{4}.csv$',f) 
        if r : 
            nb=nb+1 
    if nb == len(files) :
        msg=f"{nb}/{len(files)} valid" 
        return msg
    else :
        msg= f"still {len(files)-nb} not valid"
        return msg



# validate image Metadata : 
def validate_image_Metadata(**kwargs): 
    #file name should be : characters_****_**_**.csv (dd_mm_yyyy: the current date)
    path = '/usr/local/airflow/store_files_airflow/Image_data/Metadata' # this is 
    files=os.listdir(path) 
    #this is the number 
    nb=0
    for f in files : 
        r=re.match(r'^bicycle_data_image_[0-9]{1,2}_[0-9]{1,2}_[0-9]{4}.csv$',f) 
        if r : 
            nb=nb+1 
    if nb == len(files) :
        msg=f"{nb}/{len(files)} valid" 
    else :
        msg= f"still {len(files)-nb} not valid"
    return msg 





def validate_reports_Files(**kwargs): 
    #file name should be : bicycle_data_image_**_**_****.csv (dd_mm_yyyy: the current date)
    path = '/usr/local/airflow/store_files_airflow/Reports' # this is 
    files=os.listdir(path) 
    categories=['E-Bikes City','E-Bikes Cross','E-Bikes Kinder','E-Bikes Rennrad & Gravel','E-Bikes Trekking','E-Bikes Urban','E-Mountainbikes','Lastenfahrräder','S-Pedelecs 45kmh','SUV E-Bikes']
    nb=0
    for c in categories : 
        for f in files : 
            r=re.match(f'^{c}',f) 
            if r : 
                nb=nb+1 
        if nb == len(files) :
            msg=f"{nb}/{len(files)} valid" 
        else :
            msg= f"still {len(files)-nb} not valid"
    return msg



#remove duplicates from data : 
def drop_duplicates(**kwargs) : 
    ti = kwargs['ti']
    data=ti.xcom_pull(key='bicycle_row_data', task_ids='scrape_bicycles')
    data=data.drop_duplicates() 
    drop_duplicates = kwargs['task_instance']
    drop_duplicates.xcom_push(key="data_no_duplicates", value=data)





 
def download_image(link,destination,image_name):
    image = requests.get(link).content
    path=f'{destination}/{image_name}'
    with open(path, "wb+") as f:
        f.write(image)



#download images :
def download_bicycle_pictures(**kwargs): 
    ti = kwargs['ti']
    image_data=ti.xcom_pull(key='bicycle_data_images', task_ids='scrape_bicycles')
    cnt=1
    for link in image_data['brand_name'].unique() : 
        directory = f"{link}"
        directory = directory.replace(":","")
        path =f'/usr/local/airflow/store_files_airflow/Image_data/Images/{directory}'
        try:
            os.makedirs(path)
            print("Directory '%s' created successfully" % directory)
        except OSError as error : 
            print("Directory '%s' created successfully" % directory)
        d=image_data[image_data['brand_name']==link]
        d.reset_index(inplace=True)
        for i in d.index : 
            image_name=d['brand_name'].iat[i]+'_'+d['model_name'].iat[i]+'.jpg'
            image_name=image_name.replace("/","") 
            image_name=image_name.replace(" ","_")
            image_name=image_name.replace('"',"") 
            download_image(d['image_link'].iat[i],path,image_name)


    

#Processing tasks : 
#Convert price to float : 
def convert_price_to_float(**kwargs) : 
    path="/usr/local/airflow/store_files_airflow/Row_data/"
    files=os.listdir(path) 
    for f in files :
        data=pd.read_csv(path+f,sep=',')
        data['price']=data['price'].apply(lambda x:x.replace(',','.')) 
        data['price']=pd.to_numeric(data['price'],downcast="float") 
        data.to_csv(f'~/store_files_airflow/pre_processed_data/{f}')



def valdidate_row_files_schema(**kwargs) : 
    path="/usr/local/airflow/store_files_airflow/pre_processed_data/"
    files=os.listdir(path) 
    for f in files :
        data=pd.read_csv(path+f,sep=',')
        data['price']=data['price'].apply(lambda x:x+round(random.uniform(-450,450),3) ) 
        data.to_csv(f'~/store_files_airflow/pre_processed_data/{f}')





    





#Create daily reports for every E-bike : 
def create_daily_reports(**kwargs) : 
    path='/usr/local/airflow/store_files_airflow/pre_processed_data/' 
    files=os.listdir(path)  
    for f in files : 
        data=pd.read_csv(path+f,sep=",")
        date=data['DATE'][0]
        new_df=data.groupby(['category','brand_name','DATE'],as_index=False)['price'].sum()
        UniqueCategories = new_df.category.unique()
        #create a data frame dictionary to store your data frames
        DataFrameDict = {elem : pd.DataFrame() for elem in UniqueCategories}
        for key in DataFrameDict.keys():
            DataFrameDict[key] = new_df[:][new_df.category == key]
            DataFrameDict[key].reset_index(inplace=True)
            category=DataFrameDict[key]['category'][0]
            date=DataFrameDict[key]['DATE'][0]
            name=category+'_'+date+'.csv'
            name=name.replace("-","_")
            name=name.replace("/","")
            for i in DataFrameDict[key]['category'].unique() : 
                try:
                    i=i.replace("/","")
                    directory=f'/usr/local/airflow/store_files_airflow/Reports/{i}'
                    os.makedirs(directory)
                    print("Directory '%s' created successfully" % i)
                except OSError as error : 
                    print("Directory '%s' created successfully" % i)
                DataFrameDict[key].to_csv(f'{directory}/{name}')

def concat_data_all_dates(**kwargs) : 
    path_directory = '/usr/local/airflow/store_files_airflow/pre_processed_data'
    files=os.listdir(path_directory)
    datasets=[]
    for f in files : 
        data_path=os.path.join(path_directory,f)
        data=pd.read_csv(data_path,sep=',')
        data.drop(data.columns[[0,1]],axis=1,inplace=True)
        datasets.append(data)
    concatenated_data=pd.concat(datasets,axis=0)
    concatenated_data.to_csv('/usr/local/airflow/store_files_airflow/Dashboards/report.csv')







def concat_datasets(files:list) : 
    concatenated_data=pd.concat(files,axis=0)
    concatenated_data.reset_index(inplace=True)
    date = pd.to_datetime("today").strftime("%Y-%m-%d")
    name=concatenated_data['category'][0]+'_'+date+'.csv'
    name=name.replace("-","_")
    name=name.replace("/","")
    concatenated_data.to_csv(f'/usr/local/airflow/store_files_airflow/Concat_data/{name}') 



def concat_data(**kwargs) :
    category=kwargs['category']
    path_directory = '/usr/local/airflow/store_files_airflow/Reports'
    path_files=os.path.join(path_directory,category)
    files=os.listdir(path_files)
    datasets=[]
    for f in files : 
        data_path=os.path.join(path_files,f)
        data=pd.read_csv(data_path,sep=',')
        data.drop(data.columns[[0,1]],axis=1,inplace=True)
        datasets.append(data)
    concat_datasets(datasets)
    return datasets



def drop_columns(**kwargs) : 
    path_directory = '/usr/local/airflow/store_files_airflow/Concat_data'
    files=os.listdir(path_directory) 
    for f in files : 
        data_path=os.path.join(path_directory,f)
        data=pd.read_csv(data_path,sep=',')
        date=pd.to_datetime("today").strftime("%Y-%m-%d") 
        name=data['category'][0]+'_'+date+'.csv'
        name=name.replace("-","_")
        name=name.replace("/","")
        data.drop(['Unnamed: 0','index'],axis=1,inplace=True)
        data.to_csv(f'/usr/local/airflow/store_files_airflow/Cleansed_data/{name}')



def get_consistent_model_names(**kwargs) : 
    consistent = [] 
    path = '/usr/local/airflow/store_files_airflow/Dashboards/report.csv' 
    data=pd.read_csv(path,sep=',')
    for i,j in data['model_name'].value_counts().iteritems() : 
        if j > 50 : 
            consistent.append(i) 
    new_data=data[data['model_name'].isin(consistent)]
    new_data.to_csv('/usr/local/airflow/store_files_airflow/Dashboards/consistent.csv')
    
     
def store_time_series_data(**kwargs) : 
    path='/usr/local/airflow/store_files_airflow/Dashboards/consistent.csv' 
    data=pd.read_csv(path,sep=',')
    UniqueNames = data.model_name.unique()
    DataFrameDict = {elem : pd.DataFrame() for elem in UniqueNames}
    for key in DataFrameDict.keys():
        DataFrameDict[key] = data[:][data.model_name == key]
        DataFrameDict[key].reset_index(inplace=True) 
        DataFrameDict[key].drop(['index','Unnamed: 0','Unnamed: 0.1','id','category','brand_name','variations','is_available'],axis=1,inplace=True)
        name=DataFrameDict[key]['model_name'][0]+'.csv' 
        name=name.replace("/","")
        DataFrameDict[key].to_csv(f'/usr/local/airflow/store_files_airflow/TimeSeries_Data/{name}')


    

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022,4,27),
    'concurrency': 1,
    'retries': 0, 
    'provide_context': True
}



with DAG('scrape_bicycle_data',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/25 * * * *',
         template_searchpath=['/usr/local/airflow/sql_files']
         ) as dag:


    # prepare the environment for web scraping   
    opr_parse_bicycles = PythonOperator(task_id='parse_bicycles',python_callable=parse_bicycles,provide_context=True)
    # organize data scraped into a dataframe : 
    opr_scrape_bicycles = PythonOperator(task_id='scrape_bicycles',python_callable=scrape_bicycles , provide_context=True)
    # clean the staging ara : 
    opr_clean_staging_area = BashOperator(
        task_id='clean_staging_area_Images' , 
        bash_command='rm -r /usr/local/airflow/store_files_airflow/Image_data/Images/* '
    )
    # drop duplicates from data : 
    opr_drop_duplicates = PythonOperator(task_id='drop_duplicates',python_callable=drop_duplicates , provide_context=True)
    # download bicycle row data :
    opr_download_bicycle_row_data = PythonOperator(task_id='download_bicycle_row_data',python_callable=download_bicycle_data , provide_context=True)
    # download bicycle pictures  : 
    opr_download_bicycle_image_data = PythonOperator(task_id='download_bicycle_pictures',python_callable=download_bicycle_pictures , provide_context=True) 
    # validate row data files : 
    opr_validate_row_data_files = PythonOperator(task_id='validate_row_data_files',python_callable=validate_row_data_files,provide_context=True)
    # validate image data files : 
    opr_validate_image_Metadata = PythonOperator(task_id='validate_image_Metadata',python_callable=validate_image_Metadata,provide_context=True)
    # convert prices to float : 
    opr_convert_prices=PythonOperator(task_id='convert_prices_to_float',python_callable=convert_price_to_float,provide_context=True)
    # create daily reports : 
    opr_create_daily_reports=PythonOperator(task_id='create_daily_reports',python_callable=create_daily_reports,provide_context=True)
    # validate reports  : 
    opr_validate_daily_reports=PythonOperator(task_id='validate_reports_Files',python_callable=validate_reports_Files,provide_context=True)
    # validate schema of row files   : 
    opr_validate_row_files_schema=PythonOperator(task_id='validate_schema_row_files',python_callable=valdidate_row_files_schema,provide_context=True)
    #TimeSeries data : 
    opr_store_timeseries_data=PythonOperator(task_id='store_timeseries_data',python_callable=store_time_series_data,provide_context=True)
    #concatenate data of different categories of bicycles :
    # start by cleaning the old files :  
    opr_concatenate_data = BashOperator(
        task_id='concatenate_data' , 
        bash_command='rm -r /usr/local/airflow/store_files_airflow/Concat_data/* '
    )
    opr_send_email = EmailOperator(task_id='send_email',
        to='chiheb.airflow@gmail.com',
        subject='Task finished : Data Scraping',
        html_content=""" <h1>The daily scraping job is done with success .</h1>  <br> <h2> Check your storage layer to display the data . <h2/>""")

    #concat data all dates  : 
    opr_concat_data_all_dates=PythonOperator(task_id='concat_data_all_dates',python_callable=concat_data_all_dates,provide_context=True)
    #consistent model names : 
    opr_consistent_model_names=PythonOperator(task_id='extract_consistent_model_names',python_callable=get_consistent_model_names,provide_context=True)
    #concatenate data for 'E-Bikes City' : 
    opr_concatenate_E_bike_City=PythonOperator(task_id='concatenate_E_bike_City',python_callable=concat_data,op_kwargs={'category':'E-Bikes City'},provide_context=True)
    opr_concatenate_E_Bikes_Cross=PythonOperator(task_id='concatenate_E-Bikes_Cross',python_callable=concat_data,op_kwargs={'category':'E-Bikes Cross'},provide_context=True)
    opr_concatenate_E_Bikes_Kinder=PythonOperator(task_id='concatenate_E-Bikes_Kinder',python_callable=concat_data,op_kwargs={'category':'E-Bikes Kinder'},provide_context=True)
    opr_concatenate_E_Bikes_Rennrad_Gravel=PythonOperator(task_id='concatenate_E-Bikes_Rennrad_Gravel',python_callable=concat_data,op_kwargs={'category':'E-Bikes Rennrad & Gravel'},provide_context=True)
    opr_concatenate_E_Bikes_Trekking=PythonOperator(task_id='concatenate_E-Bikes_Trekking',python_callable=concat_data,op_kwargs={'category':'E-Bikes Trekking'},provide_context=True)
    opr_concatenate_E_Bikes_Urban=PythonOperator(task_id='concatenate_E-Bikes_Urban',python_callable=concat_data,op_kwargs={'category':'E-Bikes Urban'},provide_context=True)
    opr_concatenate_E_Mountainbikes=PythonOperator(task_id='concatenate_E_Mountainbikes',python_callable=concat_data,op_kwargs={'category':'E-Mountainbikes'},provide_context=True)
    opr_concatenate_S_Pedelecs_45kmh=PythonOperator(task_id='concatenate_S-Pedelecs_45kmh',python_callable=concat_data,op_kwargs={'category':'S-Pedelecs 45kmh'},provide_context=True)
    opr_concatenate_SUV_E_Bikes=PythonOperator(task_id='concatenate_SUV_E-Bikes',python_callable=concat_data,op_kwargs={'category':'SUV E-Bikes'},provide_context=True) 
    #clean the data : 
    opr_drop_columns = PythonOperator(task_id='clean_data',python_callable=drop_columns , provide_context=True)
    # Clean staging area cleansed_data : 
    opr_clean_Cleansed_data = BashOperator(
        task_id='clean_staging_area_Cleansed_data' , 
        bash_command='rm -r /usr/local/airflow/store_files_airflow/Cleansed_data/* '
    )

    # Clean staging area TimeSeries Data : 
    opr_clean_TimeSeries_data = BashOperator(
        task_id='clean_staging_area_TimeSeries_data' , 
        bash_command='rm -r /usr/local/airflow/store_files_airflow/TimeSeries_Data/* '
    )




    opr_parse_bicycles >> opr_clean_staging_area >> opr_scrape_bicycles >> [opr_download_bicycle_row_data , opr_download_bicycle_image_data ]
    opr_download_bicycle_row_data >> opr_drop_duplicates 
    opr_drop_duplicates >> opr_convert_prices
    opr_convert_prices >> [opr_validate_row_data_files,opr_validate_row_files_schema]
    opr_download_bicycle_image_data >> opr_validate_image_Metadata
    [opr_validate_row_data_files,opr_validate_row_files_schema] >> opr_create_daily_reports
    opr_create_daily_reports >> opr_send_email 
    opr_send_email >> opr_validate_daily_reports
    opr_validate_daily_reports >> opr_concat_data_all_dates 
    opr_concat_data_all_dates >> opr_consistent_model_names 
    [opr_consistent_model_names,opr_validate_image_Metadata] >> opr_concatenate_data
    opr_concatenate_data >> [opr_concatenate_E_bike_City,opr_concatenate_E_Bikes_Cross,opr_concatenate_E_Bikes_Kinder,opr_concatenate_E_Bikes_Rennrad_Gravel,opr_concatenate_E_Bikes_Trekking,opr_concatenate_E_Bikes_Urban,opr_concatenate_E_Mountainbikes,opr_concatenate_S_Pedelecs_45kmh,opr_concatenate_SUV_E_Bikes]
    [opr_concatenate_E_bike_City,opr_concatenate_E_Bikes_Cross,opr_concatenate_E_Bikes_Kinder,opr_concatenate_E_Bikes_Rennrad_Gravel,opr_concatenate_E_Bikes_Trekking,opr_concatenate_E_Bikes_Urban,opr_concatenate_E_Mountainbikes,opr_concatenate_S_Pedelecs_45kmh,opr_concatenate_SUV_E_Bikes] >> opr_clean_Cleansed_data
    opr_clean_Cleansed_data >> opr_drop_columns
    opr_drop_columns >> opr_clean_TimeSeries_data
    opr_clean_TimeSeries_data >> opr_store_timeseries_data 
    
