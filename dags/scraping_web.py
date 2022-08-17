import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
import uuid
import requests
from bs4 import BeautifulSoup
from time import sleep



def gen_random_text():
    return uuid.uuid4().hex
    
def parse_recipes(**kwargs):
    title = '-'
    submit_by = '-'
    description = '-'
    image_url = '-'
    calories = 0
    ingredients = []
    rec = {}
    records = []

    path = '/usr/local/airflow/store_files_airflow/'

    with open(path + 'latest_salad_recipes.txt', encoding='utf8') as f:
        entries = f.readlines()

    for entry in entries:
        r = requests.get(entry.rstrip('\n'))
        if r.status_code == 200:
            html = r.text.strip()
            soup = BeautifulSoup(html, 'lxml')
            # title
            title_section = soup.select('.recipe-summary__h1')
            # submitter
            submitter_section = soup.select('.submitter__name')
            # description
            description_section = soup.select('.submitter__description')
            # ingredients
            ingredients_section = soup.select('.recipe-ingred_txt')

            # image
            image_section = soup.select('.hero-photo__wrap #BI_openPhotoModal1')

            # calories
            calories_section = soup.select('.calorie-count')
            if calories_section:
                calories = calories_section[0].text.replace('cals', '').strip()

            if ingredients_section:
                for ingredient in ingredients_section:
                    ingredient_text = ingredient.text.strip()
                    if 'Add all ingredients to list' not in ingredient_text and ingredient_text != '':
                        ingredients.append({'step': ingredient.text.strip()})

            if description_section:
                description = description_section[0].text.strip().replace('"', '')

            if submitter_section:
                submit_by = submitter_section[0].text.strip()

            if title_section:
                title = title_section[0].text

            if image_section:
                image_url = image_section[0]['src']
                print(image_url)

            rec = {'url': entry.rstrip('\n'), 'title': title, 'submitter': submit_by, 'description': description,
                   'calories': calories,
                   'ingredients': ingredients, 'image_url': image_url}
            records.append(rec)
            

    return records

def dl_img(image_url, r_url):
    file_name = ''
    print('Downloading Image..' + image_url)
    sleep(3)

    try:
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
            'referer': r_url,
        }
        response = requests.get(image_url, headers=headers)
        # response = requests.get(image_url)

        print('Status Code = {}'.format(response.status_code))

        if response.status_code == 200:
            file_name = gen_random_text() + '.png'
            path = '/Development/airflow_scraping/dags/'
            file_path = path + file_name
            print('LOCAL FILE PATH = {}'.format(file_path))

            with open(file_path, 'wb') as fi:
                fi.write(response.content)

    except Exception as ex:
        print('Exception while downloading')
        print(str(ex))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        file_name = None
    finally:
        return file_name



def download_image(**kwargs):
    local_image_file = None
    idx = 0
    records = []
    ti = kwargs['ti']

    parsed_records = ti.xcom_pull(key=None, task_ids='parse_recipes')

    for rec in parsed_records:
        idx += 1
        image_url = rec['image_url']
        r_url = rec['url']
        print('Downloading Pic# {}'.format(idx))
        local_image_file = dl_img(image_url, r_url)
        rec['local_image'] = local_image_file
        records.append(rec)

    return records


def store_data(**kwargs):
    ti = kwargs['ti']

    parsed_records = ti.xcom_pull(key=None, task_ids='download_image')
    print('PRINTING DUMPED RECORDS in STORE DATA')
    print(parsed_records)
    return 'store_data'


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 4, 4),
    'concurrency': 1,
    'retries': 0, 
    'provide_context': True
}

with DAG('parsing_recipes',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         template_searchpath=['/usr/local/airflow/sql_files']
         ) as dag:
    opr_parse_recipes = PythonOperator(task_id='parse_recipes',
                                       python_callable=parse_recipes,provide_context=True)

    opr_download_image = PythonOperator(task_id='download_image',
                                        python_callable=download_image , provide_context=True)
    opr_store_data = PythonOperator(task_id='store_data',
                                    python_callable=store_data , provide_context=True)
    opr_create_table=MySqlOperator(task_id='create_mysql_table',mysql_conn_id="mysql_conn",sql="create_recipes.sql")
                                                                                                
    opr_email = EmailOperator(
        task_id='send_email',
        to='chiheb.airflow@gmail.com',
        subject='Airflow Finished',
        html_content=""" <h3> Congratulations , Web Scraping done </h3> """
    )

opr_parse_recipes >> opr_download_image >> opr_create_table >> opr_store_data >> opr_email
