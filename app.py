import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
import re
from datetime import datetime
from dotenv import load_dotenv
import ast

#загружаем env
load_dotenv()
google_credentials = os.getenv('GOOGLE_CREDENTIALS')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID')
YOUTUBE_KEY = os.getenv('YOUTUBE_KEY')

# загружаем гугл шит
credentials = ast.literal_eval(google_credentials)
gc = gspread.service_account_from_dict(credentials)
sh = gc.open('digis_in_stock')


# функция для отправки сообщения в телеграм
def send_message_tel(message):
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    params = {
        'chat_id': TELEGRAM_CHANNEL_ID,
        'parse_mode': 'Markdown',
        'text': message
    }
    res = requests.post(url, params=params)

# функция для парсинга, загрузки в шит и отправки обновлений в телеграм
def cat_pars(prod_cat):

    data = []
    #каждая страница начинается с 1 
    page = 1

    #задаем по умолчанию максимальное количество страниц 1
    max_page = 1

    #пока номер страницы не больше максимального количества страниц
    while page <= max_page:
        #получаем запрос по ссылке с товарами определенной категории в наличии - "FILTER_QUANTITY=Y"
        res = requests.get(f'https://digis.ru/distribution/{prod_cat}/f/clear/a/?FILTER_QUANTITY=Y&PAGEN_1={page}')
        #обрабатываем супом
        soup = BeautifulSoup(res.text, features="html.parser")
        #находим все карточки товаров
        elements = soup.find_all('tr')

        #для каждой карточки товара
        for e in elements:
            #ищем название товара или прочерк (в названии встречаются \t)
            try:
                element_title = (e.find('div', class_='head line-items line-items-middle')
                                 .text.strip()
                                 .replace('\t\t\t\t\t\t\t\t\t\t\t\t\t\n\n', ' '))
            except:
                element_title = '-'

            #ищем цену или прочерк
            try:
                element_description = e.find('div', class_='desc').text.strip()
            except:
                element_description = '-'

            #ищем описание или прочерк (в цене бывает старая зачеркнутая цен, она идет перед \n, ее удаляем регуляркой)
            try:
                element_price = re.sub(r'^.*\n', '', e.find('div', class_='price-item price-item-main').text.strip())
            except:
                element_price = '-'
            
            # собиваем элементы в словарь
            data.append({
                'title': element_title,
                'description': element_description,
                'price': element_price
            })
        

        #ищес на странице номера страниц
        pagenations = soup.find('div', class_='pager-pages-list line-items')

        #если номера есть, то берем самое большое значение
        try:
            pages = [p.text.strip().replace('...', '0') for p in pagenations.find_all('a', class_='pager-page')]
            int_pages = []
            for p in pages:
                try:
                    n = int(p)
                    int_pages.append(n)
                except:
                    continue
            max_page = max(int_pages)

        #если номеров страниц нет, то оставляем максимальное значение 1
        except:
            max_page = 1

        #добавляем 1 к странице
        page += 1

    #собиваем название. описание и цену в датафрейм
    df = pd.DataFrame(data)

    # иногда вместо цен бывает пустота, меняем ее на прочерк
    df['price'] = df['price'].replace('', '-')
    #добавляем столбец с текущим временем
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    #загружаем страницу из гугл шитс с названием категории
    wks = sh.worksheet(prod_cat)

    #сохраняем старую страницу в датафрейм
    old_df = pd.DataFrame(wks.get_all_records())

    #собираем список уникальных названий товаров
    old_title_list = old_df.title.unique()
    #собираем список уникальных новых товаров
    new_title_list = df.title.unique()

    #ищем назнания, которые были в старом списке, но уже нет в новом
    s = set(new_title_list)
    gone_list = [x for x in old_title_list if x not in s]

    #ищем названия, которые есть в новом списке, но нет в старом
    p = set(old_title_list)
    arrive_list = [x for x in new_title_list if x not in p]

    if len(gone_list) > 0:

        string_list = [str(element) for element in gone_list]
        delimiter = ";\n"
        result_string = delimiter.join(string_list)

        digis_cat_text = (f'\n \nВ категории *{prod_cat}* закончились следующие товары: \n{result_string}')


    if len(arrive_list) > 0:

        string_list = [str(element) for element in arrive_list]
        delimiter = ";\n"
        result_string = delimiter.join(string_list)

        digis_cat_text = (f'\n \nВ категории *{prod_cat}* появились следующие товары: \n{result_string}')
        
    if len(gone_list) == 0 and len(arrive_list) == 0:
        digis_cat_text = (f'\n \nВ категории *{prod_cat}* без изменений')
    #очищаем лист
    wks.clear()
    #загружаем новый натафрейм на страницу
    wks.update([df.columns.values.tolist()] + df.values.tolist())
    return digis_cat_text


#список катогорий DIGIS
main_cat_list = [
    'multimediynye-proektory',
    'svetodiodnye-ekrany-svetodiodnye-ekrany',
    'displei-displei',
    'kongress-sistemy',
    'sistemy-upravleniya-sistemy-upravleniya',
    'kamery-ptz-kamery'
]

#список катогорий Hi-tech
hitech_main_cat = [
    'proektory',
    'svetodiodnye-ekrany',
    'sistemy-otobrazheniya-informatsii',
    'kamery',
    'konferents-sistemy',
    'videokonferentssvyaz',
    'akusticheskoe-oborudovanie',
    'av-kommutatsiya',
    'oborudovanie-upravleniya',
    'interaktivnye-ustroystva'
]



def get_hifi(cat):
    data_hitek = []
    domen = 'https://hi-tech-media.ru'
    url = domen + '/equipment/' + cat + '/'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    # Найти ul с классом root-item, а затем внутри него найти нужную ссылку
    root_item_ul = soup.find('ul', class_='root-item')
    links = root_item_ul.find_all('a')
    hrefs = [link['href'] for link in links]

    
    for href in hrefs:
        url_equipment = domen + href + '?SHOWALL_1=1'
        res_equipment = requests.get(url_equipment)
        soup_equipment = BeautifulSoup(res_equipment.text, 'html.parser')
        elements = soup_equipment.find_all('div', class_='item_body')
        for e in elements:
        #модель
            element_model= e.find('h2').text.strip()
            

            #производитель и описание
            p_tags = e.find_all('p')
            #производитель
            element_factory= p_tags[0].get_text(strip=True).replace('Производитель: ', '')

            #описание
            try:
                element_description = p_tags[1].get_text(strip=True)
            except:
                element_description = ' - '

            #наличие
            element_status= e.find('span').text.strip()
            

            #цена
            try:
                element_price= e.find('strong', class_='ss').text.strip()
                
            except:
                element_price= e.find('strong').text.strip()

            data_hitek.append({
            'title': element_factory + ' ' + element_model,
            'description': element_description,
            'status': element_status,
            'price': element_price
            })
        
    df_hitek = pd.DataFrame(data_hitek)
    df_hitek = df_hitek.query('status == "В наличии"').copy()

    df_hitek.loc[:, 'timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M')

    wks = sh.worksheet(cat)
    #сохраняем старую страницу в датафрейм
    old_df = pd.DataFrame(wks.get_all_records())
    #собираем список уникальных названий товаров
    old_title_list = old_df.title.unique()
    #собираем список уникальных новых товаров
    new_title_list = df_hitek.title.unique()
    wks.clear()
    wks.update([df_hitek.columns.values.tolist()] + df_hitek.values.tolist())
       
    s = set(new_title_list)
    gone_list = [x for x in old_title_list if x not in s]

    #ищем названия, которые есть в новом списке, но нет в старом
    p = set(old_title_list)
    arrive_list = [x for x in new_title_list if x not in p]

    if len(gone_list) > 0:

        string_list = [str(element) for element in gone_list]
        delimiter = ";\n"
        result_string = delimiter.join(string_list)

        hitech_cat_text = (f'\n\nВ категории *{cat}* закончились следующие товары: \n{result_string}')


    if len(arrive_list) > 0:

        string_list = [str(element) for element in arrive_list]
        delimiter = ";\n"
        result_string = delimiter.join(string_list)

        hitech_cat_text = (f'\n\nВ категории *{cat}* появились следующие товары: \n{result_string}')
        
        
    if len(gone_list) == 0 and len(arrive_list) == 0:
        hitech_cat_text = (f'\n\nВ категории *{cat}* без изменений')
    #очищаем лист
    wks.clear()
    #загружаем новый натафрейм на страницу
    wks.update([df_hitek.columns.values.tolist()] + df_hitek.values.tolist())
    return hitech_cat_text

# создание списка вакансий и отправлка его в гугл шит
def chech_jobs(elements):
    vac_list = []
    for e in elements:
        vac_list.append(e.text.strip())
    vac_df = pd.DataFrame(vac_list)
    vac_df.columns = ['job_title']
    vac_df

    #загружаем страницу из гугл шитс с названием вакансий
    wks = sh.worksheet('jobs')
    old_jobs = pd.DataFrame(wks.get_all_records())


    #собираем список уникальных названий вакансий
    old_jobs_list = old_jobs.job_title.unique()
    #собираем список уникальных новых вакансий
    new_jobs_list = vac_df.job_title.unique()

    s = set(new_jobs_list)
    gone_list = [x for x in old_jobs_list if x not in s]

    #ищем названия, которые есть в новом списке, но нет в старом
    p = set(old_jobs_list)
    arrive_list = [x for x in new_jobs_list if x not in p]

    if len(gone_list) > 0:

        string_list = [str(element) for element in gone_list]
        delimiter = ";\n"
        result_string = delimiter.join(string_list)

        cat_text = (f'\n \nНовости *вакансий*. Перестали искать: \n{result_string}')


    if len(arrive_list) > 0:

        string_list = [str(element) for element in arrive_list]
        delimiter = ";\n"
        result_string = delimiter.join(string_list)

        cat_text = (f'\n \nНовости *вакансий*. Начали искать: \n{result_string}')
        
    if len(gone_list) == 0 and len(arrive_list) == 0:
        cat_text = (f'\n \nВ *вакансиях* без изменений.')
    #очищаем лист
    wks.clear()
    #загружаем новый натафрейм на страницу
    wks.update([vac_df.columns.values.tolist()] + vac_df.values.tolist())

    return cat_text


#поиск новых вакансий hitech
def get_hitech_jobs():
    res = requests.get('https://hi-tech-media.ru/about/vacancies/')
    #обрабатываем супом
    soup = BeautifulSoup(res.text, features="html.parser")
    #находим таблицу с вакансиями
    elements = soup.find('div', class_='news-list')
    elements = elements.find_all('h2')
    return chech_jobs(elements)

def get_digis_jobs():
    res = requests.get('https://digis.ru/about/vacancies/')
    #обрабатываем супом
    soup = BeautifulSoup(res.text, features="html.parser")
    #находим все карточки товаров
    elements = soup.find_all('div', class_='vacancy__header-bottom')

    return chech_jobs(elements)

# получение количество подписчиков в телеграм:
def get_telegram_subscribers(channel_username):
    url = f'https://t.me/{channel_username}'
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        subscribers = soup.find('div', {'class': 'tgme_page_extra'}).text.replace(' subscribers', '')
        text_message = f'\n\nКоличество подписчиков в *Телеграм*: {subscribers}'
        return text_message
    else:
        text_message = "Не удалось получить данные о *телеграмканале*"
        return text_message 
    
# получение количество подписчиков в ютьюб:
def get_youtube_info(channel_id):
    url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={YOUTUBE_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        views = data['items'][0]['statistics']['viewCount']
        subscribers = data['items'][0]['statistics']['subscriberCount']
        text_message = f'\n\nНа *ютьюб канале* подписчиков - {subscribers}, количество просмотров {views}.'
        return text_message
    else:
        text_message = '\n\nНе получилось получить информацию о *ютьюб канале*'
        return text_message

#запуск кода

if __name__ == '__main__':
    digis_final_text = '*Digis*' 

    for proj_cat in main_cat_list:
        digis_final_text += cat_pars(proj_cat)

    digis_final_text += get_digis_jobs()
    digis_final_text += get_telegram_subscribers('digisgroup')
    digis_final_text += get_youtube_info(channel_id='UCnisrWW0YJBVV4w9Mo5cfdg')

    send_message_tel(digis_final_text)

    sh = gc.open('hi-tech_in_stock')

    hitech_final_text = '*Hi-tech-media*' 

    for cat in hitech_main_cat:
        hitech_final_text += get_hifi(cat)
    
    hitech_final_text += get_hitech_jobs()
    hitech_final_text += get_telegram_subscribers('htmedia')
    hitech_final_text += get_youtube_info(channel_id='UChHSr-49b14rYGPbXlyImkw')
    send_message_tel(hitech_final_text)

