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

# загружаем гугл шит
credentials = ast.literal_eval(google_credentials)
gc = gspread.service_account_from_dict(credentials)
sh = gc.open('digis_in_stock')


# функция для отправки сообщения в телеграм
def send_message_tel(message):
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    params = {
        'chat_id': TELEGRAM_CHANNEL_ID,
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

        send_message_tel(f'В категории {prod_cat} закончились следующие товары: \n{result_string}')


    if len(arrive_list) > 0:

        string_list = [str(element) for element in arrive_list]
        delimiter = ";\n"
        result_string = delimiter.join(string_list)

        send_message_tel(f'В категории {prod_cat} появились следующие товары: \n{result_string}')
        
    if len(gone_list) == 0 and len(arrive_list) == 0:
        send_message_tel(f'В категории {prod_cat} без изменений')
    #очищаем лист
    wks.clear()
    #загружаем новый натафрейм на страницу
    wks.update([df.columns.values.tolist()] + df.values.tolist())


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

data = []

def get_hifi_elements(elements):
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

        data.append({
        'title': element_factory + ' ' + element_model,
        'description': element_description,
        'status': element_status,
        'price': element_price
        })

def get_hifi(cat):
    data = []
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
        get_hifi_elements(elements)
        
    df = pd.DataFrame(data)
    df = df[df['status'] == 'В наличии']
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M')

    wks = sh.worksheet(cat)
    #сохраняем старую страницу в датафрейм
    old_df = pd.DataFrame(wks.get_all_records())
    #собираем список уникальных названий товаров
    old_title_list = old_df.title.unique()
    #собираем список уникальных новых товаров
    new_title_list = df.title.unique()
    wks.clear()
    wks.update([df.columns.values.tolist()] + df.values.tolist())
       
    s = set(new_title_list)
    gone_list = [x for x in old_title_list if x not in s]

    #ищем названия, которые есть в новом списке, но нет в старом
    p = set(old_title_list)
    arrive_list = [x for x in new_title_list if x not in p]

    if len(gone_list) > 0:

        string_list = [str(element) for element in gone_list]
        delimiter = "; "
        result_string = delimiter.join(string_list)

        send_message_tel(f'В категории {cat} закончились следующие товары')
        send_message_tel(result_string)


    if len(arrive_list) > 0:

        string_list = [str(element) for element in arrive_list]
        delimiter = "; "
        result_string = delimiter.join(string_list)

        send_message_tel(f'В категории {cat} появились следующие товары')
        send_message_tel(result_string)
        
    if len(gone_list) == 0 and len(arrive_list) == 0:
        send_message_tel(f'В категории {cat} без изменений')
    #очищаем лист
    wks.clear()
    #загружаем новый натафрейм на страницу
    wks.update([df.columns.values.tolist()] + df.values.tolist())
    

#запуск кода

if __name__ == '__main__':
    send_message_tel('||| DIGIS начало нового анализа |||')

    for proj_cat in main_cat_list:
        cat_pars(proj_cat)

    send_message_tel('||| DIGIS анализ закончен |||')

    sh = gc.open('hi-tech_in_stock')

    send_message_tel('||| Hi-tech начало нового анализа |||')

    for cat in hitech_main_cat:
        get_hifi(cat)
    
    send_message_tel('||| Hi-tech анализ закончен |||')

