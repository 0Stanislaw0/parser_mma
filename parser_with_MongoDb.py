import base64
import csv  #
import os
import time
from random import choice
from urllib.request import *

import requests  # имитатор HTTP запросов
from bs4 import BeautifulSoup as bs  # позволят распарсить ответ, который получили от сервера
from pymongo import MongoClient

client = MongoClient()
db = client.news_parser
collection = db.posts

useragents = open('useragents.txt').read().split('\n')

headers = { 'user-agent' : choice(useragents),
            'accept' : '*/*',
} #

def get_html (url, useragent=None, proxies = None):
    """ get the html of pages"""
    r = requests.get(url, headers = useragent, proxies = proxies)
    return r.text

def removeAfter(string, suffix): # функция удаления после знака
    return string[:string.index(suffix) + len(suffix)]

mystrip = lambda string, suffix: string[:string.index(suffix) + len(suffix)] # лямбда функция удаления после знака

def newsmma(headers):
    new_url= 'https://newsmma.net/news/?page1' # сайт, которым парсим
    new = 'https://newsmma.net'
    news = []  # создаем список, куда будет записываться нужная нам информация
    urls = []  # записываем сюда url статей
    urls.append(new_url)

    session = requests.Session() # создаем сессию
    request = session.get(new_url, headers = headers) # имулируюем   открытие страницы в браузере

    if request.status_code == 200: # проверка. Получили ли данные, которые необходимо спарсить
        request = session.get(new_url, headers = headers)
        soup = bs(get_html(new_url,headers), 'lxml') # результат ответа и встроенная функция которая, позволяет распарсить ответ
        try:
            pagination =  soup.find_all('a',attrs={'class':'swchItem'}) # ищем элементы,которые отвечают за навигацию
            count = int(pagination[1].text) # хватаем предпоследний элемент, т.к. последний элемент не число
            print(count)
        except:
            print('error')
        for i in range(1,count): # пробегаем по всем страницам
            url = f"https://newsmma.net/news/?page{i}"
            if url not in urls: # и добавляем в список urls наши ссылки с необходимой нам информацией
                urls.append(url)
    for url in urls:  # краулер
        time.sleep(10)
        request = session.get(url, headers = headers)
        soup = bs(request.content, 'lxml')
        divs = soup.find_all('div', attrs={'class':'h_mtr_content'})  # ищем все дивы

        for index, div in enumerate(divs):
            text = div.find('h2', attrs={'class': 'h_mtr_title'}).text  # выбираем нам нужные и записываем в список
            #text2 = div.find('div', attrs={'class': 'h_mtr_text post-d'}).get_text().split('читать дальше »')
            href =  div.find('a', class_='entryReadAllLink').get('href')  # получаем ссылки на статью
            suburl_soup = bs(get_html("".join([new,href]),headers),'lxml')  # открываем статью, new and url  реализовано так, потому ссылка которая содержится в href представлена неполная
            area = suburl_soup.find(id='main-content')
            img_src = area.find('img').get('src')
            img_src = ("".join([new,img_src]))
            try:
                urlretrieve(img_src,img_src[30:])
                print(index,img_src[30:] + ' downloaded')
                path = os.path.abspath("{}").format(img_src[30:])
                with open(path, "rb") as image_file:
                    encoded_img = base64.b64encode(image_file.read())
            except Exception:
                print(index,img_src[30:] + ' No downloaded')
            else:
                encoded_img = ''
            table = suburl_soup.find('table')
            description = table.find('td', class_='eMessage').get_text().split("$")[0] # обрезаем. т.к. дальше копируется аякс запрос
            description = description.replace('\n', '') # удаляем лишние пробелы
            news.append({
            'text':text,
            'path':path,
            'encoded_img':encoded_img,
            'href':href,
            'description':description,
            })
    else:
        print("ERROR or Done. Status_code = " + str(request.status_code))

    post_id = collection.insert_many(news)
    return news


def file_writer(news):
    with open('parsed_news.csv', 'w') as file:
        a_pan = csv.writer(file)
        a_pan.writerow(( 'Заголовок новости', 'Краткое содержимое новости', 'url'))
        for new in news:
            a_pan.writerow((new['text'], new['description'], new['href']))

news = newsmma(headers)
file_writer(news)
