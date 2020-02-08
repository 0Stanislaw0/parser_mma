import base64, csv, os, time, requests, psycopg2
from random import choice
from urllib.request import *
from bs4 import BeautifulSoup as bs  # позволят распарсить ответ, который получили от сервера
import psycopg2


class Parser_MMA():

    def get_connection(self,new_url,headers):
        """ get connection to site"""
        session = requests.Session() # создаем сессию
        request = session.get(new_url, headers = headers)
        return request

    def get_html (self,url, useragent=None, proxies = None):
        """ get the html of pages"""
        r = requests.get(url, headers = useragent, proxies = proxies)
        return r.text

    def pagination(self,number1,number2):
        urls = []
        for i in range(number1,number2): # пробегаем по всем страницам
            url = f"https://newsmma.net/news/?page{i}"
            if url not in urls: # и добавляем в список urls наши ссылки с необходимой нам информацией
                urls.append(url)
        return urls

    def krauler(self,urls):
        new = 'https://newsmma.net'
        news =[]
        for url in urls:  # краулер
            time.sleep(10)
            request = self.get_connection(url,headers)
            soup = bs(request.content, 'lxml')
            divs = soup.find_all('div', attrs={'class':'h_mtr_content'})  # ищем все дивы

            for index, div in enumerate(divs):
                text = div.find('h2', attrs={'class': 'h_mtr_title'}).text  # выбираем нам нужные и записываем в список
                #text2 = div.find('div', attrs={'class': 'h_mtr_text post-d'}).get_text().split('читать дальше »')
                href =  div.find('a', class_='entryReadAllLink').get('href')  # получаем ссылки на статью
                suburl_soup = bs(self.get_html("".join([new,href]),headers),'lxml')  # открываем статью, new and url  реализовано так, потому ссылка которая содержится в href представлена неполная
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

                table = suburl_soup.find('table')
                description = table.find('td', class_='eMessage').get_text().split("$")[0] # обрезаем. т.к. дальше копируется аякс запрос
                description = description.replace('\n', '') # удаляем лишние пробелы
                news.append({
                'text':text,
            #    'path':path,
                'encoded_img':encoded_img,
                'href':href,
                'description':description,
                })
            else:
                print("ERROR or Done. Status_code = " + str(request.status_code))
        return news

    def file_writer_csv(self,news):
        with open('parsed_news.csv', 'w',encoding='utf-8') as file:
            a_pan = csv.writer(file)
            a_pan.writerow(( 'Заголовок новости', 'Картинка в бинарном виде', 'url', 'Описание статьи'))
            for new in news:
                a_pan.writerow((new['text'], new['encoded_img'], new['href'], new['description']))

    def write_to_PostGreSQl(self,news):
        connect = psycopg2.connect("host=localhost dbname=postgres user=postgres password=123") # Подключаемся к postgresql.
        cursor = connect.cursor() # создаем курсор в этой бд
        for  new in news:
            cursor.execute(
                        "INSERT INTO posts(headname,encoded_img,url,description) VALUES ( %s, %s, %s, %s)",(new['text'], new['encoded_img'], new['href'], new['description']))
        connect.commit()

    def create_db(self,name_db):
        connect = psycopg2.connect("host=localhost dbname=postgres user=postgres password=123") # Подключаемся к postgresql.
        cursor = connect.cursor() # создаем курсор в этой бд
        cursor.execute("""
                    CREATE TABLE %s(
                    id SERIAL PRIMARY KEY,
                    headname text,
                    encoded_img bytea,
                    url text,
                    description text)
                    """,(name_db))
        connect.commit()

if __name__ == '__main__':
    useragents = open('useragents.txt').read().split('\n')
    headers = { 'user-agent' : choice(useragents),
                    'accept' : '*/*',
                    }
    new_url = 'https://newsmma.net/news/?page1'
    mma = Parser_MMA()
    mma.create_db("testim")
    connection = mma.get_connection(new_url, headers)
    pagination = mma.pagination(1,10)
    krauler = mma.krauler(pagination)
