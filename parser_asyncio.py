import asyncio
from aiohttp import ClientSession

from time import sleep, strftime, gmtime
from lxml import html

from bs4 import BeautifulSoup

import pandas as pd
from tqdm import tqdm

import requests


new_proc_count = 0
res = []


def get_content(page_content, url):
    # Получаем корневой lxml элемент из html страницы.
    document = html.fromstring(page_content)

    def get(xpath):
        item = document.xpath(xpath)
        if item:
            return item[-1]
        return None

    name = get('/html/body/article/div[1]/div[3]/h3[1]//text()')
    text_content = get('/html/body/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/span/span[1]//text()')
    time = strftime('%Y-%m-%d', gmtime())

    answer = {'url': url, 'time': time, 'name': name, 'text_content': text_content}
    return answer


async def get_one_task(url, session):
    global new_proc_count
    async with session.get(url) as response:
        # Ожидаем ответа, получаем контент
        page_content = await response.read()

        # Парсим содержимое контента
        item = get_content(page_content, url)

        res.append(item)
        new_proc_count += 1
        print('Стр.: ' + str(new_proc_count) + ' Запарсено: ' + url)


async def bound_screen(smp, url, session):
    try:
        async with smp:
            await get_one_task(url, session)
    except Exception:
        print('Exception:\n', Exception)
        # Блокируем запросы на 10с если ошибка 429 (много запросов)
        sleep(10)


async def run(urls):
    tasks = []
    # Ограничение для запуска не более 20 асинхронных процессов
    smp = asyncio.Semaphore(20)
    headers = {'User-Agent': 'Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101'}

    # Запускаем 20 процессов
    async with ClientSession(headers=headers) as session:
        # Передаем по урлу в один процесс
        for url in urls:
            task = asyncio.ensure_future(bound_screen(smp, url, session))
            tasks.append(task)
        await asyncio.gather(*tasks)


def get_urls_news(prev_url):
    urls = []

    while True:
        try:
            html_page = requests.get(prev_url).content
        except Exception:
            print('Exception:\n', Exception)
            sleep(20)
            html_page = requests.get(prev_url).content
        soup = BeautifulSoup(html_page, 'html.parser')
        body_tag = soup.find('body')
        main_tag = body_tag.find('main', {'id': 'main'})
        div1_tag = main_tag.find('div', {'class': 'entries'})
        div2_tag = div1_tag.find('div', {'class': 'post-list'})
        div3_all_tag = div2_tag.find_all('div')
        hrefs = []
        for i in range(len(div3_all_tag)):
            get_href = div3_all_tag[i].get('id')
            if get_href != None:
                get_id = get_href.split('-')[1]
                hrefs.append(get_id)
        if not div3_all_tag[-1].find('a', {'class': 'next page-numbers'}) != None:
            break
        next_url = div3_all_tag[-1].find('a').get('href')
        for i in range(len(hrefs)):
            urls.append('https://pasmi.ru/archive/' + str(hrefs[i]) + '/')
            print('Стр.: ' + str(i) + ' Запарсено: ' + hrefs[i])
    return urls

def get_urls_investigations(prev_url):
    urls = []

    while True:
        try:
            html_page = requests.get(prev_url).content
        except Exception:
            print('Exception:\n', Exception)
            sleep(20)
            html_page = requests.get(prev_url).content
        soup = BeautifulSoup(html_page, 'html.parser')
        body_tag = soup.find('body')
        main_tag = body_tag.find('main', {'id': 'main'})
        div1_tag = main_tag.find('div', {'class': 'entries'})
        div2_tag = div1_tag.find('div', {'class': 'post-list'})
        div3_all_tag = div2_tag.find_all('div')
        hrefs = []
        for i in range(len(div3_all_tag)):
            get_href = div3_all_tag[i].get('id')
            if get_href != None:
                get_id = get_href.split('-')[1]
                hrefs.append(get_id)
        if not div3_all_tag[-1].find('a', {'class': 'next page-numbers'}) != None:
            break
        next_url = div3_all_tag[-1].find('a').get('href')
        for i in range(len(hrefs)):
            urls.append('https://pasmi.ru/archive/' + str(hrefs[i]) + '/')
            print('Стр.: ' + str(i) + ' Запарсено: ' + hrefs[i])
    return urls

def get_urls_analytica(prev_url):
    urls = []

    while True:
        try:
            html_page = requests.get(prev_url).content
        except Exception:
            print('Exception:\n', Exception)
            sleep(20)
            html_page = requests.get(prev_url).content
        soup = BeautifulSoup(html_page, 'html.parser')
        body_tag = soup.find('body')
        main_tag = body_tag.find('main', {'id': 'main'})
        div1_tag = main_tag.find('div', {'class': 'entries'})
        div2_tag = div1_tag.find('div', {'class': 'post-list'})
        div3_all_tag = div2_tag.find_all('div')
        hrefs = []
        for i in range(len(div3_all_tag)):
            get_href = div3_all_tag[i].get('id')
            if get_href != None:
                get_id = get_href.split('-')[1]
                hrefs.append(get_id)
        if not div3_all_tag[-1].find('a', {'class': 'next page-numbers'}) != None:
            break
        next_url = div3_all_tag[-1].find('a').get('href')
        for i in range(len(hrefs)):
            urls.append('https://pasmi.ru/archive/' + str(hrefs[i]) + '/')
            print('Стр.: ' + str(i) + ' Запарсено: ' + hrefs[i])
    return urls


def main():
    global res

    while True:
        print('Выберете, что хотите получить:')
        gen_read = input('    Новости - n\n\
    Расследования - i\n\
    Подробности - m\n\
        \n\
    : ')

        if gen_read == 'n' or gen_read == 'N':
            prev_url = 'https://pasmi.ru/cat/news/'
            urls = get_urls_news(prev_url)
            break
        if gen_read == 'i' or gen_read == 'I':
            prev_url = 'https://pasmi.ru/cat/investigations/'
            urls = get_urls_investigations(prev_url)
            break
        if gen_read == 'm' or gen_read == 'M':
            prev_url = 'https://pasmi.ru/cat/analytica/'
            urls = get_urls_analytica(prev_url)
            break

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(urls))
    loop.run_until_complete(future)


    res = pd.DataFrame(res).dropna(axis=0).reset_index(drop=True)
    print(res.head())
    res.to_csv('new.csv', mode='a', sep=',')
    res.to_html('new.html')

if __name__ == '__main__':
    main()
