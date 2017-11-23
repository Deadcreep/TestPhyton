import requests
import re
from bs4 import BeautifulSoup, SoupStrainer
import random
import time
import pymongo
import datetime
import threading


def get_catalog_links():
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'}
    session = requests.Session()
    request = session.get('https://www.citilink.ru/?action=changeCity&space=chlb_cl:', headers=headers)
    soup = BeautifulSoup(request.content, "html.parser")
    menu_block = soup.find('div', {'class': 'main-navigation'})
    allLinks = menu_block.find_all('a', href=True)
    correct_links = []
    file_name = 'Links.txt'
    file = open(file_name, 'w')
    for mpLinks in allLinks:
        link = mpLinks.get('href')
        temp = re.match(r'https://www.citilink.ru/catalog/', str(link))
        if temp is not None:
            digit = re.search('[0-9]', str(link))
            if digit is None:
                correct_links.append(link)
                file.write(str(link) + '\n')
    file.write(str(len(allLinks)) + ' : all links\n')
    file.write(str(len(correct_links)) + ' : categories links\n')
    file.close()
    time.sleep(5)
    return correct_links


def get_content(link, useragent, page, proxy):
    print('GetContent')
    print(str(link))
    print('proxy : ' + str(proxy))
    headers = {'user-agent': str(useragent)}
    cookies = dict(_space='chlb_cl:')
    session = requests.Session()
    payload = {'available': '0', 'p': int(page)}
    request = session.post(str(link), headers=headers, params=payload, proxies=proxy)
    return request


def parseContent(request):
    import datetime
    import ast
    import json
    print('-----------\nParse start')
    soup = BeautifulSoup(request.content, "html.parser")
    list = soup.find('div', {'class': 'product_category_list'})
    if(list is not  None):
        productBlockList = list.find_all('div', {
            'class': ['subcategory-product-item', 'product_data__gtm-js', 'product_data__pageevents-js', 'ddl_product']})
        print(request.url)
        pageProduct = []
        logfileName = 'log/Items_' + datetime.datetime.now().strftime('%d_%m_%Y_%H') + '.json'
        with open(logfileName, mode='w', encoding='utf-8') as f:
            json.dump([], f)
        txtname = 'log/items_' + datetime.datetime.now().strftime('%d_%m_%Y_%H_%S') + '.txt'
        txtfile = open(txtname, 'w')
        for block in productBlockList:
            try:
                temp = block.get('data-params')
                data = ast.literal_eval(temp)
                name = data.get('shortName')
                imagelink = 'lost'
                temp = block.find('img')
                if temp is not None:
                    imageLink = temp.get('src')
                if imageLink is None:
                    temp = block.find('img')
                if temp is not None:
                    imageLink = temp.get('data-src')
                if imageLink is None:
                    temp = block.find('wrap-img')
                if temp is not None:
                    temp2 = temp.find('a')
                    if temp2 is not None:
                        imageLink = temp2.get('href')
                price = data.get('price')
                temp = block.find('a', {'class': 'link_no-border'})
                if temp is not None:
                    productLink = temp.get('href')
                if productLink is None:
                    productLink = block.find('a', {'class': 'ddl_product_link'}).get('href')
                curTime = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
                stock = block.find('div', {'class': 'in_stock_horizontal_position'})
                store = stock.find('span', {'class': 'item'})
                avalaible = store.findChild('span')
                if avalaible is not None:
                    avalaible = True
                else:
                    avalaible = False
                if name is not None and imageLink is not None and price is not None and productLink is not None:
                    product = ({'name': name, 'image': imageLink, 'price': price, 'link': productLink, 'time': curTime,
                                'avalaible': avalaible})
                    pageProduct.append(product)
                productstring = str(name) + ' ' + str(imageLink) + ' ' + str(price) + ' ' + str(productLink) + ' ' + str(
                    curTime) + ' ' + str(avalaible) + '\n'
                txtfile.write(productstring)
                with open(logfileName, mode='w', encoding='utf-8') as feedsjson:
                    json.dump(pageProduct, feedsjson)
            except BaseException:
                continue
        time.sleep(5)
        return pageProduct


def collectDataInCategory(link, useragent, proxies):
    from multiprocessing import Process
    from multiprocessing import managers


    print('CollectData')
    print(link)
    results = managers.queue.Queue()
    processes = []

    pageCount = getCitilinkPagesCount(link, useragent)
    if pageCount is not None:
        page = 1
        while page <= pageCount:
            try:
                print('page: ' + str(page))
                ranProxy = random.choice(list(proxies))
                dict = {'https': ranProxy}
                print(ranProxy)
                # noinspection PyArgumentList
                processPage(link, useragent, page, dict)
                #_process = Process(target=ProcessPage,
                 #                  name=str('thread_' + str(page)),
                  #                 args=(link, useragent, page, ranProxy, results))
                #_process.start()
                #processes.append(_process)
                page += 1
                time.sleep(5)
                print('page processed')
            except requests.exceptions.BaseHTTPError as e:
                print('request error: ' + str(e))

   # while not results.empty():
    #    print(results.get())
    #if len(processes) > 0:
     #   for _process in processes:
      #      _process.join()
       #     print('process {0} end', _process.name)


def processPage(link, useragent, page, proxy):  # ,  results):
    client, db = connectToMongo('88.206.57.135', 27017)
    print('Process page ' + str(page) + ' in thread: ' + str(threading.current_thread().getName()))
    request = get_content(link, useragent, page, proxy)
    if request is not None:
        document = parseContent(request)
        moveInDB(db, document)
    else:
       # results.put('Bad')
        print('request empty')
        curTime = datetime.datetime.now().strftime('%d_%m_%Y_%H')
        fileName = 'NoneAttributs' + curTime + '.txt'
        file = open(fileName, 'w')
        file.write(str(curTime) + ' ' + str(link) + ' : empty request\n')
        file.close()
    client.close()
    # results.task_done()


def getCitilinkPagesCount(url, useragent):
    print('GetCitilinkPagesCount from ' + str(url))
    headers = {'user-agent': str(useragent)}
    session = requests.Session()
    cookies = dict(_space='chlb_cl:')
    payload = {'available': '0'}
    request = session.post(str(url), headers=headers, params=payload, cookies=cookies)
    temp = SoupStrainer('li')
    soup = BeautifulSoup(request.content, 'html.parser', parse_only=temp)
    pageBlock = soup.find('li', {'class': 'last'})
    print('request status: ' + str(request.status_code))
    if pageBlock is not None:
        # noinspection PyRedundantParentheses
        pageCount = (int)(pageBlock.find('a').get('data-page'))
        print(str(url) + ' pages count = ' + str(pageCount))
        time.sleep(random.randrange(1, 5))
        return pageCount
    else:
        curTime = datetime.datetime.now().strftime('%d_%m_%Y_%H')
        fileName = 'NoneAttributs' + curTime + '.txt'
        file = open(fileName, 'w')
        file.write(str(curTime) + ' ' + str(url) + ' : number of pages not found\n')
        file.close()
        return None


def connectToMongo(host, port):
    import urllib.parse

    username = urllib.parse.quote_plus('ua')
    password = urllib.parse.quote_plus('pass')
    #client = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1' % (username, password))
    client = pymongo.MongoClient('localhost', 27017)
    db = client.get_database('Catalog')
    db.authenticate(username, password)
    return client, db


def moveInDB(db, array):
    import requests
    print('move in mongo')
    if(array is not None):
        for element in array:
            #insert_res = db.items.insert({'name' : str(element['name']), 'image' : str(element['image']), 'link' : str(element['link']), 'avalaible': element['avalaible'], 'history' : {'price' : str(element['price']), 'time' : str(element['time'])}})
            #print(str(insert_res))
            params = {}
            params['$set'] = {'available': element['avalaible'], 'image': str(element['image']),
                'link': str(element['link'])};
            params['$push'] = {'history': {
                '$each': [{'price': element['price'], 'date': str(element['time'])}]}}
            doc = db.Items.find_one({'name': element['name']})
            if(doc['name'] == element['name'] and (doc['available'] != element['avalaible'] or doc['history'][-1]['price'] != element['price']) ):
                sqlconnect(doc['_id'], element['time'])
                payload = {'id': doc['_id'], 'date': element['time']}
                requests.post("http://88.206.123.192:8080/Change/NotificateAboutChanges", data=payload)
            db.Items.update_one({"name": str(element['name'])}, params, upsert=True)
        print('update count: ' + str(db.Items.count()))


def sqlconnect(id, date):
    import pymssql
    print('start connect')
    try:
        connect = pymssql.connect(server='192.168.1.41', port=1488, user='Eujhm', password='gotmema_$git*Sva',
                                  database='Uzhik')
        cursor = connect.cursor()
        #date = '\'' + str(datetime.datetime(2012, 12, 12, 12, 12, 12)) + '\''  # , "%Y.%m.%d %H:%M:%S"))
        #id = '\'567f1f77bcf86cd799439011\''
        request = 'INSERT INTO UPDATES (Id, Date) VALUES ({0}, {1})'.format(id, date)
        cursor.execute(request)
        connect.commit()
    except Exception as e:
        print(e)
    finally:
        connect.close()



def checkProxy(proxy):
    flag = True
    try:
        session = requests.Session()
        req = session.get('https://www.citilink.ru/', proxies={'https': proxy})
        if req.status_code != 200:
            flag = False
    except Exception as detail:
        print('ERROR:', detail)
        flag = False
    finally:
        session.close()
        return flag


def getProxies():
    file = open('httpsproxy.txt', 'r')
    list = []
    dict = {}
    count = 0
    for row in file:
        address = 'https://' + str(row).rstrip()
        list.append({'https': address})
    for item in list:
        name = item['https']
        flag = checkProxy(name)
        print(str(flag) + ' wait...' + str(count))
        count += 1
        if flag is True:
            dict[name] = item
    return dict


if __name__ == '__main__':
    # try:
    userAgents = open('useragents.txt').read().split('\n')

    citilinkCategories = get_catalog_links()
    proxies = getProxies()
    # noinspection PyRedundantParentheses
    print(random.choice(list((proxies))))

    for category in citilinkCategories:
        collectDataInCategory(category, random.choice(userAgents), proxies)
    print('End')
    # except Exception as e:
    #    print(str(e))          
    # finally:
