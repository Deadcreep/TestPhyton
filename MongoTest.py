# -*- coding: utf8 -*-

def ChangeHistory():
    import pymongo
    import urllib.parse
    import time

    username = urllib.parse.quote_plus('ua')
    password = urllib.parse.quote_plus('pass')
    # client = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1' % (username, password))
    client = pymongo.MongoClient('localhost', 27017)
    db = client.get_database('Catalog')
    db.authenticate(username, password)
    items = db.items
    cursor = items.find({}, modifiers={"$snapshot": True})
    count = cursor.count()
    index = 0
    while index != count:
        doc = cursor[index]

        print('Update' + doc['name'])
        temp = doc['history']
        price = temp['price']
        date = temp['time']
        # items.update_one({'_id': doc['_id']}, {'$unset': {'history': 1}})
        doc['history'] = [{'price': price, 'date': date}]
        # db.items.update_one({"_id": doc["_id"]}, params, upsert=False)


        items.save(doc)
        print(doc['history'])
        index += 1
    cursor.close()


def sqlconnect(nHost='localhost', nBase='Uzhik', nUser='user', nPasw='123'):
    import pyodbc
    import pymssql
    import datetime
    print('start connect')
    try:

        connect = pymssql.connect(server='88.206.57.135', port=1488, user='Eujhm', password='gotmema_$git*Sva', database='Uzhik')
        cursor = connect.cursor()
        cursor.execute('SELECT * FROM UPDATES')
        row = cursor.fetchone()
        print(row)
        date = '\'' + str(datetime.datetime(2012, 12, 12, 12, 12, 12)) + '\''
        print(date)
        id = '\'567f1f77bcf86cd799439011\''
        request = 'INSERT INTO UPDATES (Id, Date) VALUES ({0}, {1})'.format(id, date)
        data = (id, date)
        # perform the query
        #cursor.execute("INSERT INTO PRODUCTS(_id, date) VALUES(%s, %s)" % data)
        print(request)
        res = cursor.execute(request)
        connect.commit()
        print(res)

        return connect
    except Exception as e:
        print(e)
        return ''


def checkProxy(proxy):
    import time
    flag = True
    try:
        session = requests.Session()
        req = session.get('https://www.citilink.ru/', proxies={'https': proxy})
        print(req.status_code)
        if req.status_code != 200:
            flag = False
    except Exception as detail:
        print('ERROR:', detail)
        flag = False
    finally:
        time.sleep(0.00001)
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





def moveInDB():
    import urllib.parse
    username = urllib.parse.quote_plus('ua')
    password = urllib.parse.quote_plus('pass')
    # client = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1' % (username, password))
    client = pymongo.MongoClient('88.206.57.135', 27017)
    db = client.get_database('Catalog')
    db.authenticate(username, password)

    doc = db.Items.find_one({"name": "Ноутбук MSI GL72M 7REX-1236RU, черный"})
    print(doc['available'])

    params = {}
    params['$set'] = {'available': True};
    params['$push'] = {'history': {
        '$each': [{'price': 74000, 'date': '23.11.17 12:12'}]}}
    #db.Items.update_one({"name": "Ноутбук MSI GL72M 7REX-1236RU, черный"}, params, upsert=True)
    doc = db.Items.find_one({"name": "Ноутбук SAMSUNG NP355E5X-A01, черный"})
    print(doc['available'])

    params = {}
    params['$set'] = {'available': True};
    #db.Items.update_one({"name": "Ноутбук SAMSUNG NP355E5X-A01, черный"}, params, upsert=True)

    print(doc['available'])
    doc = db.Items.find_one({"name": "Нетбук ASUS Eee PC 1015BX-WHI180S, белый "})

    params = {}
    params['$push'] = {'history': {
        '$each': [{'price': 9999, 'date': '23.11.17 12:12'}]}}
    #db.Items.update_one({"name": "Нетбук ASUS Eee PC 1015BX-WHI180S, белый "}, params, upsert=True)






if __name__ == '__main__':
    import pymongo
    import urllib.parse
    import  datetime
    import pymssql
    import requests

    #dict = getProxies
    moveInDB()

    #payload = {'id': '5a149b4129c52c64423703c8', 'date': '12.12.12 12:12'}
    #r = requests.post("http://88.206.123.192:8080/Change/NotificateAboutChanges", data=payload)
    #print(r.status_code)
    #print(r.text)


