import logging
import requests
import sqlite3
import threading

from random import choice
from lxml.html import document_fromstring, tostring
from queue import Queue
from time import sleep
from html import unescape

result_list = []
item_list = []
with_error_links = []

class Downloader(threading.Thread):
    def __init__(self, queue, crawler):
        threading.Thread.__init__(self)
        self.queue = queue
        self.crawler = crawler

    def run(self):
        while True:
            urls = self.queue.get()
            self.download_file(urls)
            self.queue.task_done()

    def download_file(self, urls):
        global result_list
        for url in urls:
            response = self.crawler.make_request(url)
            if response:
                doc = document_fromstring(response.text)
                els = doc.xpath("//div[@class='i_square_info']//a")
                for el in els:
                    result_list.append((el.get('href'),))

class DesignerDownloader(threading.Thread):
    def __init__(self, queue, sql_queue, crawler, number):
        threading.Thread.__init__(self)
        self.queue = queue
        self.crawler = crawler
        self.number = number
        self.sql_queue = sql_queue

    def run(self):
        print('Thread # %s started' % self.number)
        while True:
            url = self.queue.get()
            if not url:
                print('Thread # %s done' % self.number)
                break
            print('Thread %s got url: %s' % (self.number, url))
            data = self.download_designer(url)
            if data:
                self.sql_queue.put(data)
            else:
                self.queue.put(url)
        self.queue.task_done()

    def download_designer(self, url):
        global item_list
        global with_error_links
        response = self.crawler.make_request(url)
        if response:
            doc = document_fromstring(response.text)
            raw_name = doc.xpath("//div[@class='wrap_profile_top']")
            raw_web = doc.xpath("//a[@class='user_contact-web']")
            els = doc.xpath("//div[@class='user_contact-item']")
            try:
                address = unescape(tostring(els[1]).decode('utf-8')).split('> ')[1].split('</d')[0]
            except IndexError:
                address = '-'
            try:
                geography = unescape(tostring(els[2]).decode('utf-8')).split('> ')[1].split('</d')[0]
            except IndexError:
                geography = '-'
            try:
                sphere = unescape(tostring(els[3]).decode('utf-8')).split('> ')[1].split('</d')[0]
            except IndexError:
                sphere = '-'

            web = raw_web[0].text if raw_web else '-'
            name = raw_name[0].get('data').split('name":"')[1].split('"}')[0]
            return (name, web, address, geography, sphere)
        else:
            with_error_links.append(url)

class SqlWriter(threading.Thread):
    def __init__(self, sql_queue, number, exist_designers):
        threading.Thread.__init__(self)
        self.number_designers = number
        self.sql_queue = sql_queue
        self.exist_designers = exist_designers

    def run(self):
        conn = sqlite3.connect("mydatabase.db")
        cursor = conn.cursor()
        counter = 0
        while counter <= self.number_designers:
            data = self.sql_queue.get()
            if data and data[0] not in self.exist_designers:
                cursor.executemany("INSERT INTO designers VALUES (?,?,?,?,?)", [data])
                conn.commit()
        self.sql_queue.task_done()

class Crawler:
    user_agents = None
    proxys = None
    total_pages_number = 0
    start_url = None
    page_links = None
    designers_links = None
    designers = {}

    def __init__(self, url):
        self.start_url = url
        with open('useragents.txt', ) as fd:
            self.user_agents = [line.split('\t')[0] for line in fd if line.strip()]
        with open('proxys.txt') as fd:
            self.proxys = [line.strip() for line in fd if line.strip()]

    def get_total(self):
        response = self.make_request(self.start_url)
        doc = document_fromstring(response.text)
        els = doc.xpath("//div[@class='pagination']//a")
        self.total_pages_number = int(els[-2].text.strip())

    def make_request(self, url):
        for x in range(15):
            user_agent = {'User-Agent': choice(self.user_agents).strip()}
            proxy = {'http': 'http://' + choice(self.proxys).strip()}
            try:
                response = requests.get(url, headers=user_agent, proxies=proxy)
                break
            except:
                sleep(2)
                response = None
        if not response:
            logging.error('cannot connect to url: '+url)
        return response

    def set_total_links(self, links):
        self.page_links = links

    def set_designer_links(self, links):
        self.designers_links = links

    def set_designers(self, designers):
        self.designers = designers

    def collect_links(self, links, conn):
        cursor = conn.cursor()
        queue = Queue()

        threads = [Downloader(queue, self) for x in range(2)]
        for t in threads:
            t.setDaemon(True)
            t.start()

        queue.put([links[0]])
        queue.put([links[1]])

        queue.join()

        for row in result_list:
            logging.info(row)
        cursor.executemany("INSERT INTO a_links VALUES (?)", result_list)
        conn.commit()

    def collect_designers(self):
        queue = Queue()
        sql_queue = Queue()

        t_number = 10

        [queue.put(link) for link in self.designers_links]

        threads = [DesignerDownloader(queue, sql_queue, self, x) for x in range(1, t_number+1)]
        for t in threads:
            t.setDaemon(True)
            t.start()
        sql_writer = SqlWriter(sql_queue, len(self.designers_links)-len(self.designers), self.designers)
        sql_writer.setDaemon(True)
        sql_writer.start()

        queue.join()

    def status(self):
        print('Number of user agents: %d' % len(self.user_agents) if self.user_agents else 0)
        print('Number of proxys: %d' % len(self.proxys) if self.proxys else 0)
        print('Total pages on web: %d' % self.total_pages_number if self.total_pages_number else 0)
        print('Start url: %s' % self.start_url if self.start_url else 'No exist')
        print('Total number of pages in DB: %d' % len(self.page_links) if self.page_links else 0)
        print('Total disigners links in DB: %d' % len(self.designers_links) if self.designers_links else 0)
        print('Total disigners in DB: %d' % (len(self.designers) if self.designers else 0))

def init_main(url):
    logging.info('Start app')
    crawler = Crawler(url)
    crawler.get_total()
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()

    sql = "SELECT link FROM links"
    cursor.execute(sql)
    links = [row[0] for row in cursor.fetchall()]
    crawler.set_total_links(links)

    sql = "SELECT link FROM a_links"
    cursor.execute(sql)
    links = [row[0] for row in cursor.fetchall()]
    crawler.set_designer_links(links)

    sql = "SELECT name FROM designers"
    cursor.execute(sql)
    designers = [row[0] for row in cursor.fetchall()]
    crawler.set_designers(designers)

    return crawler

def generate_links(conn, crawler):
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE links
                      (link text)
                   """)
    links = [(crawler.start_url+'/p/'+str(x),) for x in range(1, crawler.total_pages_number+1)]
    cursor.executemany("INSERT INTO links VALUES (?)", links)
    conn.commit()

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE designers
                          (name text, web text, address text, geography text, sphere text)
                       """)
    conn.commit()

def main(url):
    crawler = init_main(url)
    crawler.status()
    crawler.collect_designers()
    #print(crawler.designers)
    logging.info('Stop app')

if __name__ == '__main__':
    url = 'http://www.abitant.com/users'
    logging.basicConfig(filename='main.log', format='%(asctime)s %(message)s', level=logging.DEBUG)
    main(url)