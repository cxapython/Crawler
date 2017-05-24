import asyncio
from aiohttp import ClientSession, ClientError
from asyncio import Queue
from faker import Factory
from lxml import etree
from datas import create_logging, write_header_csv
from exchange import message
import time
from urllib.parse import urljoin


fake = Factory.create()


class CrawlURL(object):
    def __init__(self, max_tries=2, max_tasks=10, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.q = Queue(loop=self.loop)
        self.seen_urls = set()
        self.t0 = time.time()
        self.t1 = None
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = ClientSession(headers={'User-Agent': fake.user_agent()}, loop=self.loop)
        return self._session

    @session.setter
    def session(self, values):
        self._session = values

    def close(self):
        self.session.close()

    @staticmethod
    async def parse_link(response):
        rs = await response.read()
        selector = etree.HTML(rs)
        urls = selector.xpath('//div[@class="info-panel"]/h2/a/@href')
        if urls:
            for uri in urls:
                if 'sh.lianjia.com' in str(response.url):
                    pass
                    # 因为上海的网页结构和其它的不一样，懒得处理
                    # await message(urljoin(str(response.url), uri))
                else:
                    await message(uri)

    async def fetch(self, url):
        tries = 0
        while tries < self.max_tries:
            try:
                response = await self.session.get(url, allow_redirects=False)
                break
            except ClientError as client_error:
                logger.info(client_error)
            tries += 1
        else:
            return
        try:
            if response.status == 200:
                print(response.url, 'fetch')
                await self.parse_link(response)
            else:
                logger.info(response.url)
                logger.info(response.status)

        finally:
            await response.release()

    async def work(self):
        try:
            while 1:
                url = await self.q.get()
                assert url in self.seen_urls
                await self.fetch(url)
                self.q.task_done()
                # asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.info(e)
            logger.info(url)
            self.q.task_done()

    def add_url(self, url):
        if url not in self.seen_urls:
            self.seen_urls.add(url)
            self.q.put_nowait(url)

    async def crawl(self):
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.t0 = time.time()
        await self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()


if __name__ == '__main__':
    logger = create_logging('链家URL', 'logurl.log')
    write_header_csv()
    # 琼海没有信息
    # 苏州没有信息
    # 石家庄没有信息
    # 沈阳没有信息
    # 三亚没有信息
    # 文昌没有信息
    # 万宁没有信息
    # 海口没有信息
    # 西安没有信息
    # 陵水没有信息
    # 廊坊燕郊没有信息
    URLs = ['http://bj.lianjia.com/zufang/pg{}/', 'http://nj.lianjia.com/zufang/pg{}/',
            'http://cd.lianjia.com/zufang/pg{}/', 'http://cq.lianjia.com/zufang/pg{}/',
            'http://cs.lianjia.com/zufang/pg{}/', 'http://qd.lianjia.com/zufang/pg{}/',
            'http://dl.lianjia.com/zufang/pg{}/', 'http://dg.lianjia.com/zufang/pg{}/',
            'http://sh.lianjia.com/zufang/d{}/', 'http://sz.lianjia.com/zufang/pg{}/',
            'http://fs.lianjia.com/zufang/pg{}/', 'http://tj.lianjia.com/zufang/pg{}/',
            'http://gz.lianjia.com/zufang/pg{}/', 'http://wh.lianjia.com/zufang/pg{}/',
            'http://hz.lianjia.com/zufang/pg{}/', 'http://hf.lianjia.com/zufang/pg{}/',
            'http://xm.lianjia.com/zufang/pg{}/', 'http://jn.lianjia.com/zufang/pg{}/',
            'http://yt.lianjia.com/zufang/pg{}/', 'http://zs.lianjia.com/zufang/pg{}/',
            'http://zh.lianjia.com/zufang/pg{}/'
            ]
    loop = asyncio.get_event_loop()
    crawler = CrawlURL(max_tasks=10)
    for URL in URLs:
        for num in range(1, 101):
            crawler.add_url(URL.format(num))
    # crawler.add_url('http://bj.lianjia.com/zufang/pg16/') #测试
    loop.run_until_complete(crawler.crawl())
    print('Finished in {:.3f} seconds'.format(crawler.t1 - crawler.t0))
    print('一共抓取网页--->', len(crawler.seen_urls))
    crawler.close()
    loop.close()
