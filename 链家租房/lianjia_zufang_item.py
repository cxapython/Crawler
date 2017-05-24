import asyncio
from aiohttp import ClientSession, ClientError
from faker import Factory
from datas import create_logging, write_to_csv, write_to_mongodb
from exchange import reconnector
from lxml import etree
from hashlib import sha224


fake = Factory.create()


class CrawlItem(object):
    def __init__(self, max_tries=4, max_tasks=10, loop=asyncio.get_event_loop(), queue=asyncio.Queue()):
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.loop = loop
        self.queue = queue
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = ClientSession(
                headers={'User-Agent': fake.user_agent()}, loop=self.loop)
        return self._session

    @session.setter
    def session(self, values):
        self._session = values

    async def fetch(self, url):
        tries = 0
        while tries < self.max_tries:
            try:
                response = await self.session.get(url.decode(), allow_redirects=False)
                break
            except ClientError as client_error:
                logger.info(client_error)
            tries += 1
        else:
            return
        try:
            if response.status == 200:
                print(response.url, 'fetch')
                await self.fetch_info(response)
            else:
                logger.info(response.url)
                logger.info(response.status)
        finally:
            await response.release()

    @staticmethod
    async def fetch_info(response):
        rs = await response.read()
        selector = etree.HTML(rs)
        try:
            house = dict()
            response_url = str(response.url)
            house['链接'] = response_url
            house['价格'] = selector.xpath('//span[@class="total"]/text()')[0]
            house['_id'] = sha224(response_url.encode()).hexdigest()
            il = selector.xpath('//div[@class="zf-room"]/p/text()')
            if il:
                house['面积'] = il[0]
                house['房屋户型'] = il[1]
                house['楼层'] = il[2]
                house['房屋朝向'] = il[3]
                house['地铁'] = il[4]
                il2 = selector.xpath('//div[@class="zf-room"]/p/a/text()')
                house['小区'] = ''.join(il2[:2])
                house['位置'] = ''.join(il2[2:])
                house['时间'] = il[-1]
                il3 = selector.xpath('//div[@class="base"]/div[@class="content"]/ul/li/text()')
                il3 = [i for i in map(str.strip, il3) if i != '']
                if len(il3) == 4:
                    house['租赁方式'] = il3[0]
                    house['付款方式'] = il3[1]
                    house['房屋现状'] = il3[2]
                    house['租赁周期|供暖方式'] = il3[3]
                elif len(il3) == 3:
                    il3 = selector.xpath('//div[@class="base"]/div[@class="content"]/ul/li')
                    house['租赁方式'] = il3[0].xpath('text()')[0]
                    house['付款方式'] = il3[1].xpath('a/text()')[0]
                    house['房屋现状'] = il3[2].xpath('text()')[0]
                    house['租赁周期|供暖方式'] = il3[3].xpath('text()')[0]
                else:
                    house['租赁方式'] = '没抓取成功'
                    house['付款方式'] = '没抓取成功'
                    house['房屋现状'] = '没抓取成功'
                    house['租赁周期'] = '没抓取成功'
                il4 = selector.xpath('//div[@class="featureContent"]/ul/li')
                try:
                    for i in il4:
                        house[i.xpath('span[@class="label"]/text()')[0][:-1].strip()] \
                            = i.xpath('span[@class="text"]/text()')[0]
                except IndexError:
                    for i in range(len(il4)):
                        if i == 0:
                            house[il4[i].xpath('span[@class="label"]/text()')[0][:-1].strip()] \
                                = il4[i].xpath('span[@class="text"]/span/text()')[0]
                        else:
                            house[il4[i].xpath('span[@class="label"]/text()')[0][:-1].strip()] \
                                = il4[i].xpath('span[@class="text"]/text()')[0]
                write_to_csv(house)
                print('写入数据')
                await write_to_mongodb(house)
        except Exception as e:
            logger.info(e)
            logger.info(response.url)

    async def process_msgs(self, queue):
        try:
            while 1:
                msg = await queue.get()
                await self.fetch(msg.body)
                msg.ack()
                # asyncio.sleep(5)
        except asyncio.CancelledError:
            pass

        except Exception as e:
            self.q.task_done()

    def run(self):
        reconnect_task = self.loop.create_task(reconnector(self.queue))
        process_task = [self.loop.create_task(self.process_msgs(self.queue)) for _ in range(self.max_tasks)]
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            process_task.cancel()
            reconnect_task.cancel()
            self.loop.run_until_complete(process_task)
            self.loop.run_until_complete(reconnect_task)
        self.loop.close()


if __name__ == "__main__":
    logger = create_logging('链家ITEM', 'logitem.log')
    crawl = CrawlItem(max_tasks=10)
    crawl.run()
