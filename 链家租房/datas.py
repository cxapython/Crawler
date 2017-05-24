import logging
import csv
from motor import motor_asyncio
from pymongo.errors import DuplicateKeyError

file = 'lianjia_zufang.csv'


fieldnames = ['链接', '价格', '面积', '房屋户型', '楼层', '房屋朝向', '地铁',
              '小区', '位置', '时间', '租赁方式', '付款方式', '房屋现状',
              '租赁周期|供暖方式', '装修描述', '小区介绍', '学区介绍', '核心卖点',
              '周边配套', '交通出行', '户型介绍', '投资分析', '出租原因',
              '房源亮点', '_id']


def create_logging(logger_name, logger_file):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    handle_write = logging.FileHandler(logger_file)
    handle_print = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    handle_print.setFormatter(formatter)
    handle_write.setFormatter(formatter)
    logger.addHandler(handle_print)
    logger.addHandler(handle_write)
    return logger


def write_header_csv():
    with open(file, 'w', encoding='utf8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()


def write_to_csv(house_dict):
    with open(file, 'a+', encoding='utf8') as csvfile:
        global fieldnames
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(house_dict)


async def write_to_mongodb(house):
    client = motor_asyncio.AsyncIOMotorClient('localhost', 27017)
    db = client.lianjia
    collection = db.zufang
    try:
        await collection.insert(house)
    except DuplicateKeyError:
        pass
