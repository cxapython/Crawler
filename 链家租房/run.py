import csv

file = 'lianjia_zufang.csv'

fieldnames = ['链接', '价格', '面积', '房屋户型', '楼层', '房屋朝向', '地铁',
              '小区', '位置', '时间', '租赁方式', '付款方式', '房屋现状',
              '租赁周期|供暖方式', '装修描述', '小区介绍', '学区介绍', '核心卖点',
              '周边配套', '交通出行', '户型介绍', '投资分析', '出租原因',
              '房源亮点', '_id']


def create_csv(file_name):
    with open(file_name, 'w',) as csvfile:
        global fieldnames
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

create_csv(file)