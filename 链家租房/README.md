# asyncio + aiohttp + RabbitMQ的分布式爬虫


利用Python的aio库写的异步抓取链家网租房信息的分布式爬虫 ，asyncio作为消息事件循环
aiohttp负责请求页面，返回网页源代码，XPath负责解析信息
RabbitMQ负责发送和接收队列

**先创建csv文件，可以运行 Python3 run.py**
接着 开启RabbitMQ服务 **rabbitmq-server**
MongoDB服务 **mongod**
 运行 **Python3 lianjia_zufang_item.py** 等待接收队列

**Python3 lianjia_zufang.py** 发送消息队列

