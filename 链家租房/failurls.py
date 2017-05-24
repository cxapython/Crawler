import re
import asynqp
import asyncio


async def message(mess):
    connection = await asynqp.connect()
    channel = await connection.open_channel()
    exchange = await channel.declare_exchange('lianjia_zufang.exchange', 'direct')
    queue = await channel.declare_queue('lianjia_zufang.queue')
    await queue.bind(exchange, 'routing.key')
    msg = asynqp.Message(mess)
    exchange.publish(msg, 'routing.key')
    await channel.close()
    await connection.close()


async def fail(log):
    with open(log, 'r') as f:
        data = f.read()
        url = r'(http://.*)'
        urls = re.findall(url, data)
        for url in urls:
            await message(url)
            print('send ---', url)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # tasks = [asyncio.ensure_future(fail('logurl.log')),
    #          asyncio.ensure_future(fail('logitem.log'))]
    # loop.run_until_complete(asyncio.gather(*tasks))
    loop.run_until_complete(fail('logitem.log'))
    loop.close()