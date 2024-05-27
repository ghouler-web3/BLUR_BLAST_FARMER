import asyncio
from core.blur import Blur, db
import aiofiles
import pandas as pd
from io import BytesIO

from helpers.account import AccountData
from helpers.logger import Logger
from helpers.utils import get_proxy_web3, get_normal_web3

from config import USE_TG_BOT, TOKEN, ADMIN_ID, BLAST_RPC

async def read_excel(file_path):
    async with aiofiles.open(file_path, 'rb') as f:
        return await f.read()

async def process_data_frame(df, logger):
    semaphore = asyncio.Semaphore(len(list(df.keys())))

    async def process_row(row, original_index):
        async with semaphore:
            result = await start_wrapper(row, logger)
            result['original_order'] = original_index
            return result

    tasks = [process_row(row, idx) for idx, row in df.iterrows()]
    results = await asyncio.gather(*tasks)
    return results

async def start_wrapper(data, logger):
    await asyncio.sleep(1)
    account_data = AccountData(data)
    json_data = account_data.to_json()
    if account_data.account:

            router_inst = Blur(acc_data=account_data, logger=logger)
            w3 = await get_proxy_web3(None if not account_data.proxy else account_data.proxy.as_url, await get_normal_web3(BLAST_RPC))
            account_data.w3 = await get_proxy_web3(proxy=None if not account_data.proxy else account_data.proxy.as_url, w3=w3)

            res = await router_inst.main_()
            json_data = account_data.to_json()
            json_data['res'] = res
            json_data['error'] = 'none'
            return json_data

    else:
        logger.log_error(f"Критическая ошибка во время работы (private_key '{account_data.private_key}' is not valid)")
        json_data['res'] = 'bad_private_key'
        json_data['error'] = 'bad_private_key'
        return json_data

async def start_bot():
    from aiogram import Bot, Dispatcher
    from core.tg import router
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)

async def start_parser():
    from core.parser import Parser
    parser = Parser()
    await parser.main()

async def main():
    logger = Logger()
    data_content = await read_excel('data.xlsx')
    df = pd.read_excel(BytesIO(data_content), engine='openpyxl')

    if USE_TG_BOT:
        if not TOKEN or not ADMIN_ID:
            logger.log_error(f"Не могу начать работу, т.к. не указан BOT TOKEN или ADMIN ID")
            return
        asyncio.create_task(start_bot())

    asyncio.create_task(start_parser())

    await db.update_many({'finished': False}, {"$set": {"finished": True}})

    await process_data_frame(df, logger=logger)
    logger.log("Успешно закончил работу!")

if __name__ == '__main__':
    asyncio.run(main())