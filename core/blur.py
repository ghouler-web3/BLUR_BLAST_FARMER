import asyncio
import time
import random
import math

from eth_account.messages import encode_defunct, encode_typed_data
from datetime import datetime, timedelta

from core.blur_onchain import Onchain
from helpers.fetcher import AsyncFetcher
from helpers.database import db, parser_db, lister_db

from config import USE_TG_BOT, TOKEN, ADMIN_ID

if USE_TG_BOT and ADMIN_ID and TOKEN:
    from aiogram import Bot
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    close_button = InlineKeyboardButton(text='❌ Закрыть', callback_data='close')
    close_markup = InlineKeyboardMarkup(inline_keyboard=[[close_button]])
    bot = Bot(token=TOKEN)

class Blur(AsyncFetcher):

    def __init__(self, acc_data, logger):

        self.ad = acc_data
        self.logger = logger
        self.session = None
        self.auth = None

        self.semaphore = asyncio.Semaphore(1)
        self.onchain = Onchain()

        self.pool_status = False
        self.listing_tasks = []

    async def get_balance(self):
        for i in range(3):
            try:
                self.blur_balance = await self.onchain.get_balance(self.ad.w3, self.ad.account, '0xB772d5C5F4A2Eef67dfbc89AA658D2711341b8E5') / (10**18)
                self.balance = await self.ad.w3.eth.get_balance(self.ad.account.address) / (10**18)

                await db.update_one({'address': self.ad.account.address.lower()}, {"$set": {"blur_balance": round(self.blur_balance, 4), "balance": round(self.balance, 4)}})

            except:
                await asyncio.sleep(i+1)

    async def get_challenge(self):
        for i in range(3):
            try:

                payload = {"walletAddress": self.ad.account.address}
                res = await self.fetch_url(session=self.session, method='POST', payload=payload, url='https://core-api.prod-blast.blur.io/auth/challenge')

                if not res or not res.get('json'):
                    raise Exception

                return res['json']

            except:
                await asyncio.sleep(i+1)

    async def login(self, payload):
        for i in range(3):
            try:
                res = await self.fetch_url(session=self.session, payload=payload, method='POST', url='https://core-api.prod-blast.blur.io/auth/login')

                if not res or not res.get('json'):
                    raise Exception

                return res['json']['accessToken']

            except:
                await asyncio.sleep(i+1)

    async def get_auth(self):
        for i in range(5):
            try:
                challenge = await self.get_challenge()
                if not challenge:
                    raise Exception

                signature = self.ad.account.sign_message(encode_defunct(text=challenge['message']))
                login_payload = {"message": challenge['message'], "walletAddress": self.ad.account.address, "expiresOn": challenge["expiresOn"], "hmac": challenge["hmac"], "signature": signature.signature.hex()}

                login = await self.login(login_payload)
                if not login_payload:
                    raise Exception

                return login

            except:
                await asyncio.sleep(i+1)

    async def get_bids(self, type = 'COLLECTION'):
        for i in range(3):
            try:

                params = {
                    'filters': f'{{"criteria":{{"type":"{type}"}}}}',
                }

                res = await self.fetch_url(session=self.session, method='GET', params=params, url=f"https://core-api.prod-blast.blur.io/v1/collection-bids/user/{self.ad.account.address.lower()}")

                if not res or not res.get('json'):
                    raise Exception

                if res['json']['success']:
                    return res['json']['priceLevels']

                raise Exception

            except:
                await asyncio.sleep(i+1)

    async def get_bid_data(self, price, quantity, contract):
        for i in range(3):
            try:
                current_time = datetime.utcnow() + timedelta(days=7)
                formatted_time = current_time.isoformat() + 'Z'
                json_data = {
                    'price': {
                        'amount': str(price),
                        'unit': 'BETH',
                    },
                    'quantity': quantity,
                    'expirationTime': formatted_time,
                    'contractAddress': contract,
                    'criteria': {
                        'type': 'COLLECTION',
                        'value': {},
                    },
                }
                res = await self.fetch_url(session=self.session, method='POST', url='https://core-api.prod-blast.blur.io/v1/collection-bids/format', payload=json_data)

                if not res or not res.get('json'):
                    raise Exception

                return res['json']['signatures']

            except:
                await asyncio.sleep(i + 1)

    async def submit_bid(self, signature, market_data):
        for i in range(3):
            try:
                json_data = {
                    'signature': signature,
                    'marketplaceData': market_data,
                }
                res = await self.fetch_url(session=self.session, method='POST', url='https://core-api.prod-blast.blur.io/v1/collection-bids/submit', payload=json_data)

                if not res or not res.get('json'):
                    raise Exception

                if res['json']['success']:
                    return True

                return False

            except:
                await asyncio.sleep(i + 1)

    async def make_bid(self, price, quantity, contract):
        for i in range(3):
            try:

                bid_data = await self.get_bid_data(price, quantity, contract)
                if not bid_data:
                    raise Exception

                sign_data = await self.construsct_sign_data(bid_data)
                if not sign_data:
                    return Exception

                signature = self.ad.account.sign_message(encode_typed_data(full_message=sign_data))

                submit_bid = await self.submit_bid(signature.signature.hex(), bid_data[0]['marketplaceData'])
                if submit_bid:
                    return True

                return False

            except:
                await asyncio.sleep(i+1)

    async def construsct_sign_data(self, data):
            sign_data = {
                'types': data[0]['signData']['types'],
                'primaryType': 'Order',
                'domain': data[0]['signData']['domain'],
                'message': data[0]['signData']['value'],
            }

            #sign_data['domain']['chainId'] = int(sign_data['domain']['chainId'], 16)
            sign_data['message']['listingsRoot'] = bytes.fromhex(sign_data['message']['listingsRoot'][2:])
            sign_data['message']['expirationTime'] = int(sign_data['message']['expirationTime'])
            sign_data['message']['salt'] = int(sign_data['message']['salt'], 16)
            sign_data['message']['nonce'] = int(sign_data['message']['nonce']['hex'], 16)
            sign_data['types'].update({"EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"}, {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"}]})

            return sign_data

    async def cancel_bid(self, price, contract):
        for i in range(3):
            try:
                if isinstance(price, float) and str(price).endswith('.0'):
                    price = str(price)[:-2]
                json_data = {
                    'contractAddress': contract,
                    'criteriaPrices': [
                        {
                            'price': str(price),
                            'criteria': {
                                'type': 'COLLECTION',
                                'value': {},
                            },
                        },
                    ],
                }

                res = await self.fetch_url(session=self.session, method='POST', url='https://core-api.prod-blast.blur.io/v1/collection-bids/cancel', payload=json_data)

                if not res or not res.get('json'):
                    raise Exception

                if res['json']['success']:
                    return contract, True

                raise Exception

            except:
                await asyncio.sleep(i+1)
        return contract, False

    async def submit_listing(self, signature, market_data):
        for i in range(3):
            try:
                json_data = {
                    'marketplace': 'BLUR',
                    'signature': signature,
                    'marketplaceData': market_data,
                }
                res = await self.fetch_url(session=self.session, method='POST', url='https://core-api.prod-blast.blur.io/v1/orders/submit', payload=json_data)

                if not res or not res.get('json'):
                    raise Exception

                if res['json']['success']:
                    return True

                return False

            except:
                await asyncio.sleep(i + 1)

    async def get_listings(self, task_data):
        for i in range(3):
            try:

                params = {
                    'filters': '{"traits":[],"hasAsks":true}',
                }

                res = await self.fetch_url(session=self.session, method='GET', url=f"https://core-api.prod-blast.blur.io/v1/collections/{task_data['openseaSlug']}/prices", params=params)

                if not res or not res.get('json'):
                    raise Exception

                return res['json']['nftPrices']

            except:
                await asyncio.sleep(i+5)

    async def get_listing_fee(self, task_data):
        for i in range(3):
            try:

                res = await self.fetch_url(session=self.session, method='GET', url=f"https://core-api.prod-blast.blur.io/v1/collections/{task_data['contractAddress']}/fees")

                if not res or not res.get('json'):
                    raise Exception

                return int(res['json']['fees']['byMarketplace']['BLUR']['minimumRoyaltyBips'])

            except:
                await asyncio.sleep(i+5)
        return 0

    async def get_listing_format(self, task_data, price, fee_rate=0):
        for i in range(3):
            try:

                current_time = datetime.utcnow() + timedelta(days=7)
                formatted_time = current_time.isoformat() + 'Z'

                json_data = {
                    'marketplace': 'BLUR',
                    'orders': [
                        {
                            'price': {
                                'amount': str(price),
                                'unit': 'ETH',
                            },
                            'tokenId': str(task_data['tokenId']),
                            'feeRate': fee_rate,
                            'contractAddress': task_data['contractAddress'],
                            'expirationTime': formatted_time,
                        },
                    ],
                }
                res = await self.fetch_url(session=self.session, method='POST', payload=json_data, url=f"https://core-api.prod-blast.blur.io/v1/orders/format")

                if not res or not res.get('json'):
                    raise Exception

                return res['json']

            except:
                await asyncio.sleep(i+1)

    async def get_item_events(self, task_data):
        for i in range(3):
            try:

                params = {
                    'filters': f'{{"count":50,"tokenId":"{task_data["tokenId"]}","contractAddress":"{task_data["contractAddress"]}","eventFilter":{{"mint":{{}},"sale":{{}},"transfer":{{}},"orderCreated":{{}}}}}}',
                }

                res = await self.fetch_url(session=self.session, method='GET', url=f"https://core-api.prod-blast.blur.io/v1/activity/event-filter", params=params)

                if not res or not res.get('json'):
                    raise Exception

                for item in res['json']['activityItems']:
                    if item['eventType'] == 'SALE':
                        if item['fromTrader']['address'].lower() == self.ad.account.address.lower():
                            return item

            except:
                await asyncio.sleep(i+5)

    async def accept_sell_quote(self, task_data, quote_id):
        for i in range(3):
            try:

                json_data = {
                    'contractAddress': task_data['contractAddress'],
                    'tokens': [
                        {
                            'tokenId': str(task_data['tokenId']),
                        },
                    ],
                    'feeRate': int(task_data['fee']),
                    'quoteId': quote_id,
                }

                res = await self.fetch_url(session=self.session, method='POST', url=f"https://core-api.prod-blast.blur.io/v1/bids/accept", payload=json_data)

                if not res or not res.get('json'):
                    raise Exception

                return res['json']

            except:
                await asyncio.sleep(i+5)

    async def get_sell_quote(self, task_data):
        for i in range(3):
            try:

                json_data = {
                    'tokens': [
                        {
                            'tokenId': str(task_data['tokenId']),
                        },
                    ],
                    'contractAddress': task_data['contractAddress'],
                }

                res = await self.fetch_url(session=self.session, method='POST', url=f"https://core-api.prod-blast.blur.io/v1/bids/quote", payload=json_data)

                if not res or not res.get('json'):
                    raise Exception

                return res['json']

            except:
                await asyncio.sleep(i+5)

    async def get_notifications(self):
        for i in range(3):
            try:
                res = await self.fetch_url(session=self.session, method='GET', url='https://core-api.prod-blast.blur.io/v1/user/notifications/feed')

                if not res or not res.get('json'):
                    raise Exception

                try:
                    notifications = res['json']["notifications"]
                    notifications_ = []
                    for notification in notifications:
                        notification_time = notification["createdAt"][:-1]
                        notification_timestamp = int(datetime.fromisoformat(notification_time).timestamp())
                        if int(time.time()) - notification_timestamp < 60 * 60 * 12 and notification["data"]["makerIsAsk"] == False:
                            notifications_.append(notification)
                    return notifications_
                except:
                    return []
            except:
                await asyncio.sleep(i+1)
        return []

    async def process_notifications(self, notifs):

        for notif in notifs:

            try:

                data = notif['data']

                if not await lister_db.get(f"{data['txHash']}{data['tokenId']}{self.ad.account.address.lower()[-6:-1]}"):

                    collection_data = await parser_db.find_one({'contract': data['contractAddress']})
                    if collection_data:

                        if USE_TG_BOT and ADMIN_ID and TOKEN:
                            image = f"<a href='{data['imageUrl']}'> </a>"
                            text = f"🔈 <b>Bidder {self.ad.account.address[0:5]}...{self.ad.account.address[-5:-1]}</b>:\n" \
                                   f"NFT из коллекции {data['collectionName']} куплена за <code>{data['price']['amount']}</code> ETH{' и создан авто-листер!' if self.task['auto_list'] else ''}\n\n" \
                                   f"📊 Текущий{image}FLOOR: {collection_data['floor']} | BID: {collection_data['best_bid']}"
                            link_blur = f'https://blur.io/blast/asset/{data["contractAddress"]}/{data["tokenId"]}'
                            link_tx = InlineKeyboardButton(text='🟠 Ссылка на NFT (Blur)', url=link_blur)
                            markup = InlineKeyboardMarkup(inline_keyboard=[[link_tx], [close_button]])
                            await bot.send_message(chat_id=ADMIN_ID, text=text, reply_markup=markup, parse_mode='HTML')

                    data.update({"finished": False, 'id': f"{data['txHash']}{data['tokenId']}{self.ad.account.address.lower()[-6:-1]}", 'status': True})
                    await lister_db.insert_or_update(f"{data['txHash']}{data['tokenId']}{self.ad.account.address.lower()[-6:-1]}", data)
                    if self.task['auto_list']:
                        task = asyncio.create_task(self.list_and_monitor(f"{data['txHash']}{data['tokenId']}{self.ad.account.address.lower()[-6:-1]}"))
                        self.listing_tasks.append(task)
            except:
                pass

    async def get_nfts(self):
        for i in range(3):
            try:

                params = {
                    'filters': '{}',
                }

                res = await self.fetch_url(session=self.session, method='GET', url=f'https://core-api.prod-blast.blur.io/v1/portfolio/{self.ad.account.address.lower()}/owned', params=params)

                if not res or not res.get('json'):
                    raise Exception

                collections_in_parser = await parser_db.get_all()
                in_parser_contracts = [col['contract'].lower() for col in collections_in_parser.values()]
                tokens = []

                for token in res['json']['tokens']:
                    try:
                        if token['contractAddress'].lower() in in_parser_contracts:
                            tokens.append(token)
                    except:
                        pass

                return tokens

            except:
                await asyncio.sleep(i+1)
        return []

    async def get_default_acc(self):
        version = str(self.session.impersonate).replace('BrowserType.chrome', '')
        headers = {title: value for title, value in self.session.headers.items()}
        return {
            'address': self.ad.account.address.lower(),
            'key': self.ad.private_key,
            'id': f"{self.ad.account.address.lower()[2:12] + self.ad.account.address.lower()[-10:]}",
            'name': f"{self.ad.account.address.lower()[:5]}...{self.ad.account.address.lower()[-5:]}",
            'auth': self.auth,
            'session_data': {'version': version, 'headers': headers},
            'proxy': None if not self.ad.proxy else self.ad.proxy.as_url,
            'nfts': [],
            'settings_parser': '',
            'settings_bidder': [
                            {"name": "Минимум бидов сверху в % от SUPPLY коллекции", "code": "supply", "type": "float", 'value': 1},
                            #{"name": "Минимальное велью бидов в ETH", "code": "percent", "type": "float", "value": 5},
                            {"name": "Понижение подходящей позиции", "code": "lower", "type": "bool", 'value': False},
                            {"name": "Не ставить на первую позицию", "code": "first", "type": "bool", "value": True},
                            {"name": "Максимум бидов на коллекцию", "code": "maxbd", "type": "int", 'value': 10},
                            {"name": "Минимум кошельков сверху", "code": "minwl", "type": "int", 'value': 1},
                            {"name": "Убрать биды при покупке", "code": "black", "type": "bool", 'value': False}
                        ],
            'bids_data': [],
            'blacklist': [],
            'is_ownlist': False,
            'ownlist': [],
            'auto_list': False,
            'auto_list_percent': 5,
            'auto_list_cooldown': 60,
            'auto_bid_sell': True,
            'auto_bid_refill': True,
            'working': False,
            'status': False,
            'finished': True,
        }

    async def withdraw_if_low_bal(self):
        self.pool_status = True
        try:
            if self.blur_balance > 0.02 and self.balance > 0.00005:
                self.logger.log(f"Не хватает баланса ETH, вывожу...", self.ad.account.address)
                withdraw_blur = await self.onchain.pool(self.ad.w3, self.ad.account, round(random.uniform(0.001, 0.005), 6), direction='out')
                if not withdraw_blur or (type(withdraw_blur) == str and withdraw_blur == 'low_native'):
                    raise Exception
                else:
                    return True
            else:
                raise Exception
        except Exception as e:
            print(f"EXP: {e}")
            self.pool_status = False
            return

    async def refill_after_sale(self):
        if not self.pool_status:
            self.pool_status = True
            try:
                if self.balance > 0.02:
                    #self.logger.log(f"Не хватает баланса ETH, вывожу...", self.ad.account.address)
                    bal_to_dep = round(self.balance - 0.01, 6)
                    dep_blur = await self.onchain.pool(self.ad.w3, self.ad.account, bal_to_dep, direction='in')
                    if not dep_blur or (type(dep_blur) == str and dep_blur == 'low_native'):
                        raise Exception
                    else:
                        return True
                else:
                    raise Exception
            except Exception as e:
                print(f"EXP: {e}")
                self.pool_status = False
                return

    async def get_safe_position(self, bids_blur, min_bidders, min_bids, size, change_pos, first_pos, ex_bids=0):
        min_bids = int(size * min_bids / 100) + ex_bids
        min_bidders = min_bidders + 1
        bidders, bids, price, key = 0, 0, 0, 0

        for key in range(len(bids_blur) - 1):  # ???
            if int(bids_blur[key]["numberBidders"]) + bidders < min_bidders or int(
                    bids_blur[key]["executableSize"]) + bids < min_bids:
                bidders += int(bids_blur[key]["numberBidders"])
                bids += int(bids_blur[key]["executableSize"])
            else:
                price = float(bids_blur[key]["price"])
                if first_pos:
                    if key == 0:
                        try:
                            price = float(bids_blur[key + 1]["price"])
                        except:
                            break
                if change_pos:
                    try:
                        price = float(bids_blur[key + 1]["price"])
                    except:
                        break
                break
        return price, key

    async def main_(self, attempt=0, error='error'):

        if attempt == 10:
            await self._close()
            return error
        if attempt != 0:

            self.logger.log_error(f"Ошибка {error}, пробую снова (попытка {attempt}/10)", wallet=self.ad.account.address)
            await self._sleep(attempt)

        if self.session is None:
            user = await db.find_one({'address': self.ad.account.address.lower()})
            if user:
                session_data = user['session_data']
            else:
                session_data = None
            self.session = await self.get_session(proxy=self.ad.proxy, session_data=session_data)

        if self.auth is None:
            user = await db.find_one({'address': self.ad.account.address.lower()})
            if user:
                self.auth = user['auth']
                self.session.headers.update({"cookie": f"walletAddress={user['address'].lower()}; authToken={user['auth']}"})
            else:
                self.auth = await self.get_auth()

        self.logger.log_success(f"Успешно залогинился, начинаю работу...", wallet=self.ad.account.address)
        acc = await db.get(self.ad.account.address.lower())
        if acc is None:
            await db.insert_or_update(self.ad.account.address.lower(), await self.get_default_acc())

        await db.update_one({'address': self.ad.account.address.lower()}, {"$set": {'working': True, 'status': True, 'finished': False}})

        await lister_db.update_many({'to': self.ad.account.address.lower(), 'status': False}, {"$set": {'finished': True}})
        working_listers = await lister_db.find_many({'to': self.ad.account.address.lower(), 'finished': False, 'status': True})
        for lister in working_listers:
            self.logger.log(f"Включаю листер {lister['collectionName']} #{lister['tokenId']}...", self.ad.account.address)
            task = asyncio.create_task(self.list_and_monitor(f"{lister['txHash']}{lister['tokenId']}{self.ad.account.address.lower()[-6:-1]}"))
            self.listing_tasks.append(task)

        await self.check_and_make_bids()

    async def list_and_monitor(self, task_id):
        while True:
            try:
                task_data = await lister_db.get(task_id)
                if not task_data:
                    break  # TO TASK MEANS DELETED

                if not task_data['status']:
                    self.logger.log(f"Выключил листер {task_data['collectionName']} #{task_data['tokenId']}...", self.ad.account.address)
                    await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True}})
                    break  # USER OFFED (STATUS)

                if task_data['finished']:
                    await lister_db.update_one({'id': task_data['id']}, {"$set": {"status": False}})  # just to make sure
                    break

                if task_data.get('fee', None) is None:
                    fee = await self.get_listing_fee(task_data)
                    await lister_db.update_many({'contractAddress': task_data['contractAddress']}, {"$set": {"fee": fee}})
                    task_data['fee'] = fee

                current_listings = await self.get_listings(task_data)
                if not current_listings:
                    raise Exception('no current_listings')

                user_listing, lowest_listing = None, None
                if current_listings:
                    for idx, listing in enumerate(current_listings):
                        if idx == 0:
                            lowest_listing = listing
                        if int(listing['tokenId']) == int(task_data['tokenId']):
                            user_listing = listing

                if user_listing:
                    if user_listing['tokenId'] == lowest_listing['tokenId']:
                        need_list = False
                    else:
                        need_list = True
                else:
                    need_list = True

                if need_list:
                    new_price = float(float(lowest_listing['price']['amount']) - 0.00000001)
                    if new_price < float(task_data['price']['amount']) / (100/(100-self.task['auto_list_percent'])):
                        if self.task['auto_bid_sell']:
                            sell_quote = await self.get_sell_quote(task_data)
                            if not sell_quote:
                                raise Exception('no sell_quote')

                            accept_quote = await self.accept_sell_quote(task_data, sell_quote['quoteId'])
                            if not accept_quote:
                                raise Exception('no accept_quote')

                            if 'Recently transferred'.lower() in str(accept_quote).lower():
                                raise Exception('recently_transferred')

                            if 'Unexpected owner'.lower() in str(accept_quote).lower():
                                sold_data = await self.get_item_events(task_data)
                                if not sold_data:
                                    text = f"✅ NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> дошла до цены Min Ask и была продана в бид, но не смог узнать цену продажи!"
                                    text_log = f"✅ NFT {task_data['collectionName']} #{task_data['tokenId']} дошла до цены Min Ask и была продана в бид, но не смог узнать цену продажи!"
                                else:
                                    value_lost = round(float(sold_data['price']['amount']) - float(task_data['price']['amount']), 6)
                                    text = f"✅ NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> дошла до цены Min Ask и была продана в бид за <code>{sold_data['price']['amount']}</code> ETH\n\n📊 Результат: <code>{'' if value_lost < 0 else '+'}{value_lost}</code> ETH"
                                    text_log = f"✅ NFT {task_data['collectionName']} #{task_data['tokenId']} дошла до цены Min Ask и была продана в бид за {sold_data['price']['amount']} ETH 📊 Результат: {'' if value_lost < 0 else '+'}{value_lost} ETH"
                                if USE_TG_BOT and ADMIN_ID and TOKEN:
                                    await bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode='HTML', reply_markup=close_markup)
                                self.logger.log_success(text_log, self.ad.account.address)
                                await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True, "status": False}})
                                break

                            if accept_quote['approvals']:
                                approve_data = accept_quote['approvals'][0].get('txnData', accept_quote['approvals'][0].get('transactionRequest'))

                                tx = await self.onchain.make_tx(self.ad.w3, self.ad.account, value=0, to=approve_data['to'], data=approve_data['data'])
                                if tx == "low_native":
                                    if self.pool_status:
                                        continue
                                    else:
                                        pool_withdraw = await self.withdraw_if_low_bal()
                                        if pool_withdraw:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера <b>{task_data['collectionName']} #{task_data['tokenId']}</b> на сумму <code>0.01</code> ETH!", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_success(f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера {task_data['collectionName']} #{task_data['tokenId']} на сумму 0.01 ETH!", self.ad.account.address)
                                            continue
                                        else:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ Листер для NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> выключен, т.к. не хватает средств для аппрува", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_error(f"⚠️ Листер для NFT {task_data['collectionName']} #{task_data['tokenId']} выключен, т.к. не хватает средств для аппрува", self.ad.account.address)
                                            await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True, "status": False}})
                                            break
                                if not tx:
                                    raise Exception('no tx (sell approvals)')

                                hash, _ = await self.onchain.send_tx(self.ad.w3, self.ad.account, tx)
                                if type(hash == str) and hash == 'low_native':
                                    if self.pool_status:
                                        continue
                                    else:
                                        pool_withdraw = await self.withdraw_if_low_bal()
                                        if pool_withdraw:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера <b>{task_data['collectionName']} #{task_data['tokenId']}</b> на сумму <code>0.01</code> ETH!", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_success(f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера {task_data['collectionName']} #{task_data['tokenId']} на сумму 0.01 ETH!", self.ad.account.address)
                                            continue
                                        else:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ Листер для NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> выключен, т.к. не хватает средств для аппрува", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_error(f"⚠️ Листер для NFT {task_data['collectionName']} #{task_data['tokenId']} выключен, т.к. не хватает средств для аппрува", self.ad.account.address)
                                            await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True, "status": False}})
                                            break
                                elif not hash:
                                    raise Exception('no hash (sell approvals)')

                                tx_status = await self.onchain.check_for_status(self.ad.w3, hash)
                                if not tx_status:
                                    raise Exception('no tx_status (sell approvals)')

                            if accept_quote['accepts']:
                                await asyncio.sleep(5)
                                accept_data = accept_quote['accepts'][0].get('txnData', accept_quote['accepts'][0].get('transactionRequest'))

                                tx = await self.onchain.make_tx(self.ad.w3, self.ad.account, value=0, to=accept_data['to'], data=accept_data['data'])
                                if tx == "low_native":
                                    if self.pool_status:
                                        continue
                                    else:
                                        pool_withdraw = await self.withdraw_if_low_bal()
                                        if pool_withdraw:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера <b>{task_data['collectionName']} #{task_data['tokenId']}</b> на сумму <code>0.01</code> ETH!", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_success(f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера {task_data['collectionName']} #{task_data['tokenId']} на сумму 0.01 ETH!", self.ad.account.address)
                                            continue
                                        else:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ Листер для NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> выключен, т.к. не хватает средств для продажи NFT в бид", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_error(f"⚠️ Листер для NFT {task_data['collectionName']} #{task_data['tokenId']} выключен, т.к. не хватает средств для продажи NFT в бид", self.ad.account.address)
                                            await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True, "status": False}})
                                            break
                                if not tx:
                                    raise Exception('no tx (sell accepts)')

                                hash, _ = await self.onchain.send_tx(self.ad.w3, self.ad.account, tx)
                                if type(hash == str and hash == 'low_native'):
                                    if self.pool_status:
                                        continue
                                    else:
                                        pool_withdraw = await self.withdraw_if_low_bal()
                                        if pool_withdraw:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера <b>{task_data['collectionName']} #{task_data['tokenId']}</b> на сумму <code>0.01</code> ETH!", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_success(f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера {task_data['collectionName']} #{task_data['tokenId']} на сумму 0.01 ETH!", self.ad.account.address)
                                            continue
                                        else:
                                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                                await bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ Листер для NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> выключен, т.к. не хватает средств для продажи NFT в бид", parse_mode='HTML', reply_markup=close_markup)
                                            self.logger.log_error(f"⚠️ Листер для NFT {task_data['collectionName']} #{task_data['tokenId']} выключен, т.к. не хватает средств для продажи NFT в бид", self.ad.account.address)
                                            await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True, "status": False}})
                                            break
                                elif not hash:
                                    raise Exception('no hash (sell accepts)')

                                tx_status = await self.onchain.check_for_status(self.ad.w3, hash)
                                if not tx_status:
                                    raise Exception('no tx_status (sell accepts)')

                            if not accept_quote['accepts']:
                                raise Exception('no accept_quote (sell accepts)')

                            value_lost = round(float(sell_quote['nfts'][0]['price']['amount']) - float(task_data['price']['amount']), 6)
                            text = f"✅ NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> дошла до цены Min Ask и была продана в бид за <code>{sell_quote['nfts'][0]['price']['amount']}</code> ETH\n\n📊 Результат: <code>{'' if value_lost < 0 else '+'}{value_lost}</code> ETH"
                            text_log = f"✅ NFT {task_data['collectionName']} #{task_data['tokenId']} дошла до цены Min Ask и была продана в бид за {sell_quote['nfts'][0]['price']['amount']} ETH 📊 Результат: {'' if value_lost < 0 else '+'}{value_lost} ETH"
                            self.logger.log_success(text_log, self.ad.account.address)
                        else:
                            text = f"⚠️ NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> дошла до цены Min Ask и пока не продана, завершил работу листера!"
                            text_log = f"⚠️ NFT {task_data['collectionName']} #{task_data['tokenId']} дошла до цены Min Ask и пока не продана, завершил работу листера!"
                            self.logger.log_warning(text_log, self.ad.account.address)

                        if USE_TG_BOT and ADMIN_ID and TOKEN:
                            await bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode='HTML', reply_markup=close_markup)
                        await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True, "status": False}})
                        break

                    format_listing = await self.get_listing_format(task_data, new_price, task_data['fee'])
                    if format_listing:

                        if 'You need to own this'.lower() in format_listing.get('message', 'none').lower():
                            sold_data = await self.get_item_events(task_data)
                            if not sold_data:
                                text = f"✅ NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> успешно продана, но не смог узнать цену продажи!"
                                text_log = f"✅ NFT {task_data['collectionName']} #{task_data['tokenId']} успешно продана, но не смог узнать цену продажи!"
                            else:
                                value_lost = round(float(sold_data['price']['amount']) - float(task_data['price']['amount']), 6)
                                text = f"✅ NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> успешно продана за <code>{round(float(sold_data['price']['amount']), 6)}</code> ETH!\n\n📊 Результат: <code>{'' if value_lost < 0 else '+'}{value_lost}</code> ETH"
                                text_log = f"✅ NFT {task_data['collectionName']} #{task_data['tokenId']} успешно продана за {round(float(sold_data['price']['amount']), 6)} ETH! 📊 Результат: {'' if value_lost < 0 else '+'}{value_lost} ETH"
                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                await bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode='HTML', reply_markup=close_markup)
                            self.logger.log_success(text_log, self.ad.account.address)
                            await lister_db.update_one({'id': task_data['id']}, {"$set": {"finished": True, "status": False}})
                            break

                        if format_listing['approvals']:
                            approve_data = format_listing['approvals'][0]['transactionRequest']

                            tx = await self.onchain.make_tx(self.ad.w3, self.ad.account, value=0, to=approve_data['to'], data=approve_data['data'])
                            if tx == "low_native":
                                if self.pool_status:
                                    continue
                                else:
                                    pool_withdraw = await self.withdraw_if_low_bal()
                                    if pool_withdraw:
                                        if USE_TG_BOT and ADMIN_ID and TOKEN:
                                            await bot.send_message(chat_id=ADMIN_ID,text=f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера <b>{task_data['collectionName']} #{task_data['tokenId']}</b> на сумму <code>0.01</code> ETH!",parse_mode='HTML', reply_markup=close_markup)
                                        self.logger.log_success(f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера {task_data['collectionName']} #{task_data['tokenId']} на сумму 0.01 ETH!",self.ad.account.address)
                                        continue
                                    else:
                                        if USE_TG_BOT and ADMIN_ID and TOKEN:
                                            await bot.send_message(chat_id=ADMIN_ID,text=f"⚠️ Листер для NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> выключен, т.к. не хватает средств для продажи NFT в бид",parse_mode='HTML', reply_markup=close_markup)
                                        self.logger.log_error(f"⚠️ Листер для NFT {task_data['collectionName']} #{task_data['tokenId']} выключен, т.к. не хватает средств для продажи NFT в бид",self.ad.account.address)
                                        await lister_db.update_one({'id': task_data['id']},{"$set": {"finished": True, "status": False}})
                                        break
                            if not tx:
                                raise Exception('no tx (list approvals)')

                            hash, _ = await self.onchain.send_tx(self.ad.w3, self.ad.account, tx)
                            if type(hash == str) and hash == 'low_native':
                                if self.pool_status:
                                    continue
                                else:
                                    pool_withdraw = await self.withdraw_if_low_bal()
                                    if pool_withdraw:
                                        if USE_TG_BOT and ADMIN_ID and TOKEN:
                                            await bot.send_message(chat_id=ADMIN_ID,text=f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера <b>{task_data['collectionName']} #{task_data['tokenId']}</b> на сумму <code>0.01</code> ETH!",parse_mode='HTML', reply_markup=close_markup)
                                        self.logger.log_success(f"✅ Успешно пополнил баланс ETH (выводом из Blur Pool) для листера {task_data['collectionName']} #{task_data['tokenId']} на сумму 0.01 ETH!",self.ad.account.address)
                                        continue
                                    else:
                                        if USE_TG_BOT and ADMIN_ID and TOKEN:
                                            await bot.send_message(chat_id=ADMIN_ID,text=f"⚠️ Листер для NFT <b>{task_data['collectionName']} #{task_data['tokenId']}</b> выключен, т.к. не хватает средств для продажи NFT в бид",parse_mode='HTML', reply_markup=close_markup)
                                        self.logger.log_error(f"⚠️ Листер для NFT {task_data['collectionName']} #{task_data['tokenId']} выключен, т.к. не хватает средств для продажи NFT в бид",self.ad.account.address)
                                        await lister_db.update_one({'id': task_data['id']},{"$set": {"finished": True, "status": False}})
                                        break
                            elif not hash:
                                raise Exception('no hash (list approvals)')

                            tx_status = await self.onchain.check_for_status(self.ad.w3, hash)
                            if not tx_status:
                                raise Exception('no tx_status (list approvals)')

                        if format_listing['signatures']:
                            sign_data = await self.construsct_sign_data(format_listing['signatures'])
                            if not sign_data:
                                raise Exception('no sign_data (list signatures)')
                            signature = self.ad.account.sign_message(encode_typed_data(full_message=sign_data))

                            submit_listing = await self.submit_listing(signature.signature.hex(), format_listing['signatures'][0]['marketplaceData'])
                            if not submit_listing:
                                raise Exception('no submit_listing (list signatures)')

                    else:
                        raise Exception('no format_listing')

                await asyncio.sleep(random.uniform(self.task['auto_list_cooldown']*0.9, self.task['auto_list_cooldown']*1.1))
            except Exception as e:
                print(f"LISTER EXP: {e}")
                await asyncio.sleep(random.uniform(25, 35))
                continue

        await self.refill_after_sale()
        
        return

    async def check_and_make_bids(self):
        while True:

            try:

                cancel_bids = []
                make_bids = []

                task = await db.get(self.ad.account.address.lower())
                self.task = task

                pending_listers = await lister_db.find_many({'to': self.ad.account.address.lower(), 'finished': True, 'status': True})
                for lister in pending_listers:
                    self.logger.log(f"Включаю листер {lister['collectionName']} #{lister['tokenId']}...", self.ad.account.address)
                    await lister_db.update_one({'id': lister['id']}, {"$set": {"finished": False}})
                    lister_task = asyncio.create_task(self.list_and_monitor(f"{lister['txHash']}{lister['tokenId']}{self.ad.account.address.lower()[-6:-1]}"))
                    self.listing_tasks.append(lister_task)

                self.listing_tasks = [task for task in self.listing_tasks if not task.done()]

                if not task['status']:
                    if task['working']:
                        await db.update_one({"address": self.ad.account.address.lower()}, {"$set": {"working": False}})
                        task['working'] = False
                    else:
                        await asyncio.sleep(random.uniform(15, 30))
                        continue
                else:
                    await db.update_one({"address": self.ad.account.address.lower()}, {"$set": {"working": True}})

                min_bids, change_pos, first_pos, max_bids, min_bidders, bought_black = [settings['value'] for settings in task['settings_bidder']]

                blacklist = task["blacklist"]  # БЛЕКЛИСТ
                is_ownlist = task["is_ownlist"]
                ownlist = task["ownlist"]

                actual_bids = await self.get_bids()
                nfts = await self.get_nfts()
                actual_bids_ = [{"contract": actual["contractAddress"], "price": float(actual["price"]), "quantity": int(actual["openSize"])} for actual in actual_bids]
                await db.update_one({"address": self.ad.account.address.lower()}, {"$set": {"bids_data": actual_bids_, "nfts": nfts}})

                if task['working']:

                    await self.get_balance()

                    if self.blur_balance < 0.01:
                        if self.balance < 0.0151:
                            if USE_TG_BOT and ADMIN_ID and TOKEN:
                                await bot.send_message(chat_id=ADMIN_ID, text=f'⚠️ Биддер <b>{task["name"]}</b> выключен, т.к. недостаточно баланса!', parse_mode='HTML', reply_markup=close_markup)
                            self.logger.log_error(f'⚠️ Биддер {task["name"]} выключен, т.к. недостаточно баланса!', self.ad.account.address)
                            await db.update_one({"address": self.ad.account.address.lower()}, {"$set": {"working": False, 'status': False}})
                            continue
                        else:
                            self.logger.log(f"Не хватает баланса BETH, пополняю...", self.ad.account.address)
                            deposit_blur = await self.onchain.pool(self.ad.w3, self.ad.account, round(self.balance-0.0015, 6), direction='in')
                            if not deposit_blur or (type(deposit_blur) == str and deposit_blur == 'low_native'):
                                if USE_TG_BOT and ADMIN_ID and TOKEN:
                                    await bot.send_message(chat_id=ADMIN_ID, text=f'⚠️ Биддер <b>{task["name"]}</b> выключен, т.к. недостаточно баланса!', parse_mode='HTML', reply_markup=close_markup)
                                self.logger.log_error(f'⚠️ Биддер {task["name"]} выключен, т.к. недостаточно баланса!', self.ad.account.address)
                                await db.update_one({"address": self.ad.account.address.lower()}, {"$set": {"working": False, 'status': False}})
                                continue
                            else:
                                if USE_TG_BOT and ADMIN_ID and TOKEN:
                                    await bot.send_message(chat_id=ADMIN_ID, text=f'✅ Успешно пополнил баланс BETH (Blur Pool) для биддера <b>{task["name"]}</b> на сумму <code>{round(self.balance-0.0015, 6)}</code> ETH!', parse_mode='HTML', reply_markup=close_markup)
                                self.logger.log_success(f'✅ Успешно пополнил баланс BETH (Blur Pool) для биддера {task["name"]} на сумму {round(self.balance-0.0015, 6)} ETH!', self.ad.account.address)
                                await self.get_balance()

                    elig_cols = await parser_db.find_many({'updated': {"$gte": int(time.time() - 60*15)}})

                    # Проверяю старые биды
                    if len(actual_bids) != 0:
                        for bid in actual_bids:
                            try:
                                col = await parser_db.find_one({'contract': bid['contractAddress']})

                                if not col:
                                    cancel_bids.append({"contract": bid["contractAddress"], "price": float(bid["price"]), "quantity": bid['openSize']})
                                    continue

                                if col['volume_day'] <= 0.01:  # VERY LOW VOLUME
                                    cancel_bids.append({"contract": bid["contractAddress"], "price": float(bid["price"]),"quantity": bid['openSize']})
                                    continue

                                if not col['bids']:
                                    cancel_bids.append({"contract": bid["contractAddress"], "price": float(bid["price"]),"quantity": bid['openSize']})
                                    continue

                                if is_ownlist:
                                    if col['slug'] not in ownlist:
                                        cancel_bids.append({"contract": bid["contractAddress"], "price": float(bid["price"]),"quantity": bid['openSize']})
                                        continue

                                if col['slug'] in blacklist:
                                    cancel_bids.append({"contract": bid["contractAddress"], "price": float(bid["price"]),"quantity": bid['openSize']})
                                    continue


                                size = col['supply']
                                price, key = await self.get_safe_position(col["bids"], min_bidders, min_bids, size, change_pos, first_pos, ex_bids=bid['openSize'])

                                if price == 0:
                                    cancel_bids.append({"contract": bid["contractAddress"], "price": float(bid["price"]), "quantity": bid['openSize']})
                                    continue

                                elif float(bid["price"]) == price:
                                    if price > self.blur_balance:
                                        price = math.floor(self.blur_balance * 100) / 100
                                        if float(bid["price"]) == price:
                                            continue

                                        amount = 1
                                        cancel_bids.append({"contract": bid["contractAddress"], "price": float(bid["price"]), "quantity": bid['openSize']})

                                        contract_exists = False  #
                                        for existing_bid in make_bids:
                                            if existing_bid["contract"] == bid["contractAddress"]:
                                                contract_exists = True
                                                break
                                        if not contract_exists:
                                            if float(bid["price"]) != price:
                                                make_bids.append(
                                                    {"slug": col['slug'], "contract": bid["contractAddress"], "price": price, "quantity": amount})

                                        continue
                                else:
                                    if price > self.blur_balance:
                                        price = math.floor(self.blur_balance * 100) / 100
                                        if float(bid["price"]) == price:
                                            continue

                                        amount = 1
                                        cancel_bids.append({"contract": bid["contractAddress"], "price": bid["price"],"quantity": bid['openSize']})

                                        contract_exists = False  # MAKE NEW
                                        for existing_bid in make_bids:
                                            if existing_bid["contract"] == bid["contractAddress"]:
                                                contract_exists = True
                                                break
                                        if not contract_exists:
                                            if float(bid["price"]) != price:
                                                make_bids.append({"slug": col['slug'], "contract": bid["contractAddress"], "price": price,"quantity": amount})
                                        continue
                                    else:
                                        amount = int(self.blur_balance / price)
                                        amount = min(amount, max_bids)
                                        if amount >= 100:
                                            amount = 99

                                        cancel_bids.append({"contract": bid["contractAddress"], "price": bid["price"],"quantity": bid['openSize']})

                                        contract_exists = False  # MAKE NEW
                                        for existing_bid in make_bids:
                                            if existing_bid["contract"] == bid["contractAddress"]:
                                                contract_exists = True
                                                break
                                        if not contract_exists:
                                            make_bids.append({"slug": col['slug'], "contract": bid["contractAddress"], "price": price, "quantity": amount})
                                        continue
                            except Exception as e:
                                print(f"OLD BIDS EXP: {e}")
                                pass

                    # Проверяю коллекции
                    if len(elig_cols) != 0:
                        for collection in elig_cols:
                            try:
                                size = collection['supply']

                                if not collection['bids']: #or not collection['bid_rewards']:
                                    continue  # COL DOESNT HAVE BIDS

                                if collection['volume_day'] <= 0.01:
                                    continue  # VERY LOW VOLUME

                                if is_ownlist:
                                    if collection['slug'] not in ownlist:
                                        continue

                                if collection['slug'] in blacklist:
                                    continue

                                price, key = await self.get_safe_position(collection['bids'], min_bidders, min_bids, size, change_pos, first_pos)
                                if price == 0:
                                    continue
                                else:
                                    if price > self.blur_balance:
                                        price = math.floor(self.blur_balance * 100) / 100
                                        amount = 1
                                    else:
                                        amount = int(self.blur_balance / price)
                                        amount = min(amount, max_bids)
                                        if amount >= 100:
                                            amount = 99

                                    contract_exists = False
                                    for existing_bid in make_bids:
                                        if existing_bid["contract"] == collection["contract"]:
                                            contract_exists = True
                                            break
                                    if not contract_exists:
                                        already_made = False
                                        if len(actual_bids_) != 0:
                                            for bid in actual_bids_:
                                                if bid["contract"] == collection["contract"]:
                                                    already_made = True
                                                    break
                                        if not already_made:
                                            make_bids.append({"slug": collection['slug'], "contract": collection["contract"], "price": price, "quantity": amount})
                            except Exception as e:
                                print(f"NEW BIDS EXP: {e}")
                                pass

                    else:
                        pass  # print parser wrong

                else:
                    self.logger.log(f"Завершаю работу и отменяю все биды...", self.ad.account.address)
                    task = await db.get(self.ad.account.address.lower())
                    for bid in task["bids_data"]:
                        cancel_bids.append({"contract": bid["contract"], "price": bid["price"], "quantity": bid["quantity"]})
                    await db.update_one({"address": self.ad.account.address.lower()}, {"$set": {"bids_data": []}})

                await self.process_cancel(cancel_bids)

                self.logger.log(f"Проверяю уведомления...", self.ad.account.address)
                notifs = await self.get_notifications()
                await self.process_notifications(notifs)
                self.logger.log_success(f"Проверил уведомления!", self.ad.account.address)

                if not task['working']:
                    self.logger.log_success(f"Завершил работу, жду следующего включения!", self.ad.account.address)
                    continue

                await self.process_bids(make_bids)

                await asyncio.sleep(random.uniform(10, 20))

            except Exception as error:
                self.logger.log_error(f"Критическая ошибка при работе биддера: {error}", self.ad.account.address)
                await asyncio.sleep(random.uniform(10, 20))

    async def process_cancel(self, cancel_bids):
        self.logger.log(f"Отменяю {len(cancel_bids)} бидов", self.ad.account.address)

        async def sem_task(cancel):
            async with self.semaphore:
                return await self.cancel_bid(cancel["price"], cancel["contract"])

        tasks = [sem_task(cancel) for cancel in cancel_bids]
        results_cancel = await asyncio.gather(*tasks)
        count_cancelled = 0
        for cont, result in results_cancel:
            if result == True:
                #await db.update_one({'address': self.ad.account.address.lower()}, {"$pull": {'bids_data': {"contract": cont}}})
                count_cancelled += 1
        self.logger.log_success(f"Отменено {count_cancelled}/{len(cancel_bids)} бидов", self.ad.account.address)

    async def process_bids(self, make_bids):
        self.logger.log(f"Делаю {len(make_bids)} бидов", self.ad.account.address)

        async def sem_task(bid):
            async with self.semaphore:
                return await self.make_bid(bid["price"], bid["quantity"], bid["contract"])

        tasks = [sem_task(bid) for bid in make_bids]
        results_bid = await asyncio.gather(*tasks)

        count_made = 0
        for result in results_bid:
            if result:
                # new_bid = {"contract": contract, "price": price, "quantity": quantity}
                # await self.db.update_one(
                #     {'address': self.ad.account.address.lower()},
                #     {"$push": {"bids_data": new_bid}}
                # )
                count_made += 1

        self.logger.log_success(f"Сделано {count_made}/{len(make_bids)} бидов", self.ad.account.address)

    async def _sleep(self, value):
        self.logger.log(f"⏳ Сплю {round(value, 2)} секунд перед следующим действием...", wallet=self.ad.account.address)
        await asyncio.sleep(value)

    async def _close(self):
        try:
            await self.close_session(self.session)
        except:
            pass
