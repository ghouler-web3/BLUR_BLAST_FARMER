import asyncio
import secrets
import math
import re

from aiogram import Router
from aiogram import F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from helpers.database import db, lister_db, check_db, parser_db
from helpers.fetcher import AsyncFetcher
from helpers.utils import get_normal_web3, get_proxy_web3

from core.blur_onchain import Onchain

router = Router()
fetcher = AsyncFetcher()
onchain = Onchain()

close_button = InlineKeyboardButton(text='❌ Закрыть', callback_data='close')
close_markup = InlineKeyboardMarkup(inline_keyboard=[[close_button]])

class Blur(StatesGroup):
    setting = State()
    name = State()
    list = State()
    auto = State()
    imp = State()

async def check_sub(callback):
    try:
        markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏳ Проверить подписку", callback_data='blur_page:0')]])
        text = "[❕]  Для того, чтобы продолжить использовать TG-интерфейс бота, пожалуйста, подпишись на канал автора: @ghouler_web3\n\nПосле подписки нажми на кнопку '⏳ Проверить подписку' внизу 👇"
        await callback.message.edit_text(text=text, reply_markup=markup, parse_mode='HTML')
        await check_db.insert_or_update('check', True)
    except:
        pass

@router.message(F.text.startswith('/start'))
async def start(message: Message):
    await message.delete()
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🟠 Blur', callback_data='blur_page:0')]])
    await message.answer(f"📁 <b>Команды:</b>\n\n"
                         f"<code>/run 0x123...321</code> - <i> запуск работы кошелька</i>\n"
                         f"<code>/stop 0x123...321</code> - <i> остановка работы кошелька</i>\n"
                         f"<code>/show 0x123...321</code> - <i> информация о кошельке</i>\n"
                         f"<code>/info</code> - <i> информация о статусе кошельков</i>\n"
                         f"<code>/finish_all</code> - <i> остановка работы всех кошельков</i>\n\n"
                         f"[❕]  <i>Для открытия главного меню нажми на кнопку ниже</i> 👇", parse_mode='HTML',
                         reply_markup=markup)

@router.message(F.text.startswith('/run'))
async def run(message: Message):
    await message.delete()
    try:
        _, wallet = message.text.split()
        wallet_data = await db.get(wallet.lower())
        if wallet_data:
            if not wallet_data['working']:
                await db.update_one({'address': wallet.lower()}, {"$set": {'status': True}})
                await message.answer(f"✅ Успешно начал работу для кошелька <code>{wallet}</code>!", parse_mode='HTML', reply_markup=close_markup)
            else:
                await message.answer(f"⚠️ Кошелёк <code>{wallet}</code> уже включен!", parse_mode='HTML', reply_markup=close_markup)
        else:
            await message.answer(f"⚠️ <b>Неверно указан кошелёк!</b>\n\nПример: <code>/run 0x123...321</code>", parse_mode='HTML', reply_markup=close_markup)
    except:
        await message.answer(f"⚠️ <b>Неверно написана команда!</b>\n\nПример: <code>/run 0x123...321</code>", parse_mode='HTML', reply_markup=close_markup)

@router.message(F.text.startswith('/stop'))
async def stop(message: Message):
    await message.delete()
    try:
        _, wallet = message.text.split()
        wallet_data = await db.get(wallet.lower())
        if wallet_data:
            if wallet_data['working']:
                await db.update_key(wallet, 'status', False)
                await message.answer(f"✅ Успешно завершил работу для кошелька <code>{wallet}</code>!",  parse_mode='HTML', reply_markup=close_markup)
            else:
                await message.answer(f"⚠️ Кошелёк <code>{wallet}</code> уже выключен!", parse_mode='HTML', reply_markup=close_markup)
        else:
            await message.answer(f"⚠️ <b>Неверно указан кошелёк!</b>\n\nПример: <code>/stop 0x123...321</code>", parse_mode='HTML', reply_markup=close_markup)
    except:
        await message.answer(f"⚠️ <b>Неверно написана команда!</b>\n\nПример: <code>/stop 0x123...321</code>", parse_mode='HTML', reply_markup=close_markup)

@router.message(F.text.startswith('/show'))
async def show(message: Message):
    await message.delete()
    try:
        _, wallet = message.text.split()
        wallet_data = await db.get(wallet.lower())
        if wallet_data:
            markup, text = await create_display_task(wallet_data['id'])
            await message.answer(text=text, reply_markup=markup, parse_mode='HTML')
        else:
            await message.answer(f"⚠️ <b>Неверно указан кошелёк!</b>\n\nПример: <code>/show 0x123...321</code>", parse_mode='HTML', reply_markup=close_markup)
    except:
        await message.answer(f"⚠️ <b>Неверно написана команда!</b>\n\nПример: <code>/show 0x123...321</code>", parse_mode='HTML', reply_markup=close_markup)

@router.message(F.text.startswith('/info'))
async def info(message: Message):
    await message.delete()
    try:
        wallets = db.cache
        if len(list(wallets.keys())) > 0:
            text = f"📑<b>Информация о статусе кошельков:</b>\n\n"
            for idx, (wallet, wallet_data) in enumerate(list(wallets.items())):
                if wallet_data.get('status', None) is not None and not wallet_data['finished']:
                    text += f"<b>{idx + 1}.</b> <code>{wallet}</code> - {'🟢' if wallet_data['working'] else '🔴'}\n"
            await message.answer(text, parse_mode='HTML')
        else:
            await message.answer(f"⚠️ <b>Нет данных о кошельках!</b>", parse_mode='HTML', reply_markup=close_markup)
    except:
        await message.answer(f"⚠️ <b>Неверно написана команда!</b>\n\nПример: <code>/info</code>", parse_mode='HTML', reply_markup=close_markup)

@router.message(F.text.startswith('/finish_all'))
async def stop_all(message: Message):
    await message.delete()
    try:
        wallets = db.cache
        for wallet, _ in wallets.items():
            try:
                await db.update_key(wallet, 'status', False)
            except:
                pass
        await message.answer(f"✅ Успешно завершил работу всех кошельков!", parse_mode='HTML', reply_markup=close_markup)
    except:
        await message.answer(f"⚠️ <b>Неверно написана команда!</b>\n\nПример: <code>/finish_all</code>", parse_mode='HTML', reply_markup=close_markup)

@router.callback_query(F.data == 'close')
async def close(callback: CallbackQuery):
    await callback.answer('Закрыто!')
    await callback.message.delete()

@router.callback_query(F.data.startswith('blur'))
async def blur(callback: CallbackQuery, state: FSMContext):
    #print(callback.data)
    await state.clear()

    if not await check_db.get_all():
        await callback.answer("Ты ещё не подписался на канал!")
        await check_sub(callback)

    else:
        try:
            if callback.data.startswith('blur_page:'):
                page = int(callback.data.split(':')[1])
                bidders_data = await db.get_all()
                bidders_data = {wallet:data for wallet, data in bidders_data.items() if not data['finished']}
                text = f'🔨 <b>Blur Bidder</b>\n\nТекущее кол-во биддеров: {len(bidders_data)}'
                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=await create_bidders_keyboard(bidders_data, page=page))

            if callback.data.startswith('blur_bd:'):

                if '-' not in callback.data:
                    _, id_, op = callback.data.split(':')
                    task = await db.find_one({"id": id_})

                    if op == 'id':
                        markup, text = await create_display_task(id_)
                        await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                    if op in ['off', 'on']:
                        await db.update_one({'id': id_}, {"$set": {"status": True if op == 'on' else False}})
                        markup, text = await create_display_task(id_)
                        await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                    if op.startswith('stg'):
                        _, subop = op.split('/')

                        if subop == 'stg':
                            markup, text = await create_display_settings(id_, main=True)
                            await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                        if subop == 'bd':
                            markup, text = await create_display_settings(id_, bidder=True)
                            await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                        if subop == 'name':
                            back_settings = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{task['id']}:stg/stg")]])
                            await callback.message.edit_text(text=f"[❕] Отправь новое имя для биддера ниже 👇",reply_markup=back_settings)
                            await state.set_state(Blur.name)
                            await state.update_data(m=back_settings, id=id_, callback=callback)
                        if subop.startswith('bd|'):
                            _, code = subop.split("|")
                            value = next((setting for setting in task['settings_bidder'] if setting['code'] == code), None)
                            if value["type"] == "bool":
                                for setting in task['settings_bidder']:
                                    if setting['code'] == code:
                                        setting['value'] = not setting['value']
                                        break
                                await db.update_one({'id': id_}, {"$set": {"settings_bidder": task['settings_bidder']}})
                                markup, text = await create_display_settings(id_, bidder=True)
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                            else:
                                back_settings = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{task['id']}:stg/bd")]])
                                await callback.message.edit_text(text=f"[❕] Отправь новое значение для настройки '{value['name']}' ниже 👇",reply_markup=back_settings)
                                await state.set_state(Blur.setting)
                                await state.update_data(m=back_settings, setting_type=value['type'], setting_name=value['name'], setting_code=value['code'], id=id_, callback=callback)

                        if subop == 'imp':
                            back_settings = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{task['id']}:stg/stg")]])
                            await callback.message.edit_text(text=f"[❕] Отправь ID экспорта настроек ниже 👇", reply_markup=back_settings)
                            await state.set_state(Blur.imp)
                            await state.update_data(m=back_settings, id=id_, callback=callback, list=False)

                        if subop == 'main':
                            markup, text = await create_display_settings(id_, settingsmain=True)
                            await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                        if subop in ['auto|on', 'auto|off']:
                            await db.update_one({'id': id_}, {"$set": {"auto_list": True if 'on' in subop else False}})
                            markup, text = await create_display_settings(id_, settingsmain=True)
                            await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                        if subop in ['own|on', 'own|off']:
                            await db.update_one({'id': id_}, {"$set": {"is_ownlist": True if 'on' in subop else False}})
                            markup, text = await create_display_settings(id_, settingsmain=True)
                            await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                        if subop.startswith('own|') or subop.startswith('blk|'):
                            list, type = subop.split("|")
                            if type == 'main':
                                markup, text = await create_display_settings(id_, settingsmain=list)
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                            if type == 'add' or type == 'del':
                                back_settings = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{task['id']}:stg/{list}|main")]])
                                await callback.message.edit_text(text=f"[❕] Отправь slug(и) коллекций, которые хочешь {'добавить' if type == 'add' else 'удалить'} в {'📓 BlackList' if list == 'blk' else '📗 OwnList'} ниже 👇", reply_markup=back_settings)
                                await state.set_state(Blur.list)
                                await state.update_data(m=back_settings, id=id_, callback=callback, list=list, mode=type, list_name='blacklist' if list == 'blk' else 'ownlist')
                            if type == 'dall':
                                confirm_markup = InlineKeyboardBuilder().row(*[InlineKeyboardButton(text='✅ Подтвердить', callback_data=f"blur_bd:{task['id']}:stg/blk|dallc"), InlineKeyboardButton(text='❌ Отменить', callback_data=f"blur_bd:{id_}:stg/blk|main")])
                                await callback.message.edit_text(text=f"[❔] Ты действительно хочешь очистить весь {'📓 BlackList' if list == 'blk' else '📗 OwnList'}?", reply_markup=confirm_markup.as_markup())
                            if type == 'dallc':
                                await db.update_one({"id": id_}, {"$set": {f"{'blacklist' if list == 'blk' else 'ownlist'}": []}})
                                markup, text = await create_display_settings(id_, settingsmain=list)
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                            if type == 'imp':
                                back_settings = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton( text='⬅️ Назад', callback_data=f"blur_bd:{task['id']}:stg/{list}|main")]])
                                await callback.message.edit_text(text=f"[❕] Отправь ID экспорта {'📓 BlackList' if list == 'blk' else '📗 OwnList'} ниже 👇", reply_markup=back_settings)
                                await state.set_state(Blur.imp)
                                await state.update_data(m=back_settings, id=id_, callback=callback, list=list, type='blacklist' if list == 'blk' else 'ownlist')
                            if type.startswith('page'):
                                page_num = int(type[4:])
                                markup, text = await create_display_settings(id_, settingsmain=list, page=page_num)
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                        if subop.startswith('auto|'):
                            _, type = subop.split('|')
                            if type == 'main':
                                markup, text = await create_display_autolist(id_)
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                            if type in ['cool', 'ask']:
                                back_settings = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{task['id']}:stg/auto|main")]])
                                setting_name = 'Auto-List Cooldown (в секундах)' if type == 'cool' else 'Auto-List Min Ask'
                                setting = 'auto_list_cooldown' if type == 'cool' else 'auto_list_percent'
                                await callback.message.edit_text(text=f"[❕] Отправь новое значение для настройки '{setting_name}' ниже 👇", reply_markup=back_settings)
                                await state.set_state(Blur.auto)
                                await state.update_data(m=back_settings, id=id_, callback=callback, setting_name=setting_name, setting=setting)
                            if type.startswith('bid'):
                                await db.update_one({'id': id_}, {"$set": {"auto_bid_sell": True if 'on' in subop else False}})
                                markup, text = await create_display_autolist(id_)
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

                    if op.startswith('nfts'):

                        if op == 'nfts':
                            markup, text = await create_display_nfts(id_, main=True)
                            await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                        else:
                            if op.startswith('nfts|view'):
                                _, index, page = op.split('/')
                                markup, text = await create_display_nfts(id_, nft_index=int(index), page=int(page))
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                            if op.startswith('nfts|list'):
                                _, index, page, mode = op.split('/')
                                lister = await lister_db.find_one({"to": task['address'], "tokenId": task['nfts'][int(index)]['tokenId'], "contractAddress": task['nfts'][int(index)]['contractAddress']})
                                if not lister:
                                    collection = await parser_db.find_one({'contract': task['nfts'][int(index)]['contractAddress']})
                                    if collection:
                                        slug = collection['slug']
                                    else:
                                        slug = None
                                    hash = ''.join(secrets.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(64))
                                    lister_id = f"{hash}{task['nfts'][int(index)]['tokenId']}{task['address'].lower()[-6:-1]}"
                                    lister = {'finished': True, 'status': False, 'openseaSlug': slug, 'txHash': hash, 'collectionName': f"{str(task['nfts'][int(index)]['name']).split('#')[0]}", 'contractAddress': task['nfts'][int(index)]['contractAddress'], 'tokenId': task['nfts'][int(index)]['tokenId'], 'to': task['nfts'][int(index)]['owner']['address'], 'price': {'amount': task['nfts'][int(index)].get('lastSale', {}).get('amount', 0)}, 'id': lister_id}
                                    if slug:
                                        await lister_db.insert_or_update(lister_id, lister)
                                else:
                                    lister_id = lister['id']

                                if lister['openseaSlug'] is None:
                                    await callback.answer(f"Ошибка включения листера для этой NFT")
                                else:
                                    if lister['status'] and mode == 'on' and lister['finished']:
                                        await callback.answer(f"Листер ещё запускается! Подожди!")
                                    elif not lister['status'] and mode == 'off' and not lister['finished']:
                                        await callback.answer(f"Листер ещё выключается! Подожди!")
                                    else:
                                        await lister_db.update_one({'id': lister_id}, {"$set": {"status": True if mode == 'on' else False}})
                                        await callback.answer(f"{'Включаю' if mode == 'on' else 'Выключаю'} листер...")
                                        markup, text = await create_display_nfts(id_, nft_index=int(index), page=int(page))
                                        await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                            if op.startswith('nfts|finish_listers'):
                                await callback.answer(f"Выключаю все листеры! Пожалуйста, подожди!")
                                await lister_db.update_many({"status": True, 'to': task['address'].lower()}, {"$set": {"status": False}})
                                markup, text = await create_display_nfts(id_, main=True)
                                await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                            if op.startswith('nfts|sell'):
                                _, index, page, mode = op.split('/')
                                if mode == 'ask':
                                    lister = await lister_db.find_one({"to": task['address'], "tokenId": task['nfts'][int(index)]['tokenId'], "contractAddress": task['nfts'][int(index)]['contractAddress']})
                                    if not lister or lister['finished']:
                                        confirm_markup = InlineKeyboardBuilder().row(*[InlineKeyboardButton(text='✅ Подтвердить', callback_data=f"blur_bd:{id_}:nfts|sell/{index}/{page}/conf"), InlineKeyboardButton(text='❌ Отменить', callback_data=f"blur_bd:{id_}:nfts|view/{index}/{page}")])
                                        await callback.message.edit_text(text=f"[❔] Ты действительно хочешь продать эту NFT в бид?", reply_markup=confirm_markup.as_markup())
                                    else:
                                        await callback.answer(f"Сначала выключи листер и дождись его выключения!")
                                if mode == 'conf':
                                    markup, text = await create_display_nfts(id_, nft_index=int(index), page=int(page), callback=callback, sell=True)
                                    await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

        except Exception as e:
            error = str(e).lower()
            try:
                if 'message is not modified' not in error:
                    #print(e)
                    if 'nfts' in callback.data and 'list index out of range' in error:
                        await callback.answer("Этой NFT больше нет!", show_alert=True)
                        markup, text = await create_display_nfts(id_, main=True)
                        await callback.message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
                    else:
                        await callback.answer("Неизвестная ошибка!")
            except:
                pass
    try:
        await callback.answer()
    except:
        pass

async def create_bidders_keyboard(bidders_data, page, PAGE_SIZE=10):
    keyboard = InlineKeyboardBuilder()
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    bidders_page = list(bidders_data.items())[start:end]

    total_bidders = 0
    for address, data in bidders_page:
        if not data['finished']:
            total_bidders += 1
            name = f"{data['address'][:5]}...{data['address'][-5:]}" if not data.get('name') else data['name']
            button = InlineKeyboardButton(text=f"{'✅' if data['status'] else '⛔️'} {name}", callback_data=f"blur_bd:{data['id']}:id")
            keyboard.row(button)

    page_buttons = []

    if page > 0:
        prev_button = InlineKeyboardButton(text="⬅️ Пред. стр.", callback_data=f"blur_page:{page - 1}")
        page_buttons.append(prev_button)

    if end < len(bidders_data):
        next_button = InlineKeyboardButton(text="➡️ След. стр.", callback_data=f"blur_page:{page + 1}")
        page_buttons.append(next_button)

    keyboard.row(*page_buttons)

    if total_bidders == 0:
        keyboard.row(InlineKeyboardButton(text="🔄 Обновить", callback_data=f"blur_page:0"))

    return keyboard.as_markup()

async def create_display_task(id_):
    task = await db.find_one({"id": id_})
    markup = InlineKeyboardBuilder()
    name = f"{task['address'][:5]}...{task['address'][-5:]}" if not task.get('name') else task['name']
    status = InlineKeyboardButton(text=f"{'⛔️ Выключить' if task['status'] else '✅ Включить'}", callback_data=f"blur_bd:{task['id']}:{'off' if task['status'] else 'on'}")
    reload = InlineKeyboardButton(text='🔄 Обновить', callback_data=f"blur_bd:{task['id']}:id")
    settings = InlineKeyboardButton(text='⚙️ Настройки', callback_data=f"blur_bd:{task['id']}:stg/stg")
    nfts = InlineKeyboardButton(text="📁 NFT's List", callback_data=f"blur_bd:{task['id']}:nfts")
    back = InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_page:0")
    text = f"🦺 <b>Blur Bidder: {name}</b>\n\n" \
           f"📌 Статус: <i>{'работает' if task['status'] else 'не начал работу'}</i>\n" \
           f"📌 Текущее кол-во бидов: <i>{len(task['bids_data'])}</i>\n"
    markup.row(status, settings).row(nfts).row(reload).row(back)

    return markup.as_markup(), text

async def create_display_settings(id_, main=False, settingsmain=False, bidder=False, preset=False, autolist=False, page=1):
    task = await db.find_one({"id": id_})
    markup = InlineKeyboardBuilder()
    number_emojis = {1: "1️⃣",2: "2️⃣",3: "3️⃣" ,4: "4️⃣",5: "5️⃣", 6: "6️⃣",7: "7️⃣",8: "8️⃣"}

    async def get_display_value(value):
        if isinstance(value, bool):
            display_value = "Да" if value else "Нет"
        else:
            display_value = value
        return display_value

    name = f"{task['address'][:5]}...{task['address'][-5:]}" if not task.get('name') else task['name']

    if main:
        beth_balance = round(task.get('blur_balance',0), 6)
        eth_balance = round(task.get('balance',0), 6)
        text_balance = f"💳 Кошелёк: <code>{task['address']}</code>\n💰 Баланс: {eth_balance} ETH / {beth_balance} BETH"
        text = f"⚙️ <b>Настройки биддера {name}:</b>\n\n{text_balance}\n\n💾 ID экспорта настроек: <code>{task['id']}</code>"
        name = InlineKeyboardButton(text="🏷 Изменить название", callback_data=f"blur_bd:{task['id']}:stg/name")
        main_settings = InlineKeyboardButton(text="🛠 Основные настройки", callback_data=f"blur_bd:{task['id']}:stg/main")
        bidder_settings = InlineKeyboardButton(text="⚖️ Настройки биддера", callback_data=f"blur_bd:{task['id']}:stg/bd")
        #presets = InlineKeyboardButton(text="📁 Пресеты", callback_data=f"blur_bd:{id_}:")
        import_settings = InlineKeyboardButton(text="💾 Импортировать настройки", callback_data=f"blur_bd:{task['id']}:stg/imp")
        back = InlineKeyboardButton(text="⬅️ Назад", callback_data=f"blur_bd:{task['id']}:id")
        markup.add(*[name, main_settings, bidder_settings, import_settings, back])
        return markup.adjust(1).as_markup(), text

    if bidder:
        text = f"⚙️ <b>Настройки биддера {name}:</b>\n\n"
        for index, setting in enumerate(task["settings_bidder"], start=1):
            emoji = number_emojis.get(index, str(index))
            display_value = await get_display_value(setting['value'])
            text += f"{emoji} {setting['name']}: <i>{display_value}</i>\n"
            markup.add(InlineKeyboardButton(text=f"{emoji} {setting['name']}", callback_data=f"blur_bd:{task['id']}:stg/bd|{setting['code']}"))
        markup.add(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"blur_bd:{task['id']}:stg/stg"))
        return markup.adjust(1).as_markup(), text

    if settingsmain:

        async def blur_get_list_text(task, list_items, list_type, id_, page=1):
            items_per_page = 100
            start_index = (page - 1) * items_per_page
            end_index = start_index + items_per_page

            list_page = list_items[start_index:end_index]

            if list_type == 'blk':
                header_text = "📓 BlackList"
            elif list_type == "own":
                header_text = "📗 OwnList"
            else:
                return

            add_callback = f"blur_bd:{id_}:stg/{list_type}|add"
            remove_callback = f"blur_bd:{id_}:stg/{list_type}|del"
            remove_all_callback = f"blur_bd:{id_}:stg/{list_type}|dall"

            list_text = f"<b>{header_text} для биддера {task['name']}</b>\n\nКоличество коллекций в <b>{header_text}</b>'е: <i>{len(list_items)}</i>\n\n💾 ID экспорта: <code>{id_}</code>\n\n"
            i = start_index + 1
            for item in list_page:
                list_text += f"{i}. <code>{item}</code>\n"
                i += 1

            markup = InlineKeyboardBuilder()

            first_row_buttons = [InlineKeyboardButton(text='➕ Добавить', callback_data=add_callback)]
            if len(list_items) > 0:
                first_row_buttons.append(InlineKeyboardButton(text='➖ Убрать', callback_data=remove_callback))
            markup.row(*first_row_buttons)

            if len(list_items) > 0:
                markup.add(InlineKeyboardButton(text='✖️ Удалить все', callback_data=remove_all_callback))

            pagination_buttons = []
            if page > 1:
                pagination_buttons.append(InlineKeyboardButton(text='◀️ Предыдущая страница', callback_data=f"blur_bd:{id_}:stg/{list_type}|page{page-1}"))
            if end_index < len(list_items):
                pagination_buttons.append(InlineKeyboardButton(text='Следующая страница ▶️',callback_data=f"blur_bd:{id_}:stg/{list_type}|page{page+1}"))
            if pagination_buttons:
                markup.row(*pagination_buttons)
            markup.row(InlineKeyboardButton(text="💾 Импортировать лист", callback_data=f"blur_bd:{id_}:stg/{list_type}|imp"))
            markup.row(InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{id_}:stg/main"))
            return list_text, markup

        if settingsmain is True:

            beth_balance = task.get('blur_balance', 0)
            eth_balance = task.get('balance', 0)
            text = f"⚙️ <b>Основные настройки {name}:</b>\n\n💳 Кошелёк: <code>{task['address']}</code>\n💰 Баланс: {eth_balance} ETH / {beth_balance} BETH"
            autolist = InlineKeyboardButton(text=f"{'✅ Включить AutoList' if not task['auto_list'] else '⛔️ Выключить AutoList'}", callback_data=f"blur_bd:{task['id']}:stg/auto|{'on' if not task['auto_list'] else 'off'}")
            ownlist = InlineKeyboardButton(text=f"{'✅ Включить OwnList' if not task['is_ownlist'] else '⛔️ Выключить OwnList'}", callback_data=f"blur_bd:{task['id']}:stg/own|{'on' if not task['is_ownlist'] else 'off'}")
            auto_settings = InlineKeyboardButton(text="⚙️ Настройка", callback_data=f"blur_bd:{task['id']}:stg/auto|main")
            own_settings = InlineKeyboardButton(text=f"📗 OwnList ({len(task['ownlist'])})", callback_data=f"blur_bd:{task['id']}:stg/own|main")
            blacklist = InlineKeyboardButton(text=f"📓 BlackList ({len(task['blacklist'])})", callback_data=f"blur_bd:{task['id']}:stg/blk|main")
            back = InlineKeyboardButton(text="⬅️ Назад", callback_data=f"blur_bd:{id_}:stg/stg")

            if task['auto_list']:
                markup.row(*[autolist, auto_settings])
            else:
                markup.row(autolist)

            if task['is_ownlist']:
                markup.row(*[ownlist, own_settings])
            else:
                markup.row(ownlist)

            markup.row(blacklist).row(back)
            return markup.as_markup(), text

        elif settingsmain in ["blk", "own"]:
            list_items = task["blacklist"] if settingsmain == "blk" else task["ownlist"]
            text, markup = await blur_get_list_text(task, list_items, settingsmain, id_, page)
            return markup.as_markup(), text

async def create_display_autolist(id_):
    task = await db.find_one({"id": id_})
    markup = InlineKeyboardBuilder()
    name = f"{task['address'][:5]}...{task['address'][-5:]}" if not task.get('name') else task['name']

    autolist_text = f"\n🔄 Auto-List: {'✅' if task['auto_list'] else '⛔️'}\n🕹 Bid Sell: {'✅' if task['auto_bid_sell'] else '⛔️'}\n📉 Auto-List Min Ask {task['auto_list_percent']}%\n⏱ Auto-List Cooldown {round(task['auto_list_cooldown']//60, 1)} мин."
    text = f"⚙️ <b>Настройки биддера {name}:</b>\n{autolist_text}"

    change_ask = InlineKeyboardButton(text='📉 Поменять Min Ask', callback_data=f"blur_bd:{task['id']}:stg/auto|ask")
    change_cooldown = InlineKeyboardButton(text="⏱ Поменять Cooldown", callback_data=f"blur_bd:{task['id']}:stg/auto|cool")
    bid_sell = InlineKeyboardButton(text=f"{'✅ Включить Bid Sell' if not task['auto_bid_sell'] else '⛔️ Выключить Bid Sell'}", callback_data=f"blur_bd:{task['id']}:stg/auto|bid{'on' if not task['auto_bid_sell'] else 'off'}")
    back = InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{id_}:stg/main")

    markup.add(bid_sell).add(change_ask).add(change_cooldown).add(back)

    return markup.adjust(1).as_markup(), text

async def create_display_nfts(id_, main=False, nft_index=None, sell=False, callback=None, page=1):
    task = await db.find_one({"id": id_})
    markup = InlineKeyboardBuilder()
    name = f"{task['address'][:5]}...{task['address'][-5:]}" if not task.get('name') else task['name']

    nfts = task['nfts']

    async def create_pagination_keyboard(data, id_, current_page=1, items_per_page=10):

        total_pages = (len(data) + items_per_page - 1) // items_per_page
        start_index = (current_page - 1) * items_per_page
        end_index = min(start_index + items_per_page, len(data))

        keyboard = InlineKeyboardBuilder()

        for i in range(start_index, end_index):
            name = f'🖼 {data[i].get("name")}' if data[i].get("name", None) != None else f"[❗️] Unknown Token #{data[i].get('tokenId', 0)}"
            button = InlineKeyboardButton(text=name, callback_data=f"blur_bd:{id_}:nfts|view/{i}/{current_page}")
            keyboard.row(button)

        if current_page > 1 and current_page < total_pages:
            prev_button = InlineKeyboardButton(text="◀️ Пред. стр.", callback_data=f"blur_bd:{id_}:nfts|page/{current_page - 1}")
            next_button = InlineKeyboardButton(text="След. стр. ▶️", callback_data=f"blur_bd:{id_}:nfts|page/{current_page + 1}")
            keyboard.row(*[prev_button, next_button])
        elif current_page > 1:
            prev_button = InlineKeyboardButton(text="◀️ Пред. стр.", callback_data=f"blur_bd:{id_}:nfts|page/{current_page - 1}")
            keyboard.row(prev_button)
        elif current_page < total_pages:
            next_button = InlineKeyboardButton(text="След. стр. ▶️",callback_data=f"blur_bd:{id_}:nfts|page/{current_page + 1}")
            keyboard.row(next_button)

        return keyboard

    if main is True:
        active_listers = await lister_db.find_many({"to": task['address'], "finished": False})
        text = f'<b>📁 Bidder {name} NFTs:</b>\n\n🧾 Количество NFT: {len(nfts)}\n🔎 Количество активных листеров: {len(active_listers)}\n💰 Примерная стоимость всех <b>NFT</b>: <code>{round(sum([float(nft.get("highestBid", {}).get("amount", 0)) for nft in nfts]), 4)}</code> ETH\n'
        markup = await create_pagination_keyboard(nfts, id_, current_page=page)
        if len(active_listers) != 0:
            markup.row(InlineKeyboardButton(text="⛔️ Выключить все листеры", callback_data=f"blur_bd:{id_}:nfts|finish_listers"))
        markup.row(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"blur_bd:{id_}:id"))

    if nft_index is not None:
        nft_data = task["nfts"][nft_index]
        name = f'🖼 {nft_data.get("name")}' if nft_data.get("name", None) != None else f"[❗️] Unknown Token #{nft_data.get('tokenId', 0)}"
        text = f'<b>{name}</b>:\n\n'

        if not sell:
            listed_data = f'\n<b>📑 Текущая цена листинга:</b> <code>{round(float(nft_data["price"]["amount"]),6)}</code> ETH' if nft_data['price'] else ""
            rarity_data = f"\n🥇 <b>RarityScore:</b> <code>{nft_data['rarityScore']}</code>\n🎖 <b>RarityRank:</b> <code>{nft_data['rarityRank']}</code>\n" if nft_data['rarityScore'] else ""
            last_sale_data = f"\n🧾 <b>Последняя продажа:</b> <code>{nft_data['lastSale']['amount']}</code> ETH\n" if nft_data["lastSale"] else ''

            auto_lister = await lister_db.find_one({"to": task['address'], "tokenId": nft_data['tokenId'], "contractAddress": nft_data['contractAddress']})
            if not auto_lister:
                auto_lister_data = '⛔️ Авто-листер: <i>выключен</i>'
                auto_lister_status = False
            else:
                auto_lister_status = not auto_lister['finished']
                auto_lister_data = f'{"⛔️" if not auto_lister_status else "✅"} Авто-листер: <i>{"выключен" if not auto_lister_status else "включен"}</i>'

            text += f'<a href="{nft_data["imageUrl"]}"> </a>\n{auto_lister_data}{listed_data}{last_sale_data}{rarity_data}'

            markup.row(InlineKeyboardButton(text=f'{"✅ Включить авто-листер" if not auto_lister_status else "⛔️ Выключить авто-листер"}', callback_data=f"blur_bd:{id_}:nfts|list/{nft_index}/{page}/{'on' if not auto_lister_status else 'off'}"))
            if nft_data['highestBid']:
                markup.row(InlineKeyboardButton(text=f'🗑 Продать в оффер ({nft_data["highestBid"]["amount"]} ETH)', callback_data=f"blur_bd:{id_}:nfts|sell/{nft_index}/{page}/ask"))
            markup.row(InlineKeyboardButton(text='🟠 Открыть на Blur', url=f"https://blur.io/blast/asset/{nft_data['contractAddress']}/{nft_data['tokenId']}"))
            markup.row(InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{id_}:nfts"))

        if sell:

            async def accept_sell_quote(task_data, quote_id, fee, headers, proxy):
                for i in range(3):
                    try:

                        json_data = {
                            'contractAddress': task_data['contractAddress'],
                            'tokens': [
                                {
                                    'tokenId': str(task_data['tokenId']),
                                },
                            ],
                            'feeRate': int(fee),
                            'quoteId': quote_id,
                        }

                        res = await fetcher.fetch_url(method='POST', url=f"https://core-api.prod-blast.blur.io/v1/bids/accept", payload=json_data, headers=headers, proxies=proxy)

                        if not res or not res.get('json'):
                            raise Exception

                        return res['json']

                    except:
                        await asyncio.sleep(i + 5)

            async def get_listing_fee(task_data, headers, proxy):
                for i in range(3):
                    try:

                        res = await fetcher.fetch_url(method='GET', headers=headers, url=f"https://core-api.prod-blast.blur.io/v1/collections/{task_data['contractAddress']}/fees", proxies=proxy)

                        if not res or not res.get('json'):
                            raise Exception

                        return int(res['json']['fees']['byMarketplace']['BLUR']['minimumRoyaltyBips'])

                    except:
                        await asyncio.sleep(i + 5)
                return 0

            async def get_sell_quote(task_data, headers, proxy):
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

                        res = await fetcher.fetch_url(method='POST', url=f"https://core-api.prod-blast.blur.io/v1/bids/quote", payload=json_data, headers=headers, proxies=proxy)

                        if not res or not res.get('json'):
                            raise Exception

                        return res['json']

                    except:
                        await asyncio.sleep(i + 5)

            async def sell_wait(callback, event):
                start_text = f'<b>{name}</b>:\n\n'
                clocks = ['🕛', '🕐', '🕑', '🕒', '🕓', '🕔', '🕕', '🕖', '🕗', '🕘', '🕙', '🕚']
                dots = ['', '.', '..', '...']
                text = "Инициирую продажу, подожди"

                while True:
                    for i in range(len(clocks)):
                        if event.is_set():
                            return
                        current_clock = clocks[i]
                        current_dots = dots[i % len(dots)]
                        message_text = f"{start_text}{current_clock} {text}{current_dots}"
                        await callback.message.edit_text(message_text, parse_mode='HTML')
                        await asyncio.sleep(0.5)

            async def process_selling(event):

                w3 = await get_proxy_web3(task['proxy'], await get_normal_web3('https://rpc.blast.io'))
                account = w3.eth.account.from_key(task['key'])
                headers = task['session_data']['headers']
                headers.update({"cookie" : f"walletAddress={task['address'].lower()}; authToken={task['auth']}"})

                for i in range(3):
                    try:
                        fee = await get_listing_fee(nft_data, headers, task['proxy'])

                        sell_quote = await get_sell_quote(nft_data, headers, task['proxy'])
                        if not sell_quote:
                            raise Exception

                        accept_quote = await accept_sell_quote(nft_data, sell_quote['quoteId'], fee, headers, task['proxy'])
                        if not accept_quote:
                            raise Exception
                        if 'Recently transferred'.lower() in str(accept_quote).lower():
                            event.set()
                            return 'recent_transfer'
                        if 'Unexpected owner'.lower() in str(accept_quote).lower():
                            event.set()
                            return 'not_found'

                        if accept_quote['approvals']:
                            approve_data = accept_quote['approvals'][0].get('txnData', accept_quote['approvals'][0].get('transactionRequest'))

                            tx = await onchain.make_tx(w3, account, value=0, to=approve_data['to'], data=approve_data['data'])
                            if tx == "low_native":
                                event.set()
                                return 'low_native'

                            if not tx:
                                raise Exception

                            hash, _ = await onchain.send_tx(w3, account, tx)
                            if not hash:
                                raise Exception

                            tx_status = await onchain.check_for_status(w3, hash)
                            if not tx_status:
                                raise Exception

                        if accept_quote['accepts']:
                            await asyncio.sleep(5)
                            accept_data = accept_quote['accepts'][0].get('txnData', accept_quote['accepts'][0].get('transactionRequest'))

                            tx = await onchain.make_tx(w3, account, value=0, to=accept_data['to'], data=accept_data['data'])
                            if tx == "low_native":
                                event.set()
                                return 'low_native'

                            if not tx:
                                raise Exception

                            hash, _ = await onchain.send_tx(w3, account, tx)
                            if not hash:
                                raise Exception

                            tx_status = await onchain.check_for_status(w3, hash)
                            if not tx_status:
                                raise Exception

                        if not accept_quote['accepts']:
                            raise Exception

                        event.set()
                        return f'good_{sell_quote["nfts"][0]["price"]["amount"]}'

                    except Exception as e:
                        #print(f"SELL BID EXP: {e}")
                        await asyncio.sleep(i+1)

                event.set()
                return 'error'

            event = asyncio.Event()
            asyncio.create_task(sell_wait(callback, event))
            selling_result = await process_selling(event)

            start_text = f'<b>{name}</b>:\n\n'

            if selling_result.startswith('good_'):
                _, price = selling_result.split('_')
                text = f"{start_text}✅ <b>Продажа завершена успешно!</b>\n<i>Цена продажи <code>{price}</code> ETH!</i>"
            elif selling_result == 'not_found':
                text = f"{start_text}❌ <b>Ты уже не владеешь этой NFT!</b>\n<i>Вероятно, эта NFT уже была продана!</i>"
            elif selling_result == 'recent_transfer':
                text = f"{start_text}❌ <b>NFT была куплена менее 30 минут назад!</b>\n<i>Нельзя продать NFT если она куплена менее 30 минут назад!</i>"
            elif selling_result == 'low_native':
                text = f"{start_text}❌ <b>Недостаточно ETH для продажи NFT в бид!</b>\n<i>Рекомендую пополнить баланс ETH и попробовать снова!</i>"
            elif selling_result == 'error':
                text = f"{start_text}⚠️ <b>Ошибка при продаже!</b>\n<i>Попробуй ещё раз!</i>"
            else:
                text = f"{start_text}❓ <b>Неизвестный результат!</b>\n<i>При продаже возникла неизвестная ошибка!</i>"

            markup.row(InlineKeyboardButton(text='⬅️ Назад', callback_data=f"blur_bd:{id_}:nfts"))

            await asyncio.sleep(2)

    return markup.as_markup(), text

@router.message(Blur.setting)
async def handle_settings(message: Message, state: FSMContext):
    await message.delete()
    data = await state.get_data()
    try:

        if data['setting_type'] == "int":
            new_value = int(message.text)
        else:
            new_value = float(message.text)

        if new_value >= 0:

            task = await db.find_one({"id": data['id']})
            for setting in task['settings_bidder']:
                if data['setting_code'] == setting['code']:
                    setting['value'] = new_value

            await db.update_one({'id': data['id']}, {"$set": {"settings_bidder": task['settings_bidder']}})
            markup, text = await create_display_settings(data['id'], bidder=True)
            await data['callback'].message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

        else:
            try:
                await data['callback'].message.edit_text(text=f"[❗] Ошибка! Значение НЕ должно быть меньше нуля!\n\nОтправь значение для настройки '{data['setting_name']}' ещё раз 👇", reply_markup=data['m'])
            except:
                pass
    except:
        try:
            await data['callback'].message.edit_text(text=f"[❗] Ошибка! Значение должно быть числовым!\n\nОтправь значение для настройки '{data['setting_name']}' ещё раз 👇", reply_markup=data['m'])
        except:
            pass

@router.message(Blur.name)
async def handle_name(message: Message, state: FSMContext):
    await message.delete()
    data = await state.get_data()
    try:

        await db.update_one({'id': data['id']}, {"$set": {"name": message.text}})
        markup, text = await create_display_settings(data['id'], main=True)
        await data['callback'].message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

    except:
        try:
            await data['callback'].message.edit_text(text=f"[❗] Ошибка!\nЧто-то пошло не так, попробуй ещё раз!", reply_markup=data['m'])
        except:
            pass

@router.message(Blur.list)
async def handle_list(message: Message, state: FSMContext):
    await message.delete()
    data = await state.get_data()
    items_per_page = 100
    try:
        current_list = await db.find_one({'id': data['id']})
        current_list = current_list[data['list_name']]
        new_items = message.text.split('\n')
        enumeration_pattern = re.compile(r'^\d+\.\s*')
        new_items = [enumeration_pattern.sub('', item) for item in new_items]

        if data['mode'] == 'add':
            for item in new_items:
                item_no_spaces = item.replace(" ", "")
                if item_no_spaces not in current_list:
                    current_list.append(item_no_spaces.lower())

        elif data['mode'] == 'del':
            for item in new_items:
                if item in current_list:
                    current_list.remove(item)

        await db.update_one({'id': data['id']}, {'$set': {f'{data["list_name"]}': current_list}})
        last_index = len(current_list) - 1
        page = math.ceil((last_index + 1) / items_per_page)

        markup, text = await create_display_settings(id_=data['id'], settingsmain=data['list'], page=page)
        await data['callback'].message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

    except:
        try:
            await data['callback'].message.edit_text(text=f"[❗] Ошибка!\nЧто-то пошло не так, попробуй ещё раз!",reply_markup=data['m'])
        except:
            pass

@router.message(Blur.auto)
async def handle_auto(message: Message, state: FSMContext):
    await message.delete()
    data = await state.get_data()
    try:

        if int(message.text) >= 0:

            if data['setting'] == 'auto_list_cooldown' and int(message.text) > 30:

                await db.update_one({'id': data['id']}, {"$set": {f"{data['setting']}": int(message.text)}})
                markup, text = await create_display_autolist(data['id'])
                await data['callback'].message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

            else:
                try:
                    await data['callback'].message.edit_text(
                        text=f"[❗] Ошибка! Значение НЕ должно быть меньше 30 секунд!\n\nОтправь значение для настройки '{data['setting_name']}' ещё раз 👇",
                        reply_markup=data['m'])
                except:
                    pass

        else:
            try:
                await data['callback'].message.edit_text(text=f"[❗] Ошибка! Значение НЕ должно быть меньше нуля!\n\nОтправь значение для настройки '{data['setting_name']}' ещё раз 👇", reply_markup=data['m'])
            except:
                pass
    except:
        try:
            await data['callback'].message.edit_text(text=f"[❗] Ошибка! Значение должно быть числовым!\n\nОтправь значение для настройки '{data['setting_name']}' ещё раз 👇", reply_markup=data['m'])
        except:
            pass

@router.message(Blur.imp)
async def handle_imp(message: Message, state: FSMContext):
    await message.delete()
    data = await state.get_data()
    try:

        task_to_copy = await db.find_one({'id': message.text})
        if not task_to_copy:
            raise Exception

        if data['list']:
            await db.update_one({"id": data['id']}, {"$set": {data['type']: task_to_copy[data['type']]}})
            markup, text = await create_display_settings(data['id'], settingsmain=data['list'])
            await data['callback'].message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)
        else:
            await db.update_one({"id": data['id']}, {"$set": {'settings_bidder': task_to_copy['settings_bidder']}})
            markup, text = await create_display_settings(data['id'], main=True)
            await data['callback'].message.edit_text(text=text, parse_mode='HTML', reply_markup=markup)

    except:
        try:
            await data['callback'].message.edit_text(text=f"[❗] Ошибка! Указан неверный ID для импорта!", reply_markup=data['m'])
        except:
            pass