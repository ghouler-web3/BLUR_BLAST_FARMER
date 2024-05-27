import asyncio
import time

from helpers.database import parser_db
from helpers.fetcher import AsyncFetcher

class Parser(AsyncFetcher):

    def __init__(self, proxy=None):

        self.session = None
        self.proxy = proxy
        self.semaphore = asyncio.Semaphore(5)

        self.slugs = []

        self.data = {}

    async def get_collections(self):
        for i in range(3):
            try:

                params = {
                    'filters': '{"sort":"VOLUME_ONE_DAY","order":"DESC"}',
                }

                res = await self.fetch_url(session=self.session, params=params, method='GET', url='https://core-api.prod-blast.blur.io/v1/collections')

                if not res or not res.get('json'):
                    raise Exception

                return res['json']

            except:
                await asyncio.sleep(i + 1)

    async def get_collection_data(self, slug):
        for i in range(3):
            try:

                res = await self.fetch_url(session=self.session, method='GET', url=f"https://core-api.prod-blast.blur.io/v1/collections/{slug}")

                if not res or not res.get('json'):
                    raise Exception

                return res['json']

            except:
                await asyncio.sleep(i + 3)

    async def get_collection_bids(self, slug):
        for i in range(3):
            try:

                params = {
                    'filters': '{"criteria":{"type":"COLLECTION","value":{}}}',
                }

                res = await self.fetch_url(session=self.session, method='GET', params=params, url=f"https://core-api.prod-blast.blur.io/v1/collections/{slug}/executable-bids")

                if not res or not res.get('json'):
                    raise Exception

                return res['json']

            except:
                await asyncio.sleep(i + 3)

    async def fetch_collection_data(self, slug):
        for i in range(3):
            try:
                collection_data = await self.get_collection_data(slug)
                if not collection_data:
                    raise Exception

                if await self.to_float_amount(collection_data['collection']['bestCollectionBid']) != 0:
                    collection_bids = await self.get_collection_bids(slug)
                    if not collection_bids:
                        raise Exception
                    bids = collection_bids["priceLevels"]
                else:
                    bids = []

                return {
                    'bids': bids,
                    'slug': slug,
                    'name': collection_data['collection']['name'],
                    'contract': collection_data['collection']['contractAddress'],
                    'image': collection_data['collection']['imageUrl'],
                    'supply': collection_data['collection']['totalSupply'],
                    'owners': collection_data['collection']['numberOwners'],
                    'floor': await self.to_float_amount(collection_data['collection']['floorPrice']),
                    'floor_day': await self.to_float_amount(collection_data['collection']['floorPriceOneDay']),
                    'volume_day': await self.to_float_amount(collection_data['collection']['volumeOneDay']),
                    'best_bid': await self.to_float_amount(collection_data['collection']['bestCollectionBid']),
                    'total_bids_value': await self.to_float_amount(collection_data['collection']['totalCollectionBidValue']),
                    'updated': int(time.time()),
                }

            except Exception as e:
                #print(e)
                await asyncio.sleep(i+1)

    async def fetch_and_save(self):
        while True:
            try:

                initial_time = int(time.time())

                existing_data = await parser_db.get_all()

                old_slugs = set(existing_data.keys())

                cols = await self.get_collections()
                new_slugs = {slug['collectionSlug'] for slug in cols['collections']}

                all_slugs = old_slugs.union(new_slugs)

                tasks = [self.fetch_collection_data_with_semaphore(slug) for slug in all_slugs]
                results = await asyncio.gather(*tasks)

                for slug, result in zip(all_slugs, results):
                    if result is not None:
                        existing_data.update({slug: result})

                parser_db.cache = existing_data
                await parser_db._save()

                #print('fetch done')
                await asyncio.sleep(min(60-int(time.time()-initial_time), 15))

            except Exception as e:
                #print(f"Exp: {e}")
                await asyncio.sleep(5)

    async def fetch_collection_data_with_semaphore(self, slug):
        async with self.semaphore:
            return await self.fetch_collection_data(slug)

    async def main(self):

        if self.session is None:
            self.session = await self.get_session(proxy=self.proxy)

        await self.fetch_and_save()

    async def to_float_amount(self, value):
        try:
            return float(value['amount'])
        except:
            return 0

if __name__ == "__main__":


    async def start():

        parser = Parser()
        await parser.main()

    asyncio.run(start())
