import asyncio
from web3.providers.async_rpc import AsyncHTTPProvider
from web3 import AsyncWeb3

async def get_proxy_web3(proxy, w3, attempt=0):

    if attempt > 30:
        return w3

    try:
        request_kwargs = {"proxy": proxy} if proxy else {}
        rpc = str(w3.provider).replace('RPC connection ', '')

        provider = AsyncHTTPProvider(rpc, request_kwargs=request_kwargs)
        web3 = AsyncWeb3(provider)

        connection_established = await web3.is_connected()
        if connection_established:
            return web3
        else:
            raise Exception
    except Exception:
        await asyncio.sleep(1)

    return await get_proxy_web3(proxy=proxy, w3=w3, attempt=attempt + 1)

async def get_normal_web3(RPC):
    return AsyncWeb3(AsyncHTTPProvider(RPC))