import asyncio
import random
import json
from curl_cffi import requests
import sys
from .user_agent import get_random, country_mappings

def set_windows_event_loop_policy():
    if sys.version_info >= (3, 8) and sys.platform.lower().startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

set_windows_event_loop_policy()

class AsyncFetcher():

    def get_random_impersonate_and_ua(self, session_data):
        if not session_data:
            chrome_version_details, windows_nt_version, arch, bitness = get_random()
            headers = {
                "accept": "*/*",
                "accept-language": "de-DE,de",
                "user-agent": f"Mozilla/5.0 (Windows NT {windows_nt_version}; Win64; {arch}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version_details['full_version']} Safari/537.36",
                "sec-ch-ua": f'"Google Chrome";v="{chrome_version_details["major_version"]}", "Not:A Brand";v="99", "Chromium";v="{chrome_version_details["major_version"]}"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-model": '""',
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
            if chrome_version_details == '119':
                impersonate = requests.BrowserType.chrome119
            else:
                impersonate = requests.BrowserType.chrome120
            return impersonate, headers
        else:
            headers = session_data['headers']
            if session_data['version'] == '119':
                impersonate = requests.BrowserType.chrome119
            else:
                impersonate = requests.BrowserType.chrome120
            return impersonate, headers

    async def get_ip_info(self, session):
        for i in range(3):
            try:
                ip_res = await self.fetch_url(session=session, url='https://ipinfo.io/widget', method='GET')
                if not ip_res or not ip_res['json']:
                    raise Exception

                return ip_res['json']

            except:
                await asyncio.sleep(i+1)

    async def get_session(self, proxy=None, session_data=None, **session_kwargs):
        proxy = None if not proxy else {'http': proxy.as_url, 'https': proxy.as_url}
        impersonate, headers = self.get_random_impersonate_and_ua(session_data)
        session_kwargs["impersonate"] = session_kwargs.get("impersonate", impersonate)
        session_kwargs['headers'] = headers
        session = requests.AsyncSession(**session_kwargs, proxies=proxy)
        ip_info = await self.get_ip_info(session)
        if ip_info:
            country = ip_info.get('country', 'DE')
            lang_header = country_mappings.get(country, 'de-DE,de;q=0.9')
            if random.choice([True, False]):
                lang_header = lang_header.split(';q=')[0]
            session.headers.update({"accept-language": lang_header})

        return session

    async def close_session(self, session):
        try:
            session_closed = session._closed
        except:
            session_closed = False
        if not session_closed:
             session.close()

    async def fetch_url(self, session=None, url=None, method="GET", payload=None, headers=None, params=None, data=None, cookies=None, proxies=None, timeout=60, retries=15):

        if session is None:
            session = requests.AsyncSession()

        if not url:
            #print("AsyncFetcher: No URL provided.")
            return None

        for attempt in range(retries):
            try:
                if method.lower() == 'get':
                    response = await session.get(url=url, timeout=timeout, headers=headers, params=params, json=payload, data=data, cookies=cookies, proxy=proxies)
                    return await self.process_response(response)
                elif method.lower() == 'post':
                    response = await session.post(url=url, timeout=timeout, headers=headers, params=params, json=payload, data=data, cookies=cookies, proxy=proxies)
                    return await self.process_response(response)
                else:
                    #print("AsyncFetcher: invalid METHOD")
                    return
            except Exception as e:
                #print(f"AsyncFetcher: Retry {attempt + 1}/{retries} failed: {e}")
                await asyncio.sleep(attempt + 1)

        #print("AsyncFetcher: Max retries reached.")
        return None

    async def process_response(self, response):

        data = {}

        try:
            data['cookies'] = response.cookies
        except:
            data['cookies'] = None

        try:
            data['url'] = str(response.url)
        except:
            data['url'] = None

        try:
            data['content'] = await response.read()
        except:
            data['content'] = None

        try:
            data['text'] = response.text
        except:
            data['text'] = None

        try:
            data['headers'] = response.headers
        except:
            data['headers'] = None

        try:
            data['json'] = json.loads(data['text'])
        except:
            data['json'] = None

        #print(f"AsyncHelper JSON: {data['text'][:200]}")

        return data

