from web3.auto import w3
from better_proxy import Proxy

def is_valid(value):
    if value is None:
        return
    if isinstance(value, int) or isinstance(value, float):
        return
    if isinstance(value, str) and value == 'nan':
        return
    if isinstance(value, str):
        return value
    return

class AccountData:

    def __init__(self, data):

        self.data = data
        try:
            proxy = data.get('proxy')
            if type(data.get('proxy')) == str:
                if 'http' not in data.get('proxy'):
                    proxy = f"http://{data.get('proxy')}"
            self.proxy = Proxy.from_str(proxy)
        except Exception as e:
            self.proxy = None

        self.private_key = is_valid(data.get('private_key'))
        try:
            self.account = w3.eth.account.from_key(self.private_key)
        except:
            self.account = None

        self.w3 = None

    def to_json(self):
        return {
            "private_key": self.private_key,
            "proxy": str(self.proxy.as_url) if self.proxy else 'None',
        }

