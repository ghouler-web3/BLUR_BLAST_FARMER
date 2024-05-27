import asyncio
import aiofiles
import json
import os
from typing import Any, Dict, List

class AsyncJSONDatabase:
    def __init__(self, filename: str):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.filename = os.path.join(script_dir, filename)
        self.lock = asyncio.Lock()
        self.cache: Dict[str, Any] = {}

    async def _load(self):
        async with self.lock:
            try:
                async with aiofiles.open(self.filename, 'r', encoding='utf-8') as f:
                    data = await f.read()
                    self.cache = json.loads(data)
            except FileNotFoundError:
                self.cache = {}
            except json.JSONDecodeError:
                self.cache = {}

    async def _save(self):
        async with self.lock:
            async with aiofiles.open(self.filename, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.cache, indent=2))

    async def insert_or_update(self, key: str, value: Any):
        await self._load()
        self.cache[key] = value
        await self._save()

    async def get(self, key: str) -> Any:
        await self._load()
        return self.cache.get(key)

    async def delete(self, key: str):
        await self._load()
        if key in self.cache:
            del self.cache[key]
            await self._save()

    async def update_key(self, name: str, key: str, value: Any):
        await self._load()
        if name in self.cache:
            if isinstance(self.cache[name], dict):
                self.cache[name][key] = value
            else:
                raise ValueError(f"Wallet data for '{name}' is not a dictionary.")
        else:
            self.cache[name] = {key: value}
        await self._save()

    async def get_all(self) -> Dict[str, Any]:
        await self._load()
        return self.cache

    def _match_query(self, item: Dict[str, Any], query: Dict[str, Any]) -> bool:
        for key, condition in query.items():
            if isinstance(condition, dict):
                for op, value in condition.items():
                    if op == '$gte':
                        if not item.get(key, float('-inf')) >= value:
                            return False
                    elif op == '$lte':
                        if not item.get(key, float('inf')) <= value:
                            return False
                    elif op == '$gt':
                        if not item.get(key, float('-inf')) > value:
                            return False
                    elif op == '$lt':
                        if not item.get(key, float('inf')) < value:
                            return False
                    elif op == '$eq':
                        if not item.get(key) == value:
                            return False
                    elif op == '$ne':
                        if not item.get(key) != value:
                            return False
                    elif op == '$in':
                        if not item.get(key) in value:
                            return False
                    elif op == '$nin':
                        if not item.get(key) not in value:
                            return False
                    else:
                        return False
            elif item.get(key) != condition:
                return False
        return True

    def _apply_update(self, item: Dict[str, Any], update: Dict[str, Any]):
        for op, fields in update.items():
            if op == '$set':
                for key, value in fields.items():
                    item[key] = value
            elif op == '$inc':
                for key, value in fields.items():
                    if key in item and isinstance(item[key], (int, float)):
                        item[key] += value
                    else:
                        item[key] = value
            elif op == '$dec':
                for key, value in fields.items():
                    if key in item and isinstance(item[key], (int, float)):
                        item[key] -= value
                    else:
                        item[key] = -value
            elif op == '$unset':
                for key in fields.keys():
                    if key in item:
                        del item[key]
            elif op == '$push':
                for key, value in fields.items():
                    if key in item and isinstance(item[key], list):
                        item[key].append(value)
                    else:
                        item[key] = [value]
            elif op == '$pull':
                for key, value in fields.items():
                    if key in item and isinstance(item[key], list):
                        item[key] = [v for v in item[key] if not self._match_query(v, value)]

    async def find_one(self, query: Dict[str, Any]) -> Dict[str, Any]:
        await self._load()
        for key, value in self.cache.items():
            if isinstance(value, dict) and self._match_query(value, query):
                return value
        return None

    async def find_many(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        await self._load()
        results = []
        for key, value in self.cache.items():
            if isinstance(value, dict) and self._match_query(value, query):
                results.append(value)
        return results

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any]) -> bool:
        await self._load()
        for key, value in self.cache.items():
            if isinstance(value, dict) and self._match_query(value, query):
                self._apply_update(value, update)
                await self._save()
                return True
        return False

    async def update_many(self, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        await self._load()
        count = 0
        for key, value in self.cache.items():
            if isinstance(value, dict) and self._match_query(value, query):
                self._apply_update(value, update)
                count += 1
        if count > 0:
            await self._save()
        return count

db = AsyncJSONDatabase('databases/db.json')
parser_db = AsyncJSONDatabase('databases/parser.json')
lister_db = AsyncJSONDatabase('databases/lister.json')
check_db = AsyncJSONDatabase('databases/check.json')  # YEAH THIS IS STUPID