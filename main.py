from aiohttp import ClientSession
import asyncio
from more_itertools import chunked
from db import engine, Session, People, Base

MAX_SIZE = 10


async def paste_to_db(people_list):
    async with Session() as session:
        people_list_orm = [People(**item) for item in people_list if item]
        session.add_all(people_list_orm)
        await session.commit()


async def get_people(people_id: int, client: ClientSession):
    url = f'https://swapi.dev/api/people/{people_id}'
    async with client.get(url, ssl=False) as response:
        json_data = await response.json()
    if json_data.get('detail') == 'Not found':
        return None
    json_data['id'] = people_id
    del (json_data['created'],
         json_data['edited'],
         json_data['url'],
         )
    print(json_data)
    json_data['films'] = await get_name_list(json_data['films'], client, 'title')
    json_data['homeworld'] = await get_name_from_inner_url(json_data['homeworld'], client, 'name')
    json_data['species'] = await get_name_list(json_data['species'], client, 'name')
    json_data['starships'] = await get_name_list(json_data['starships'], client, 'name')
    json_data['vehicles'] = await get_name_list(json_data['vehicles'], client, 'name')

    return json_data


async def get_name_from_inner_url(url, client: ClientSession, field_name):
    async with client.get(url, ssl=False) as response:
        json_data = await response.json()
    return json_data[field_name]


async def get_name_list(url_list: list, client: ClientSession, field_name: str = 'name') -> str:
    result = ','.join([await get_name_from_inner_url(url, client, field_name) for url in url_list])

    return result


async def main():
    tasks = []
    async with ClientSession() as session:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

        for id_chunk in chunked(range(1, 100), MAX_SIZE):
            coros = [get_people(people_id, client=session) for people_id in id_chunk]
            people_list = await asyncio.gather(*coros)
            db_coro = paste_to_db(people_list)
            paste_to_db_task = asyncio.create_task(db_coro)
            tasks.append(paste_to_db_task)
        tasks = asyncio.all_tasks() - {asyncio.current_task()}
        for task in tasks:
            await task


asyncio.run(main())
