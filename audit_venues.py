import asyncio
from db import Database

async def audit():
    db = Database()
    await db.connect()
    
    # Extract venues that were explicitly marked as not found
    not_found = await db.pool.fetch("""
        SELECT name, query 
        FROM venues 
        WHERE found = false OR found IS NULL
        ORDER BY id DESC
    """)
    
    print(f"Total not found: {len(not_found)}")
    print("List of venues not found:")
    for v in not_found:
        name = v['name'] if v['name'] else "NO_NAME_SAVED"
        query = v['query'] if v['query'] else "NO_QUERY_SAVED"
        print(f" - [Query]: {query[:50]:<50} | [Name]: {name[:30]}")

if __name__ == '__main__':
    asyncio.run(audit())
