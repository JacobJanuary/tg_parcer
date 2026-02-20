import asyncio
from db import Database

async def run_audit():
    db = Database()
    await db.connect()
    
    print("=" * 50)
    print(" 🕵️ DEEP DATABASE AUDIT REPORT")
    print("=" * 50)
    
    # 1. EVENTS AUDIT
    print("\n--- 1. EVENTS AUDIT ---")
    events = await db.pool.fetch("SELECT * FROM events ORDER BY id DESC")
    print(f"Total events in database: {len(events)}")
    
    duplicates = {}
    fingerprint_map = {}
    desc_map = {}
    
    for e in events:
        # Check by new fingerprint
        date_str = e['event_date'].isoformat() if e['event_date'] else None
        fp = db._fingerprint(e['title'], date_str)
        if fp in fingerprint_map:
            fingerprint_map[fp].append(e)
        else:
            fingerprint_map[fp] = [e]
            
        # Check by strict title match
        title_lower = e['title'].lower().strip() if e['title'] else ""
        
        # Check for EXACT description duplicates (suspicious)
        desc = e['description'].strip() if e['description'] else ""
        if len(desc) > 50:
            if desc in desc_map:
                desc_map[desc].append(e)
            else:
                desc_map[desc] = [e]
                
    # Report Fingerprint Duplicates
    fp_dupes = {k: v for k, v in fingerprint_map.items() if len(v) > 1}
    if fp_dupes:
        print(f"❌ FOUND {len(fp_dupes)} EVENT DUPLICATES (by title+date):")
        for fp, group in list(fp_dupes.items())[:5]: # Show up to 5
            print(f"  - Dupes for Title: '{group[0]['title']}' on {group[0]['event_date']}")
            for g in group: print(f"    ID: {g['id']} | Src: {g['source']}")
    else:
        print("✅ No duplicate events found based on Title + Date fingerprinting.")
        
    # Report Description Duplicates
    desc_dupes = {k: v for k, v in desc_map.items() if len(v) > 1}
    # Filter out empty or too short descriptions
    if desc_dupes:
        print(f"⚠️ FOUND {len(desc_dupes)} groups of identical descriptions:")
        for desc, group in list(desc_dupes.items())[:3]:
            print(f"  - Desc starts: '{desc[:50]}...' shared by {len(group)} events:")
            for e in group: print(f"    ID: {e['id']} | Title: {e['title']}")
    else:
        print("✅ No copy-pasted (identical 1:1) descriptions found across different events.")
        
        
    # 2. VENUES AUDIT
    print("\n--- 2. VENUES AUDIT ---")
    venues = await db.pool.fetch("SELECT * FROM venues ORDER BY id DESC")
    print(f"Total venues in database: {len(venues)}")
    
    v_name_map = {}
    v_coords_map = {}
    found_count = 0
    not_found_count = 0
    
    for v in venues:
        if v['found']: found_count += 1
        else: not_found_count += 1
        
        name = v['name']
        if name:
            name_lower = name.lower().strip()
            if name_lower in v_name_map:
                v_name_map[name_lower].append(v)
            else:
                v_name_map[name_lower] = [v]
                
        if v['lat'] and v['lng']:
            # round to 4 decimals (~11 meters) to catch duplicates
            coord = (round(v['lat'], 4), round(v['lng'], 4))
            if coord in v_coords_map:
                v_coords_map[coord].append(v)
            else:
                v_coords_map[coord] = [v]
                
    # Report Venue Duplicates
    name_dupes = {k: v for k, v in v_name_map.items() if len(v) > 1}
    if name_dupes:
        print(f"❌ FOUND {len(name_dupes)} VENUE NAME DUPLICATES:")
        for name, group in list(name_dupes.items())[:5]:
            print(f"  - Name '{name}' matched {len(group)} times:")
            for g in group: print(f"    ID: {g['id']} | Query: {g['query']} | Lat: {g['lat']}")
    else:
        print("✅ No duplicate venue names detected.")
        
    coord_dupes = {k: v for k, v in v_coords_map.items() if len(v) > 1}
    if coord_dupes:
        print(f"⚠️ FOUND {len(coord_dupes)} VENUE CLUSTERS (same coordinates):")
        for coord, group in list(coord_dupes.items())[:3]:
            print(f"  - Coordinate {coord} shared by:")
            for g in group: print(f"    ID: {g['id']} | Name: {g['name']}")
    else:
        print("✅ No venues share the exact same coordinates.")
        
    print(f"Venues found on Google Maps: {found_count} | Not found/Fallback: {not_found_count}")
    
    
    # 3. LINKAGE AUDIT (Events -> Venues)
    print("\n--- 3. DATA INTEGRITY (EVENTS -> VENUES) ---")
    linked = await db.pool.fetchval("SELECT COUNT(*) FROM events WHERE venue_id IS NOT NULL")
    print(f"Events properly linked to a Google Maps venue: {linked} / {len(events)}")
    
    
    # 4. RANDOM SAMPLES (Manual verification)
    print("\n--- 4. RANDOM SAMPLE REVIEW (Last 5 linked events) ---")
    sample_events = await db.pool.fetch('''
        SELECT e.title, e.event_date, e.description, v.name as v_name, v.address, v.google_maps_url 
        FROM events e 
        JOIN venues v ON e.venue_id = v.id 
        ORDER BY e.detected_at DESC LIMIT 5
    ''')
    for i, e in enumerate(sample_events):
        print(f"\n[{i+1}] {e['title']} ({e['event_date']})")
        print(f"    Desc: {e['description'][:100]}...")
        print(f"    Venue: {e['v_name']} | {e['address']}")
        print(f"    URL: {e['google_maps_url']}")

    await db.close()

if __name__ == '__main__':
    asyncio.run(run_audit())
