import os
import json
import time
import math
import asyncio
import urllib.parse
import aiohttp
import redis.asyncio as aioredis

import firebase_admin
from firebase_admin import credentials, firestore
from quart import Quart, request, jsonify

app = Quart(__name__)

# --- Configuration & Environment Variables ---
raw_coc_keys = os.environ.get("COC_KEYS", "")
COC_KEYS = [k.strip() for k in raw_coc_keys.split(",") if k.strip()]

firebase_creds_raw = os.environ.get("FIREBASE_CREDENTIALS")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

# Initialize Redis Client
try:
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    print("✅ Connected to Redis successfully.", flush=True)
except Exception as e:
    print(f"⚠️ Error connecting to Redis: {e}", flush=True)
    redis_client = None

# Initialize Firebase Admin SDK
db = None
if firebase_creds_raw:
    try:
        cred_dict = json.loads(firebase_creds_raw)
        if "private_key" in cred_dict:
            pk = cred_dict["private_key"].replace("\\n", "\n")
            cred_dict["private_key"] = pk

        cred = credentials.Certificate(cred_dict)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Connected to Firebase successfully.", flush=True)
    except Exception as e:
        print(f"⚠️ Error parsing or connecting to Firebase: {e}", flush=True)
else:
    print("⚠️ WARNING: FIREBASE_CREDENTIALS environment variable is missing or empty!", flush=True)

START_TIME = time.time()
WORKER_RUNNING = False

# --- Background Worker for Continuous Fetching ---

async def background_war_fetcher():
    """Continuously fetches and caches current wars for all registered clans every 5 minutes."""
    global WORKER_RUNNING
    WORKER_RUNNING = True
    print("🚀 [Background Worker] Worker loop started successfully!", flush=True)

    while True:
        try:
            print("🔄 [Background Worker] Starting scheduled clan war fetch cycle...", flush=True)
            clans = await get_registered_clans()
            if clans:
                print(f"📋 Processing {len(clans)} registered clans...", flush=True)
                active_count = 0
                for clan in clans:
                    tag = clan.get('clantag') or clan.get('clan tag') or clan.get('tag')
                    name = clan.get('clan') or clan.get('Clan Name') or 'Unknown'
                    if tag:
                        print(f"🔍 Fetching current war for clan: {name} ({tag})...", flush=True)
                        try:
                            doc_id = await process_and_cache_clan_war(tag)
                            if doc_id:
                                active_count += 1
                                print(f"  └─ ✅ Cached war record: war:{doc_id}", flush=True)
                            else:
                                print(f"  └─ ℹ️ Clan {name} is not currently in war or war details are hidden.", flush=True)
                        except Exception as e:
                            print(f"  └─ ⚠️ Error processing clan {tag}: {e}", flush=True)
                        
                        await asyncio.sleep(1.5)
                
                print(f"✅ [Background Worker] Completed cycle. {active_count} active wars cached into Redis.", flush=True)
            else:
                print("⚠️ [Background Worker] No clans found in Redis or Firebase to process.", flush=True)

        except Exception as e:
            print(f"⚠️ Error in background war worker: {e}", flush=True)
        
        print("💤 [Background Worker] Sleeping for 5 minutes before next run...", flush=True)
        await asyncio.sleep(300)

@app.before_serving
async def startup():
    """Start background automated polling worker when Quart server starts."""
    print("⚙️ Initializing app background hooks...", flush=True)
    app.add_background_task(background_war_fetcher)

# --- CoC API & Data Functions ---

async def fetch_coc_endpoint(endpoint_path):
    """Generic fetch utility for CoC API using key rotation."""
    if not COC_KEYS:
        print("⚠️ No COC_KEYS set in environment!", flush=True)
        return None

    url = f"https://api.clashofclans.com/v1/{endpoint_path}"

    async with aiohttp.ClientSession() as session:
        for key in COC_KEYS:
            headers = {
                "Authorization": f"Bearer {key}",
                "Accept": "application/json"
            }
            try:
                async with session.get(url, headers=headers, timeout=8) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status in (403, 429):
                        continue
                    elif resp.status == 404:
                        return None
            except Exception as err:
                print(f"⚠️ API request exception: {err}", flush=True)
                continue
    return None

async def fetch_coc_player(player_tag):
    """Fetch player info using COC_KEYS."""
    clean_tag = player_tag.strip().upper()
    if not clean_tag.startswith("#"):
        clean_tag = f"#{clean_tag}"
    encoded_tag = urllib.parse.quote(clean_tag)
    return await fetch_coc_endpoint(f"players/{encoded_tag}")

async def get_registered_clans():
    """Retrieve clans list from Redis, fallback to Firebase Firestore if empty."""
    clans = []
    
    if redis_client:
        try:
            key_type = await redis_client.type("clans")
            if key_type in ("ReJSON-RL", "JSON"):
                clans = await redis_client.json().get("clans")
            elif key_type == "string":
                raw = await redis_client.get("clans")
                if raw:
                    clans = json.loads(raw)
        except Exception as e:
            print(f"⚠️ Error reading 'clans' from Redis: {e}", flush=True)

    if not clans and db:
        try:
            print("🔍 Fetching registered clans from Firebase Firestore...", flush=True)
            docs = db.collection('clans').stream()
            for doc in docs:
                cdata = doc.to_dict()
                tag = cdata.get('clan tag') or cdata.get('clantag') or cdata.get('tag') or ''
                name = cdata.get('Clan Name') or cdata.get('clan') or ''
                if tag:
                    clans.append({
                        "clan": name,
                        "clantag": tag,
                        "familyfriendly": cdata.get('family', 'yes'),
                        "permanent association": cdata.get('clan type', 'FWA'),
                        "association": cdata.get('clan type', 'FWA')
                    })
            
            if clans and redis_client:
                try:
                    await redis_client.json().set("clans", "$", clans)
                except Exception:
                    await redis_client.set("clans", json.dumps(clans))
                print(f"✅ Cached {len(clans)} clans from Firebase into Redis.", flush=True)
        except Exception as fb_err:
            print(f"⚠️ Error fetching clans from Firebase: {fb_err}", flush=True)

    return clans or []

async def process_and_cache_clan_war(clan_tag):
    """Fetch current war for a clan tag and store structured data in Redis."""
    clean_tag = clan_tag.strip().upper()
    if not clean_tag.startswith("#"):
        clean_tag = f"#{clean_tag}"

    encoded_tag = urllib.parse.quote(clean_tag)
    war_data = await fetch_coc_endpoint(f"clans/{encoded_tag}/currentwar")

    if not war_data or war_data.get('state') == 'notInWar':
        return None

    clan_a = war_data.get('clan', {})
    clan_b = war_data.get('opponent', {})

    start_time_raw = war_data.get('startTime', str(int(time.time())))
    doc_id = str(abs(hash(f"{clan_a.get('tag')}_{clan_b.get('tag')}_{start_time_raw}")) % 1000000)

    rosters = []
    all_positions = sorted(list(set([m.get('mapPosition', 0) for m in clan_a.get('members', [])])))

    for pos in all_positions:
        ca_m = next((m for m in clan_a.get('members', []) if m.get('mapPosition') == pos), {})
        cb_m = next((m for m in clan_b.get('members', []) if m.get('mapPosition') == pos), {})

        rosters.append({
            "position": pos,
            "clan_a_member": {
                "name": ca_m.get('name', 'N/A'),
                "tag": ca_m.get('tag', ''),
                "attacks": ca_m.get('attacks', []),
                "defenses": ca_m.get('opponentAttacks', [])
            },
            "clan_b_member": {
                "name": cb_m.get('name', 'N/A'),
                "tag": cb_m.get('tag', ''),
                "attacks": cb_m.get('attacks', []),
                "defenses": cb_m.get('opponentAttacks', [])
            }
        })

    structured_doc = {
        "_id": doc_id,
        "clan_tag": clean_tag,
        "conflict_data": {
            "war_metadata": {
                "prep_day_start": war_data.get('preparationStartTime', 'N/A'),
                "battle_day_start": war_data.get('startTime', 'N/A'),
                "war_ends": war_data.get('endTime', 'N/A'),
                "status": war_data.get('state', 'inWar').capitalize(),
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
            },
            "clans": {
                "clan_a": {
                    "name": clan_a.get('name'),
                    "tag": clan_a.get('tag'),
                    "level": clan_a.get('clanLevel'),
                    "badge": clan_a.get('badgeUrls', {}).get('medium'),
                    "stars": clan_a.get('stars', 0),
                    "destruction_percentage": f"{clan_a.get('destructionPercentage', 0):.2f}%",
                    "attacks_used": clan_a.get('attacks', 0),
                    "members_count": len(clan_a.get('members', []))
                },
                "clan_b": {
                    "name": clan_b.get('name'),
                    "tag": clan_b.get('tag'),
                    "level": clan_b.get('clanLevel'),
                    "badge": clan_b.get('badgeUrls', {}).get('medium'),
                    "stars": clan_b.get('stars', 0),
                    "destruction_percentage": f"{clan_b.get('destructionPercentage', 0):.2f}%",
                    "attacks_used": clan_b.get('attacks', 0),
                    "members_count": len(clan_b.get('members', []))
                }
            },
            "rosters": rosters
        }
    }

    redis_key = f"war:{doc_id}"
    if redis_client:
        try:
            await redis_client.json().set(redis_key, "$", structured_doc)
        except Exception:
            await redis_client.set(redis_key, json.dumps(structured_doc))

    return doc_id

# --- Routes ---

@app.route('/')
@app.route('/live_wars')
async def home():
    """Homepage: Displays all active/cached war records stored in Redis using light layout."""
    if not redis_client:
        return "Redis is not connected.", 500

    try:
        live_wars_list = []
        async for key in redis_client.scan_iter("war:*"):
            doc_id = key.split(":")[-1]
            data = None
            try:
                key_type = await redis_client.type(key)
                if key_type in ("ReJSON-RL", "JSON"):
                    data = await redis_client.json().get(key)
                elif key_type == "string":
                    cached_json = await redis_client.get(key)
                    if cached_json:
                        data = json.loads(cached_json)
            except Exception as e:
                print(f"⚠️ Error reading key {key}: {e}", flush=True)

            if data:
                conflict_data = data.get('conflict_data', {})
                war_meta = conflict_data.get('war_metadata', {})
                clans = conflict_data.get('clans', {})
                clan_a = clans.get('clan_a', {})
                clan_b = clans.get('clan_b', {})
                
                live_wars_list.append({
                    "doc_id": doc_id,
                    "clan_a_name": clan_a.get('name', 'Unknown Clan A'),
                    "clan_a_tag": clan_a.get('tag', ''),
                    "clan_b_name": clan_b.get('name', 'Unknown Clan B'),
                    "clan_b_tag": clan_b.get('tag', ''),
                    "status": war_meta.get('status', 'N/A'),
                    "war_ends": war_meta.get('war_ends', 'N/A')
                })

        rows_html = ""
        for war in live_wars_list:
            rows_html += f"""
            <tr>
                <td style="font-family: monospace;">{war['doc_id']}</td>
                <td><b>{war['clan_a_name']}</b><br><span style="font-size:12px; color: gray;">{war['clan_a_tag']}</span></td>
                <td><b>{war['clan_b_name']}</b><br><span style="font-size:12px; color: gray;">{war['clan_b_tag']}</span></td>
                <td><span style="color: green; font-weight: bold;">{war['status']}</span></td>
                <td style="font-size: 14px;">{war['war_ends']}</td>
                <td><a href="/war_conflicts?{war['doc_id']}" style="background-color: #2980b9; color: white; padding: 6px 12px; border-radius: 4px; font-size: 13px; font-weight: bold; text-decoration: none;">Inspect Match</a></td>
            </tr>
            """

        if not rows_html:
            rows_html = "<tr><td colspan='6' style='text-align:center; padding: 15px; color: gray;'>No live wars currently cached in Redis. Waiting for background fetch worker or <a href='/api/update_wars'>click here to trigger refresh</a>.</td></tr>"

        full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Chocolate Clash: Live Redis Wars</title>
    <style>
        body {{ font-family: "Times New Roman", Times, serif; font-size: 16px; background-color: #ffffff; color: #000000; margin: 0; padding: 20px; }}
        #container {{ width: 85%; max-width: 1100px; margin: 20px auto; border: 1px solid black; text-align: center; padding: 20px; background-color: #ffffff; box-shadow: 0 4px 10px rgba(0,0,0,0.05); }}
        #title {{ font-weight: bold; font-size: 32px; color: #000000; }}
        #top {{ display: block; text-align: center; font-size: 16px; margin-bottom: 20px; color: #333; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ border-bottom: 2px solid black; padding: 10px; font-size: 15px; text-align: left; background-color: #f8f9fa; }}
        td {{ border-bottom: 1px solid gray; font-size: 15px; text-align: left; padding: 10px 8px; vertical-align: middle; color: #000000; }}
        a {{ text-decoration: none; color: blue; }}
        .update-btn {{ background-color: #27ae60; color: white; padding: 8px 16px; border-radius: 4px; text-decoration: none; font-weight: bold; margin-left: 10px; font-size: 14px; }}
    </style>
</head>
<body>
    <div id="container">
        <span id="title">FWA ChocolateClash</span>
        <br><br>
        <span id="top">
            <b>⚡ Live Redis War Records</b><br>
            Background worker automatically updates every 5 minutes.
            <a href="/api/update_wars" class="update-btn">🔄 Force Refresh Now</a>
        </span>

        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Clan A</th>
                    <th>Opponent (Clan B)</th>
                    <th>Status</th>
                    <th>War Ends</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
</body>
</html>"""
        return full_html, 200

    except Exception as e:
        return f"Internal Error loading live wars: {str(e)}", 500

@app.route('/update_wars', methods=['GET', 'POST'])
@app.route('/api/update_wars', methods=['GET', 'POST'])
async def update_wars():
    """Manual trigger to fetch all registered clans and populate Redis immediately."""
    print("🔘 Manual war refresh triggered via API...", flush=True)
    clans = await get_registered_clans()
    if not clans:
        return jsonify({"status": "error", "message": "No clans found in Redis or Firebase"}), 400

    updated_ids = []
    for c in clans:
        tag = c.get('clantag') or c.get('clan tag') or c.get('tag')
        if tag:
            doc_id = await process_and_cache_clan_war(tag)
            if doc_id:
                updated_ids.append(doc_id)
            await asyncio.sleep(1.0)

    return jsonify({
        "status": "success",
        "clans_processed": len(clans),
        "active_wars_cached": len(updated_ids),
        "war_doc_ids": updated_ids
    }), 200

@app.route('/api/check_clan_status', methods=['POST'])
async def check_clan_status():
    """API endpoint to check if players are currently in a clan."""
    req_data = await request.get_json()
    tags = req_data.get('tags', [])
    if not tags:
        return jsonify({"error": "No tags provided"}), 400

    async def verify_tag(tag):
        pdata = await fetch_coc_player(tag)
        if pdata and 'clan' in pdata:
            return tag, {
                "in_clan": True,
                "clan_name": pdata['clan'].get('name', 'Unknown Clan'),
                "clan_tag": pdata['clan'].get('tag', '')
            }
        return tag, {"in_clan": False}

    tasks = [verify_tag(tag) for tag in set(tags)]
    results = await asyncio.gather(*tasks)
    return jsonify(dict(results)), 200

@app.route('/war_conflicts', methods=['GET'])
async def war_conflicts():
    doc_id = request.args.get('docid')
    if not doc_id and request.args:
        doc_id = list(request.args.keys())[0]

    if not doc_id:
        return "Missing Document ID parameter.", 400

    try:
        data = None
        source_db = ""
        redis_key = f"war:{doc_id}"

        if redis_client:
            try:
                key_type = await redis_client.type(redis_key)
                if key_type in ("ReJSON-RL", "JSON"):
                    data = await redis_client.json().get(redis_key)
                    if data:
                        source_db = "Redis Database (JSON Cache)"
                elif key_type == "string":
                    cached_json = await redis_client.get(redis_key)
                    if cached_json:
                        data = json.loads(cached_json)
                        source_db = "Redis Database (String Cache)"
            except Exception as r_err:
                print(f"⚠️ Redis read error: {r_err}", flush=True)

        if not data:
            if db:
                try:
                    doc_ref = db.collection('ended_wars').document(doc_id)
                    doc = doc_ref.get()
                    if doc.exists:
                        data = doc.to_dict()
                        source_db = "Firebase Firestore"
                        if redis_client:
                            try:
                                await redis_client.json().set(redis_key, "$", data)
                            except Exception:
                                try:
                                    await redis_client.set(redis_key, json.dumps(data))
                                except Exception as cache_err:
                                    print(f"⚠️ Redis write-back error: {cache_err}", flush=True)
                except Exception as fb_err:
                    print(f"⚠️ Firebase Firestore query error: {fb_err}", flush=True)

        if not data:
            return f"War Document Not Found in Redis or Firebase: {doc_id}", 404

        conflict_data = data.get('conflict_data', {})
        war_meta = conflict_data.get('war_metadata', {})
        clans = conflict_data.get('clans', {})
        rosters = conflict_data.get('rosters', [])

        clan_a = clans.get('clan_a', {})
        clan_b = clans.get('clan_b', {})

        def format_attacks(attacks_list):
            if not attacks_list:
                return "<span style='color: #7f8c8d;'>No attacks</span>"
            formatted = []
            for i, att in enumerate(attacks_list, 1):
                if isinstance(att, str):
                    formatted.append(f"{i}. {att}")
                elif isinstance(att, dict):
                    formatted.append(f"{i}. Hit #{att.get('defender', '?')}: {att.get('stars', 0)}★ {att.get('destruction', 0)}%")
            return "<br>".join(formatted)

        def format_defenses(defenses_list):
            if not defenses_list:
                return "<span style='color: #c0392b; font-weight: bold;'>Pending / Untouched</span>"
            formatted = []
            for i, def_log in enumerate(defenses_list, 1):
                if isinstance(def_log, str):
                    formatted.append(f"{i}. {def_log}")
                elif isinstance(def_log, dict):
                    formatted.append(f"{i}. Hit by #{def_log.get('attacker', '?')}: {def_log.get('stars', 0)}★ {def_log.get('destruction', 0)}%")
            return "<br>".join(formatted)

        view1_rows = ""
        view2_rows = ""

        for item in rosters:
            pos = item.get('position', '-')
            ca = item.get('clan_a_member', {})
            cb = item.get('clan_b_member', {})

            ca_name = ca.get('name', 'N/A')
            ca_tag = ca.get('tag', '')
            cb_name = cb.get('name', 'N/A')
            cb_tag = cb.get('tag', '')

            ca_att_html = format_attacks(ca.get('attacks'))
            cb_def_html = format_defenses(cb.get('defenses'))
            ca_def_html = format_defenses(ca.get('defenses'))
            cb_att_html = format_attacks(cb.get('attacks'))

            view1_rows += f"""<tr>
                <td>{pos}</td>
                <td>{ca_name} (<a class="player-tag-link" data-tag="{ca_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={ca_tag}">{ca_tag}</a>) <span class="clan-status" data-tag="{ca_tag}"></span></td>
                <td>{ca_att_html}</td>
                <td class='lb'>{pos}</td>
                <td>{cb_name} (<a class="player-tag-link" data-tag="{cb_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={cb_tag}">{cb_tag}</a>) <span class="clan-status" data-tag="{cb_tag}"></span></td>
                <td>{cb_def_html}</td>
            </tr>\n"""

            view2_rows += f"""<tr>
                <td>{pos}</td>
                <td>{ca_name} (<a class="player-tag-link" data-tag="{ca_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={ca_tag}">{ca_tag}</a>) <span class="clan-status" data-tag="{ca_tag}"></span></td>
                <td>{ca_def_html}</td>
                <td class='lb'>{pos}</td>
                <td>{cb_name} (<a class="player-tag-link" data-tag="{cb_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={cb_tag}">{cb_tag}</a>) <span class="clan-status" data-tag="{cb_tag}"></span></td>
                <td>{cb_att_html}</td>
            </tr>\n"""

        full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Chocolate Clash: Viewing War</title>
    <style>
        body {{ font-family: "Times New Roman", Times, serif; font-size: 16px; background-color: #ffffff; color: #000000; margin: 0; padding: 20px; }}
        #container {{ width: 80%; margin: 20px auto; border: 1px solid black; text-align: center; padding: 15px; background-color: #ffffff; }}
        #title {{ font-weight: bold; font-size: 32px; color: #000000; }}
        #top {{ display: block; text-align: left; font-size: 16px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        td {{ border-bottom: 1px solid gray; font-size: 15px; text-align: left; padding: 5px; vertical-align: top; color: #000000; }}
        td.lb {{ border-left: 1px solid gray; }}
        a {{ text-decoration: none; color: blue; }}
        .db-badge {{ font-size: 12px; color: green; font-weight: bold; float: right; }}
        
        .toggle-container {{
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 15px;
            margin: 15px 0;
            background: #f0f3f8;
            padding: 10px;
            border-radius: 8px;
            border: 1px solid #ccd1d9;
        }}
        .toggle-btn {{
            padding: 8px 18px;
            font-size: 14px;
            font-weight: bold;
            border: 1px solid #34495e;
            background-color: #ffffff;
            color: #2c3e50;
            cursor: pointer;
            border-radius: 4px;
            transition: 0.2s;
        }}
        .toggle-btn.active {{
            background-color: #2980b9;
            color: #ffffff;
            border-color: #2980b9;
        }}
        .badge-in-clan {{
            background-color: #27ae60;
            color: #fff;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
            margin-left: 5px;
        }}
        .badge-no-clan {{
            background-color: #e74c3c;
            color: #fff;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
            margin-left: 5px;
        }}
        .nav-back-link {{
            float: left;
            font-size: 14px;
            font-weight: bold;
            color: #2980b9;
        }}
    </style>
</head>
<body>
    <div id="container">
        <a href="/" class="nav-back-link">← Home (Live Wars)</a>
        <span id="title">FWA ChocolateClash</span>
        <br><br>
        <span id="top">
            Showing details for a saved clan war.
            <span class="db-badge">Loaded from {source_db}</span><br><br>
            <b>Prep. Day Start: </b>{war_meta.get('prep_day_start', 'N/A')}<br>
            <b>Battle Day Start: </b>{war_meta.get('battle_day_start', 'N/A')}<br>
            <b>War Ends: </b>{war_meta.get('war_ends', 'N/A')}<br><br>
            <b>Status: </b>{war_meta.get('status', 'N/A')}<br>
            <b>Last Updated: </b>{war_meta.get('last_updated', 'N/A')}<br>
        </span>

        <div class="toggle-container">
            <button id="btn-view1" class="toggle-btn active" onclick="switchView(1)">
                ⚔️ View 1: {clan_a.get('name')} Attacks ➔ {clan_b.get('name')} Defenses
            </button>
            <button id="btn-view2" class="toggle-btn" onclick="switchView(2)">
                🛡️ View 2: {clan_a.get('name')} Defenses ⬅ {clan_b.get('name')} Attacks
            </button>
            <button id="btn-check-clan" class="toggle-btn" style="background-color: #27ae60; color: white;" onclick="checkClanStatus()">
                🔍 Check Live Clan Status
            </button>
        </div>

        <table>
            <tbody>
                <tr>
                    <td style="width:2%">&nbsp;</td>
                    <td colspan="2" style="width:48%">
                        <span id="header-clan-a" style="color:red; font-weight:bold; font-size:17px;">
                            {clan_a.get('name')} (Attacks)
                        </span>
                    </td>
                    <td style="width:2%">&nbsp;</td>
                    <td colspan="2" style="width:48%">
                        <span id="header-clan-b" style="color:red; font-weight:bold; font-size:17px;">
                            {clan_b.get('name')} (Bases / Defense Status)
                        </span>
                    </td>
                </tr>
                <tr>
                    <td><img style="width: 48px; height: 48px;" src="{clan_a.get('badge', 'https://api-assets.clashofclans.com/leagues/72/e--YMyIexEQQhE4imLoJcwhYn6Uy8KqlgyY3_kFV6t4.png')}" alt="Badge"></td>
                    <td colspan="2">
                        {clan_a.get('name', 'N/A')} (<a href="https://link.clashofclans.com/en/?action=OpenClanProfile&tag={clan_a.get('tag')}">{clan_a.get('tag', '')}</a>) lvl. {clan_a.get('level', 0)}<br>
                        {clan_a.get('members_count', 0)} people, {clan_a.get('stars', 0)}★ {clan_a.get('destruction_percentage', '0.0%')} {clan_a.get('attacks_used', 0)} Attacks
                    </td>
                    <td class="lb" style="width: 2%;"><img style="width: 48px; height: 48px;" src="{clan_b.get('badge', 'https://api-assets.clashofclans.com/leagues/72/e--YMyIexEQQhE4imLoJcwhYn6Uy8KqlgyY3_kFV6t4.png')}" alt="Badge"></td>
                    <td colspan="2">
                        {clan_b.get('name', 'N/A')} (<a href="https://link.clashofclans.com/en/?action=OpenClanProfile&tag={clan_b.get('tag')}">{clan_b.get('tag', '')}</a>) lvl. {clan_b.get('level', 0)}<br>
                        {clan_b.get('members_count', 0)} people, {clan_b.get('stars', 0)}★ {clan_b.get('destruction_percentage', '0.0%')} {clan_b.get('attacks_used', 0)} Attacks
                    </td>
                </tr>
            </tbody>

            <tbody id="roster-view1">
                {view1_rows}
            </tbody>

            <tbody id="roster-view2" style="display: none;">
                {view2_rows}
            </tbody>
        </table>
    </div>

    <script>
        function switchView(mode) {{
            const v1 = document.getElementById('roster-view1');
            const v2 = document.getElementById('roster-view2');
            const b1 = document.getElementById('btn-view1');
            const b2 = document.getElementById('btn-view2');
            const hA = document.getElementById('header-clan-a');
            const hB = document.getElementById('header-clan-b');

            if (mode === 1) {{
                v1.style.display = '';
                v2.style.display = 'none';
                b1.classList.add('active');
                b2.classList.remove('active');
                hA.innerText = "{clan_a.get('name')} (Attacks)";
                hB.innerText = "{clan_b.get('name')} (Bases / Defense Status)";
            }} else {{
                v1.style.display = 'none';
                v2.style.display = '';
                b2.classList.add('active');
                b1.classList.remove('active');
                hA.innerText = "{clan_a.get('name')} (Bases / Defense Status)";
                hB.innerText = "{clan_b.get('name')} (Attacks)";
            }}
        }}

        async function checkClanStatus() {{
            const btn = document.getElementById('btn-check-clan');
            btn.disabled = true;
            btn.innerText = "⏳ Checking live status...";

            const tagElements = document.querySelectorAll('.player-tag-link');
            const tagsSet = new Set();
            tagElements.forEach(el => {{
                const tag = el.getAttribute('data-tag');
                if (tag) tagsSet.add(tag);
            }});

            const tagsArray = Array.from(tagsSet);
            if (tagsArray.length === 0) {{
                btn.innerText = "❌ No Tags Found";
                return;
            }}

            try {{
                const response = await fetch('/api/check_clan_status', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ tags: tagsArray }})
                }});

                if (!response.ok) throw new Error("API check failed");
                const results = await response.json();

                document.querySelectorAll('.clan-status').forEach(span => {{
                    const tag = span.getAttribute('data-tag');
                    const info = results[tag];
                    if (info) {{
                        if (info.in_clan) {{
                            span.innerHTML = `<span class="badge-in-clan" title="${{info.clan_tag}}">🟢 In Clan: ${{info.clan_name}}</span>`;
                        }} else {{
                            span.innerHTML = `<span class="badge-no-clan">🔴 No Clan</span>`;
                        }}
                    }}
                }});

                btn.innerText = "✅ Check Complete";
            }} catch (err) {{
                console.error(err);
                btn.innerText = "❌ Verification Error";
            }} finally {{
                setTimeout(() => {{
                    btn.disabled = false;
                    btn.innerText = "🔍 Check Live Clan Status";
                }}, 4000);
            }}
        }}
    </script>
</body>
</html>"""
        return full_html, 200

    except Exception as e:
        return f"Internal Fetch Failure: {str(e)}", 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 9064))
    app.run(host="0.0.0.0", port=port)
