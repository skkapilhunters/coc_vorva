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
    print("✅ Connected to Redis successfully.")
except Exception as e:
    print(f"⚠️ Error connecting to Redis: {e}")
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
        print("✅ Connected to Firebase successfully.")
    except Exception as e:
        print(f"⚠️ Error parsing or connecting to Firebase: {e}")
else:
    print("⚠️ WARNING: FIREBASE_CREDENTIALS environment variable is missing or empty!")

START_TIME = time.time()
ACTIVE_TOKENS = {}

async def fetch_coc_player(player_tag):
    """Fetch player info using COC_KEYS with automatic fallback on failure/rate-limit."""
    if not COC_KEYS:
        return None

    clean_tag = player_tag.strip().upper()
    if not clean_tag.startswith("#"):
        clean_tag = f"#{clean_tag}"

    encoded_tag = urllib.parse.quote(clean_tag)
    url = f"https://api.clashofclans.com/v1/players/{encoded_tag}"

    async with aiohttp.ClientSession() as session:
        for key in COC_KEYS:
            headers = {
                "Authorization": f"Bearer {key}",
                "Accept": "application/json"
            }
            try:
                async with session.get(url, headers=headers, timeout=5) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status in (403, 429):
                        continue
                    elif resp.status == 404:
                        return None
            except Exception:
                continue
    return None

def get_base_html(title, content):
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
        <style>
            :root {{
                --bg-color: #0f111a; 
                --card-bg: #1e2235; 
                --card-header: #262b42;
                --accent-color: #4e73df;
                --accent-hover: #3b5998;
                --success-color: #2ecc71; 
                --text-color: #f8f9fc; 
                --text-muted: #a0aec0; 
                --danger-color: #e74c3c;
                --border-color: #2d3748;
            }}
            body {{ 
                font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif; 
                background-color: var(--bg-color); 
                color: var(--text-color); 
                margin: 0; 
                padding: 20px 0; 
                display: flex; 
                justify-content: center; 
                align-items: center; 
                min-height: 100vh; 
                box-sizing: border-box;
            }}
            .container {{ width: 100%; max-width: 1050px; padding: 0 20px; box-sizing: border-box; }}
            .profile-card {{ 
                background: var(--card-bg); 
                border-radius: 16px; 
                padding: 30px; 
                text-align: center; 
                box-shadow: 0 10px 30px rgba(0,0,0,0.5); 
                margin-bottom: 24px; 
                border: 1px solid var(--border-color);
            }}
            .status-badge {{ 
                display: inline-flex; 
                align-items: center; 
                background: rgba(46, 204, 113, 0.1); 
                color: var(--success-color); 
                padding: 6px 16px; 
                border-radius: 20px; 
                font-weight: 600; 
            }}
            .status-dot {{ width: 8px; height: 8px; background-color: var(--success-color); border-radius: 50%; margin-right: 8px; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 20px; }}
            .stat-card {{ background: var(--card-bg); border-radius: 12px; padding: 20px; text-align: center; border: 1px solid var(--border-color); }}
            .stat-value {{ font-size: 1.6rem; font-weight: bold; }}
            .stat-label {{ font-size: 0.85rem; color: var(--text-muted); text-transform: uppercase; }}
            .nav-btn {{ 
                display: inline-block; 
                background-color: var(--accent-color); 
                color: white; 
                text-decoration: none; 
                padding: 12px 24px; 
                font-size: 15px; 
                font-weight: bold; 
                border-radius: 8px; 
                margin-top: 15px; 
                transition: 0.2s; 
            }}
            .nav-btn:hover {{ background-color: var(--accent-hover); }}
            footer {{ text-align: center; margin-top: 30px; font-size: 0.8rem; color: var(--text-muted); }}
        </style>
    </head>
    <body>
        <div class="container">{content}<footer>Powered by Quart & Standalone Async Architecture</footer></div>
    </body>
    </html>
    """

@app.route('/')
async def home():
    uptime_seconds = int(time.time() - START_TIME)
    uptime_hours, uptime_mins = uptime_seconds // 3600, (uptime_seconds % 3600) // 60

    homepage_content = f"""
    <div class="profile-card">
        <h1>Clan War Tracker Dashboard</h1>
        <div class="status-badge"><span class="status-dot"></span>ONLINE & OPERATIONAL (Redis + Firebase Active)</div>
        <br>
        <a href="/live_wars" class="nav-btn">🚀 View All Live Wars (Redis Cache)</a>
    </div>
    <div class="stats-grid">
        <div class="stat-card"><div class="stat-value">{uptime_hours}h {uptime_mins}m</div><div class="stat-label">Uptime</div></div>
    </div>
    """
    return get_base_html("Clan War Tracker - Dashboard", homepage_content)

@app.route('/live_wars', methods=['GET'])
async def live_wars():
    """Displays all active/cached war records currently stored in Redis."""
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
                print(f"⚠️ Error reading key {key}: {e}")

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
            <tr style="border-bottom: 1px solid var(--border-color);">
                <td style="padding: 14px 12px; font-family: monospace; color: var(--text-muted);">{war['doc_id']}</td>
                <td style="padding: 14px 12px;"><b>{war['clan_a_name']}</b><br><span style="font-size:0.8rem; color: var(--text-muted);">{war['clan_a_tag']}</span></td>
                <td style="padding: 14px 12px;"><b>{war['clan_b_name']}</b><br><span style="font-size:0.8rem; color: var(--text-muted);">{war['clan_b_tag']}</span></td>
                <td style="padding: 14px 12px;"><span style="background: rgba(46,204,113,0.15); color: #2ecc71; padding: 4px 12px; border-radius: 12px; font-size: 0.85rem; font-weight: bold;">{war['status']}</span></td>
                <td style="padding: 14px 12px; font-size: 0.9rem; color: var(--text-muted);">{war['war_ends']}</td>
                <td style="padding: 14px 12px;"><a href="/war_conflicts?{war['doc_id']}" style="background-color: var(--accent-color); color: white; padding: 8px 16px; border-radius: 6px; text-decoration: none; font-size: 0.85rem; font-weight: bold;">Inspect Match</a></td>
            </tr>
            """

        if not rows_html:
            rows_html = "<tr><td colspan='6' style='text-align:center; padding: 30px; color: var(--text-muted);'>No live wars currently cached in Redis.</td></tr>"

        page_content = f"""
        <div style="background: var(--card-bg); border-radius: 16px; padding: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); border: 1px solid var(--border-color);">
            <h1 style="text-align: center; margin-bottom: 8px;">⚡ Live Redis War Records</h1>
            <p style="text-align: center; color: var(--text-muted); margin-bottom: 25px;">Real-time match data currently loaded from memory.</p>
            
            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse; text-align: left;">
                    <thead>
                        <tr style="border-bottom: 2px solid var(--accent-color); color: var(--text-muted); font-size: 0.8rem; text-transform: uppercase;">
                            <th style="padding: 12px;">ID</th>
                            <th style="padding: 12px;">Clan A</th>
                            <th style="padding: 12px;">Opponent (Clan B)</th>
                            <th style="padding: 12px;">Status</th>
                            <th style="padding: 12px;">War Ends</th>
                            <th style="padding: 12px;">Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
            
            <div style="text-align: center; margin-top: 25px;">
                <a href="/" style="color: var(--accent-color); text-decoration: none; font-weight: bold;">← Back to Dashboard</a>
            </div>
        </div>
        """
        return get_base_html("Live Wars - Redis Cache", page_content), 200

    except Exception as e:
        return f"Internal Error loading live wars: {str(e)}", 500

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

        # 1. Try fetching from Redis safely according to Key Type
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
                print(f"⚠️ Redis read error: {r_err}")

        # 2. Fallback to Firebase Firestore if missing in Redis
        if not data:
            if db:
                try:
                    doc_ref = db.collection('ended_wars').document(doc_id)
                    doc = doc_ref.get()
                    if doc.exists:
                        data = doc.to_dict()
                        source_db = "Firebase Firestore"
                        
                        # Save back to Redis cache
                        if redis_client:
                            try:
                                await redis_client.json().set(redis_key, "$", data)
                            except Exception:
                                try:
                                    await redis_client.set(redis_key, json.dumps(data))
                                except Exception as cache_err:
                                    print(f"⚠️ Redis write-back error: {cache_err}")
                except Exception as fb_err:
                    print(f"⚠️ Firebase Firestore query error: {fb_err}")

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
                return "<span style='color: var(--text-muted);'>No attacks</span>"
            formatted = []
            for i, att in enumerate(attacks_list, 1):
                if isinstance(att, str):
                    formatted.append(f"{i}. {att}")
                elif isinstance(att, dict):
                    formatted.append(f"{i}. Hit #{att.get('defender', '?')}: {att.get('stars', 0)}★ {att.get('destruction', 0)}%")
            return "<br>".join(formatted)

        def format_defenses(defenses_list):
            if not defenses_list:
                return "<span style='color: var(--danger-color); font-weight: bold;'>Pending / Untouched</span>"
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

            # Mode 1
            view1_rows += f"""<tr>
                <td style='text-align: center; color: var(--text-muted);'>{pos}</td>
                <td><b style='color: var(--text-color);'>{ca_name}</b> (<a class="player-tag-link" data-tag="{ca_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={ca_tag}">{ca_tag}</a>) <span class="clan-status" data-tag="{ca_tag}"></span></td>
                <td>{ca_att_html}</td>
                <td class='lb' style='text-align: center; color: var(--text-muted);'>{pos}</td>
                <td><b style='color: var(--text-color);'>{cb_name}</b> (<a class="player-tag-link" data-tag="{cb_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={cb_tag}">{cb_tag}</a>) <span class="clan-status" data-tag="{cb_tag}"></span></td>
                <td>{cb_def_html}</td>
            </tr>\n"""

            # Mode 2 (Swapped)
            view2_rows += f"""<tr>
                <td style='text-align: center; color: var(--text-muted);'>{pos}</td>
                <td><b style='color: var(--text-color);'>{ca_name}</b> (<a class="player-tag-link" data-tag="{ca_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={ca_tag}">{ca_tag}</a>) <span class="clan-status" data-tag="{ca_tag}"></span></td>
                <td>{ca_def_html}</td>
                <td class='lb' style='text-align: center; color: var(--text-muted);'>{pos}</td>
                <td><b style='color: var(--text-color);'>{cb_name}</b> (<a class="player-tag-link" data-tag="{cb_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={cb_tag}">{cb_tag}</a>) <span class="clan-status" data-tag="{cb_tag}"></span></td>
                <td>{cb_att_html}</td>
            </tr>\n"""

        full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chocolate Clash: Viewing War</title>
    <style>
        :root {{
            --bg-color: #0f111a;
            --card-bg: #1e2235;
            --accent-color: #4e73df;
            --success-color: #2ecc71;
            --text-color: #f8f9fc;
            --text-muted: #a0aec0;
            --danger-color: #e74c3c;
            --border-color: #2d3748;
        }}
        body {{ 
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif; 
            font-size: 15px; 
            background-color: var(--bg-color); 
            color: var(--text-color); 
            margin: 0; 
            padding: 20px; 
        }}
        #container {{ 
            width: 95%; 
            max-width: 1200px; 
            margin: 20px auto; 
            border: 1px solid var(--border-color); 
            border-radius: 16px;
            padding: 25px; 
            background-color: var(--card-bg); 
            box-shadow: 0 10px 30px rgba(0,0,0,0.5);
            box-sizing: border-box;
        }}
        #title {{ 
            font-weight: 800; 
            font-size: 30px; 
            color: var(--text-color); 
            text-align: center;
            display: block;
        }}
        #top {{ 
            display: block; 
            text-align: left; 
            font-size: 15px; 
            background: rgba(0,0,0,0.2);
            padding: 15px 20px;
            border-radius: 10px;
            margin-top: 15px;
            border: 1px solid var(--border-color);
            line-height: 1.6;
        }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        td {{ border-bottom: 1px solid var(--border-color); font-size: 14px; text-align: left; padding: 10px 8px; vertical-align: top; color: var(--text-color); }}
        td.lb {{ border-left: 1px solid var(--border-color); }}
        a {{ text-decoration: none; color: #64b5f6; font-weight: bold; }}
        a:hover {{ text-decoration: underline; }}
        .db-badge {{ font-size: 13px; color: var(--success-color); font-weight: bold; float: right; background: rgba(46,204,113,0.1); padding: 4px 10px; border-radius: 6px; }}
        
        .toggle-container {{
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 12px;
            margin: 20px 0;
            background: rgba(0,0,0,0.25);
            padding: 12px;
            border-radius: 10px;
            border: 1px solid var(--border-color);
            flex-wrap: wrap;
        }}
        .toggle-btn {{
            padding: 10px 18px;
            font-size: 14px;
            font-weight: bold;
            border: 1px solid var(--border-color);
            background-color: #121420;
            color: var(--text-muted);
            cursor: pointer;
            border-radius: 6px;
            transition: 0.2s;
        }}
        .toggle-btn.active {{
            background-color: var(--accent-color);
            color: #ffffff;
            border-color: var(--accent-color);
        }}
        .badge-in-clan {{
            background-color: var(--success-color);
            color: #fff;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
            margin-left: 5px;
        }}
        .badge-no-clan {{
            background-color: var(--danger-color);
            color: #fff;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
            margin-left: 5px;
        }}
        .nav-back-link {{
            display: inline-block;
            margin-bottom: 15px;
            color: var(--accent-color);
            text-decoration: none;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div id="container">
        <a href="/live_wars" class="nav-back-link">← Back to Live Wars</a>
        <span id="title">⚡ FWA ChocolateClash</span>
        
        <div id="top">
            <span class="db-badge">Loaded from {source_db}</span>
            <b>Prep. Day Start: </b>{war_meta.get('prep_day_start', 'N/A')}<br>
            <b>Battle Day Start: </b>{war_meta.get('battle_day_start', 'N/A')}<br>
            <b>War Ends: </b>{war_meta.get('war_ends', 'N/A')}<br><br>
            <b>Status: </b><span style="color: var(--success-color); font-weight: bold;">{war_meta.get('status', 'N/A')}</span><br>
            <b>Last Updated: </b>{war_meta.get('last_updated', 'N/A')}<br>
        </div>

        <!-- View Switcher & Clan Check Button -->
        <div class="toggle-container">
            <button id="btn-view1" class="toggle-btn active" onclick="switchView(1)">
                ⚔️ View 1: {clan_a.get('name')} Attacks ➔ {clan_b.get('name')} Defenses
            </button>
            <button id="btn-view2" class="toggle-btn" onclick="switchView(2)">
                🛡️ View 2: {clan_a.get('name')} Defenses ⬅ {clan_b.get('name')} Attacks
            </button>
            <button id="btn-check-clan" class="toggle-btn" style="background-color: var(--success-color); color: white;" onclick="checkClanStatus()">
                🔍 Check Live Clan Status
            </button>
        </div>

        <table>
            <tbody>
                <tr style="background: rgba(255,255,255,0.03);">
                    <td style="width:2%">&nbsp;</td>
                    <td colspan="2" style="width:48%">
                        <span id="header-clan-a" style="color: #ff5252; font-weight:bold; font-size:17px;">
                            {clan_a.get('name')} (Attacks)
                        </span>
                    </td>
                    <td style="width:2%">&nbsp;</td>
                    <td colspan="2" style="width:48%">
                        <span id="header-clan-b" style="color: #ff5252; font-weight:bold; font-size:17px;">
                            {clan_b.get('name')} (Bases / Defense Status)
                        </span>
                    </td>
                </tr>
                <tr>
                    <td><img style="width: 48px; height: 48px;" src="{clan_a.get('badge', 'https://api-assets.clashofclans.com/leagues/72/e--YMyIexEQQhE4imLoJcwhYn6Uy8KqlgyY3_kFV6t4.png')}" alt="Badge"></td>
                    <td colspan="2">
                        <b>{clan_a.get('name', 'N/A')}</b> (<a href="https://link.clashofclans.com/en/?action=OpenClanProfile&tag={clan_a.get('tag')}">{clan_a.get('tag', '')}</a>) lvl. {clan_a.get('level', 0)}<br>
                        <span style="color: var(--text-muted);">{clan_a.get('members_count', 0)} people, {clan_a.get('stars', 0)}★ {clan_a.get('destruction_percentage', '0.0%')} {clan_a.get('attacks_used', 0)} Attacks</span>
                    </td>
                    <td class="lb" style="width: 2%;"><img style="width: 48px; height: 48px;" src="{clan_b.get('badge', 'https://api-assets.clashofclans.com/leagues/72/e--YMyIexEQQhE4imLoJcwhYn6Uy8KqlgyY3_kFV6t4.png')}" alt="Badge"></td>
                    <td colspan="2">
                        <b>{clan_b.get('name', 'N/A')}</b> (<a href="https://link.clashofclans.com/en/?action=OpenClanProfile&tag={clan_b.get('tag')}">{clan_b.get('tag', '')}</a>) lvl. {clan_b.get('level', 0)}<br>
                        <span style="color: var(--text-muted);">{clan_b.get('members_count', 0)} people, {clan_b.get('stars', 0)}★ {clan_b.get('destruction_percentage', '0.0%')} {clan_b.get('attacks_used', 0)} Attacks</span>
                    </td>
                </tr>
            </tbody>

            <!-- Mode 1 Roster -->
            <tbody id="roster-view1">
                {view1_rows}
            </tbody>

            <!-- Mode 2 Roster -->
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
