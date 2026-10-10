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

# --- Reusable Raboy's Flagship Dark Theme CSS ---
THEME_CSS = """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;700&display=swap');
        
        :root {
            --canvas: #0F141B; --raised: #171D26; --raised-high: #1D2632; --inset: #0B1016;
            --outline: #34404F; --outline-soft: #283340;
            --text: #F3F5F7; --text-secondary: #B5BDC9; --text-muted: #8B95A5;
            --gold: #D6B15E; --success: #68C79B; --warning: #E7A85F; --error: #EE8790; --info: #81AFE8;
            --gold-hover: #e0c27c;
            --font-sans: 'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif;
            --font-mono: 'IBM Plex Mono', ui-monospace, 'Cascadia Mono', Consolas, monospace;
            --r-control: 10px; --r-card: 16px; --r-pill: 999px;
        }
        
        body { font-family: var(--font-sans); font-size: 15px; background-color: var(--canvas); color: var(--text); margin: 0; -webkit-font-smoothing: antialiased; }
        a { color: var(--info); text-decoration: none; }
        a:hover { text-decoration: underline; }
        
        /* Navigation Bar */
        .bar { display: flex; align-items: center; padding: 0 40px; height: 60px; background: var(--canvas); border-bottom: 1px solid var(--outline-soft); }
        .brand { color: var(--text); font-size: 18px; font-weight: 500; display: flex; align-items: center; gap: 8px; }
        .brand small { color: var(--text-muted); font-size: 13px; font-weight: 400; }
        
        /* Layout */
        .container { width: 90%; max-width: 1200px; margin: 40px auto; }
        .ui-panel { padding: 20px; border: 1px solid var(--outline); border-radius: var(--r-card); background: var(--raised); margin-bottom: 20px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); }
        .ui-title { color: var(--gold); font-size: 20px; font-weight: 700; margin: 0 0 10px 0; }
        .ui-lead { margin-top: 0; color: var(--text-secondary); font-size: 15px; }
        
        /* Tables */
        table { width: 100%; border-collapse: collapse; margin-top: 15px; background: var(--raised); border-radius: var(--r-card); overflow: hidden; border-style: hidden; box-shadow: 0 0 0 1px var(--outline-soft); }
        th { border-bottom: 2px solid var(--outline); padding: 14px; font-size: 13px; text-align: left; background-color: var(--raised-high); color: var(--text-muted); font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em; }
        td { border-bottom: 1px solid var(--outline-soft); font-size: 14px; text-align: left; padding: 14px; vertical-align: middle; color: var(--text); }
        td.mono { font-family: var(--font-mono); color: var(--text-secondary); }
        td.lb { border-left: 1px solid var(--outline-soft); }
        
        /* Buttons */
        .btn { display: inline-flex; align-items: center; justify-content: center; gap: 8px; min-height: 40px; padding: 0 16px; border-radius: var(--r-control); border: none; background: var(--gold); color: var(--inset); font-weight: 700; font-size: 14px; text-decoration: none; cursor: pointer; transition: transform 0.12s var(--ease-out, ease-out), background 0.15s; }
        .btn:hover { background: var(--gold-hover); text-decoration: none; transform: scale(0.975); }
        .btn-outline { background: transparent; border: 1px solid var(--outline); color: var(--text-secondary); }
        .btn-outline.active { border-color: var(--gold); background: var(--raised-high); color: var(--text); }
        .btn-success { background: var(--success); color: var(--inset); }
        
        /* Badges & specific elements */
        .badge { padding: 4px 8px; border-radius: var(--r-pill); font-size: 11px; font-weight: 700; font-family: var(--font-mono); display: inline-block; }
        .badge-success { background: rgba(104, 199, 155, 0.15); color: var(--success); border: 1px solid var(--success); }
        .badge-error { background: rgba(238, 135, 144, 0.15); color: var(--error); border: 1px solid var(--error); }
        .db-badge { float: right; font-size: 12px; color: var(--success); font-weight: bold; border: 1px solid var(--success); padding: 4px 10px; border-radius: var(--r-pill); background: rgba(104, 199, 155, 0.1); }
        .toggle-container { display: flex; flex-wrap: wrap; justify-content: flex-start; align-items: center; gap: 15px; margin: 20px 0; padding: 15px; border-radius: var(--r-card); background: var(--raised); border: 1px solid var(--outline-soft); }
        .nav-back { color: var(--text-muted); font-size: 14px; font-weight: 500; display: inline-block; margin-bottom: 20px; }
        .nav-back:hover { color: var(--text); }
        
        /* Helper Utilities */
        .text-success { color: var(--success); font-weight: 700; }
        .text-error { color: var(--error); font-weight: 700; }
        .text-muted { color: var(--text-muted); font-size: 12px; }
    </style>
"""

@app.route('/')
@app.route('/live_wars')
async def home():
    """Homepage: Displays all active/cached war records stored in Redis using the new dark layout."""
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
            <tr>
                <td class="mono">{war['doc_id']}</td>
                <td><strong>{war['clan_a_name']}</strong><br><span class="text-muted">{war['clan_a_tag']}</span></td>
                <td><strong>{war['clan_b_name']}</strong><br><span class="text-muted">{war['clan_b_tag']}</span></td>
                <td><span class="text-success">{war['status']}</span></td>
                <td class="mono">{war['war_ends']}</td>
                <td><a href="/war_conflicts?{war['doc_id']}" class="btn">Inspect Match</a></td>
            </tr>
            """

        if not rows_html:
            rows_html = "<tr><td colspan='6' style='text-align:center; padding: 30px; color: var(--text-muted);'>No live wars currently cached in Redis.</td></tr>"

        full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chocolate Clash: Live Redis Wars</title>
    {THEME_CSS}
</head>
<body>
    <header class="bar">
        <a class="brand" href="/">Chocolate Clash <small>by FWA</small></a>
    </header>
    
    <div class="container">
        <div class="ui-panel">
            <h1 class="ui-title">⚡ Live Redis War Records</h1>
            <p class="ui-lead">Showing active match data currently loaded in memory.</p>
        </div>

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
                return "<span class='text-muted'>No attacks</span>"
            formatted = []
            for i, att in enumerate(attacks_list, 1):
                if isinstance(att, str):
                    formatted.append(f"{i}. {att}")
                elif isinstance(att, dict):
                    formatted.append(f"{i}. Hit #{att.get('defender', '?')}: {att.get('stars', 0)}★ {att.get('destruction', 0)}%")
            return "<br>".join(formatted)

        def format_defenses(defenses_list):
            if not defenses_list:
                return "<span class='text-error'>Pending / Untouched</span>"
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
                <td class="mono">{pos}</td>
                <td><strong>{ca_name}</strong> (<a class="player-tag-link" data-tag="{ca_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={ca_tag}">{ca_tag}</a>) <span class="clan-status" data-tag="{ca_tag}"></span></td>
                <td>{ca_att_html}</td>
                <td class='lb mono'>{pos}</td>
                <td><strong>{cb_name}</strong> (<a class="player-tag-link" data-tag="{cb_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={cb_tag}">{cb_tag}</a>) <span class="clan-status" data-tag="{cb_tag}"></span></td>
                <td>{cb_def_html}</td>
            </tr>\n"""

            # Mode 2 (Swapped)
            view2_rows += f"""<tr>
                <td class="mono">{pos}</td>
                <td><strong>{ca_name}</strong> (<a class="player-tag-link" data-tag="{ca_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={ca_tag}">{ca_tag}</a>) <span class="clan-status" data-tag="{ca_tag}"></span></td>
                <td>{ca_def_html}</td>
                <td class='lb mono'>{pos}</td>
                <td><strong>{cb_name}</strong> (<a class="player-tag-link" data-tag="{cb_tag}" href="https://link.clashofclans.com/en/?action=OpenPlayerProfile&tag={cb_tag}">{cb_tag}</a>) <span class="clan-status" data-tag="{cb_tag}"></span></td>
                <td>{cb_att_html}</td>
            </tr>\n"""

        full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chocolate Clash: Viewing War</title>
    {THEME_CSS}
</head>
<body>
    <header class="bar">
        <a class="brand" href="/">Chocolate Clash <small>by FWA</small></a>
    </header>

    <div class="container">
        <a href="/" class="nav-back">← Back to Live Wars</a>
        
        <div class="ui-panel" style="position: relative;">
            <span class="db-badge">Loaded from {source_db}</span>
            <h1 class="ui-title">Saved Clan War Details</h1>
            <p class="ui-lead" style="margin-bottom: 5px;"><strong>Prep. Day Start:</strong> <span class="mono">{war_meta.get('prep_day_start', 'N/A')}</span></p>
            <p class="ui-lead" style="margin-bottom: 5px;"><strong>Battle Day Start:</strong> <span class="mono">{war_meta.get('battle_day_start', 'N/A')}</span></p>
            <p class="ui-lead" style="margin-bottom: 5px;"><strong>War Ends:</strong> <span class="mono">{war_meta.get('war_ends', 'N/A')}</span></p>
            <p class="ui-lead" style="margin-bottom: 5px;"><strong>Status:</strong> <span class="text-success">{war_meta.get('status', 'N/A')}</span></p>
            <p class="ui-lead"><strong>Last Updated:</strong> <span class="mono">{war_meta.get('last_updated', 'N/A')}</span></p>
        </div>

        <!-- View Switcher & Clan Check Button -->
        <div class="toggle-container">
            <button id="btn-view1" class="btn btn-outline active" onclick="switchView(1)">
                ⚔️ {clan_a.get('name')} Attacks ➔ {clan_b.get('name')} Defenses
            </button>
            <button id="btn-view2" class="btn btn-outline" onclick="switchView(2)">
                🛡️ {clan_a.get('name')} Defenses ⬅ {clan_b.get('name')} Attacks
            </button>
            <button id="btn-check-clan" class="btn btn-success" style="margin-left: auto;" onclick="checkClanStatus()">
                🔍 Check Live Clan Status
            </button>
        </div>

        <table>
            <tbody>
                <tr>
                    <td style="width:2%; border-bottom:none;">&nbsp;</td>
                    <td colspan="2" style="width:48%; border-bottom:none;">
                        <span id="header-clan-a" style="color:var(--gold); font-weight:700; font-size:16px;">
                            {clan_a.get('name')} (Attacks)
                        </span>
                    </td>
                    <td style="width:2%; border-bottom:none;" class="lb">&nbsp;</td>
                    <td colspan="2" style="width:48%; border-bottom:none;">
                        <span id="header-clan-b" style="color:var(--gold); font-weight:700; font-size:16px;">
                            {clan_b.get('name')} (Bases / Defense Status)
                        </span>
                    </td>
                </tr>
                <tr>
                    <td><img style="width: 48px; height: 48px; border-radius: 8px;" src="{clan_a.get('badge', 'https://api-assets.clashofclans.com/leagues/72/e--YMyIexEQQhE4imLoJcwhYn6Uy8KqlgyY3_kFV6t4.png')}" alt="Badge"></td>
                    <td colspan="2" style="line-height: 1.6;">
                        <strong>{clan_a.get('name', 'N/A')}</strong> (<a href="https://link.clashofclans.com/en/?action=OpenClanProfile&tag={clan_a.get('tag')}">{clan_a.get('tag', '')}</a>) lvl. {clan_a.get('level', 0)}<br>
                        <span class="text-muted">{clan_a.get('members_count', 0)} people, {clan_a.get('stars', 0)}★ {clan_a.get('destruction_percentage', '0.0%')} {clan_a.get('attacks_used', 0)} Attacks</span>
                    </td>
                    <td class="lb"><img style="width: 48px; height: 48px; border-radius: 8px;" src="{clan_b.get('badge', 'https://api-assets.clashofclans.com/leagues/72/e--YMyIexEQQhE4imLoJcwhYn6Uy8KqlgyY3_kFV6t4.png')}" alt="Badge"></td>
                    <td colspan="2" style="line-height: 1.6;">
                        <strong>{clan_b.get('name', 'N/A')}</strong> (<a href="https://link.clashofclans.com/en/?action=OpenClanProfile&tag={clan_b.get('tag')}">{clan_b.get('tag', '')}</a>) lvl. {clan_b.get('level', 0)}<br>
                        <span class="text-muted">{clan_b.get('members_count', 0)} people, {clan_b.get('stars', 0)}★ {clan_b.get('destruction_percentage', '0.0%')} {clan_b.get('attacks_used', 0)} Attacks</span>
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
                            span.innerHTML = `<span class="badge badge-success" title="${{info.clan_tag}}">🟢 In Clan: ${{info.clan_name}}</span>`;
                        }} else {{
                            span.innerHTML = `<span class="badge badge-error">🔴 No Clan</span>`;
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
