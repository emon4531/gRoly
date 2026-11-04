# -*- coding: utf-8 -*-
"""Guild Glory Bot — Full dashboard with live glory tracking"""
import asyncio, json, os, time, hashlib, secrets
from typing import Dict, List, Any, Optional
from aiohttp import web
import urllib3, requests, httpx, jwt
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import ReqCLan_pb2, encode_id_clan_pb2, data_pb2, output_pb2
from google.protobuf.json_format import MessageToDict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ELITE_ARENA_DIR = os.path.join(BASE_DIR, "ELITE_ARENA")
OB = "OB55"
KEY = bytes([89,103,38,116,99,37,68,69,117,104,54,37,90,99,94,56])
IV  = bytes([54,111,121,90,68,114,50,50,69,51,121,99,104,106,77,37])

GAME_HEADERS = {
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9)",
    "Connection": "Keep-Alive", "Accept-Encoding": "gzip",
    "Content-Type": "application/octet-stream",
    "X-Unity-Version": "2018.4.11f1", "X-GA": "v1 1",
    "ReleaseVersion": OB, "Expect": "100-continue"
}

REGION_SERVER_MAP = {
    "IND": ("https://client.ind.freefiremobile.com", "client.ind.freefiremobile.com"),
    "ME":  ("https://clientbp.ggblueshark.com", "clientbp.ggblueshark.com"),
    "VN":  ("https://clientbp.ggpolarbear.com", "clientbp.ggpolarbear.com"),
    "BD":  ("https://clientbp.ggwhitehawk.com", "clientbp.ggwhitehawk.com"),
    "PK":  ("https://clientbp.ggblueshark.com", "clientbp.ggblueshark.com"),
    "SG":  ("https://clientbp.ggpolarbear.com", "clientbp.ggpolarbear.com"),
    "BR":  ("https://client.us.freefiremobile.com", "client.us.freefiremobile.com"),
    "NA":  ("https://client.us.freefiremobile.com", "client.us.freefiremobile.com"),
    "ID":  ("https://clientbp.ggpolarbear.com", "clientbp.ggpolarbear.com"),
    "RU":  ("https://clientbp.ggpolarbear.com", "clientbp.ggpolarbear.com"),
    "TH":  ("https://clientbp.ggpolarbear.com", "clientbp.ggpolarbear.com"),
    "SAC": ("https://client.br.freefiremobile.com", "client.br.freefiremobile.com"),
    "TW":  ("https://client.tw.freefiremobile.com", "client.tw.freefiremobile.com"),
    "CIS": ("https://client.cis.freefiremobile.com", "client.cis.freefiremobile.com"),
}
DEFAULT_SERVER_URL = "https://clientbp.ggblueshark.com"
DEFAULT_HOST = "clientbp.ggblueshark.com"

def encrypt_payload(data):
    return AES.new(KEY, AES.MODE_CBC, IV).encrypt(pad(data, AES.block_size))

def get_server_info(region):
    val = REGION_SERVER_MAP.get(str(region).upper(), DEFAULT_SERVER_URL)
    return val if isinstance(val, tuple) else (val, DEFAULT_HOST)

def get_server_url(region): return get_server_info(region)[0]

# ─────────── GLORY TRACKING ───────────
GUILD_HISTORY_FILE = "guild_history.json"
_guild_history: Dict[str, List[dict]] = {}   # {clan_id: [{t, xp}]}
_match_history: List[dict] = []              # [{t, squad_id, clan_id, members, duration}]
_session_start = time.time()
_last_poll: Dict[str, float] = {}

def _load_guild_history():
    global _guild_history
    if os.path.exists(GUILD_HISTORY_FILE):
        try:
            with open(GUILD_HISTORY_FILE, "r") as f:
                _guild_history = json.load(f)
        except Exception: _guild_history = {}

def _save_guild_history():
    try:
        with open(GUILD_HISTORY_FILE, "w") as f:
            json.dump(_guild_history, f)
    except Exception: pass

def track_guild_xp(clan_id: str, xp: int):
    cid = str(clan_id)
    if cid not in _guild_history: _guild_history[cid] = []
    hist = _guild_history[cid]
    # throttle: only add if 30s+ since last OR xp changed
    if hist and hist[-1]["xp"] == xp and time.time() - hist[-1]["t"] < 30:
        return
    hist.append({"t": int(time.time()), "xp": int(xp)})
    _guild_history[cid] = hist[-500:]
    _save_guild_history()

def get_glory_stats(clan_id: str) -> dict:
    cid = str(clan_id)
    hist = _guild_history.get(cid, [])
    if len(hist) < 1:
        return {"current_xp": 0, "baseline_xp": 0, "gained_total": 0,
                "gained_1h": 0, "gained_15m": 0, "gained_5m": 0,
                "rate_per_hour": 0, "samples": 0}
    now = time.time()
    latest = hist[-1]["xp"]
    first = hist[0]["xp"]
    def window(sec):
        pts = [h for h in hist if now - h["t"] <= sec]
        return latest - pts[0]["xp"] if pts else 0
    g_1h = window(3600); g_15m = window(900); g_5m = window(300)
    # rate from 15m window
    pts_15 = [h for h in hist if now - h["t"] <= 900]
    if len(pts_15) >= 2:
        dt_h = max((pts_15[-1]["t"] - pts_15[0]["t"]) / 3600.0, 0.01)
        rate = int((latest - pts_15[0]["xp"]) / dt_h)
    else:
        rate = 0
    return {"current_xp": latest, "baseline_xp": first,
            "gained_total": latest - first, "gained_1h": g_1h,
            "gained_15m": g_15m, "gained_5m": g_5m,
            "rate_per_hour": rate, "samples": len(hist)}

def record_match(squad_id: str, clan_id: str, members: List[str], duration: float):
    _match_history.append({"t": int(time.time()), "squad_id": squad_id,
                           "clan_id": str(clan_id), "members": members,
                           "duration": int(duration)})
    _match_history[:] = _match_history[-200:]

async def fetch_clan_xp_async(clan_id: str, region: str, token_cache: dict) -> Optional[int]:
    server_base, host = get_server_info(region)
    url = f"{server_base}/GetClanInfoByClanID"
    try:
        my_data = encode_id_clan_pb2.MyData()
        my_data.field1 = int(clan_id); my_data.field2 = 1
        payload = encrypt_payload(my_data.SerializeToString())
    except Exception: return None
    for uid, creds in token_cache.items():
        token = creds.get("token")
        if not token: continue
        headers = GAME_HEADERS.copy()
        headers["Authorization"] = f"Bearer {token}" if not token.startswith("Bearer ") else token
        headers["Host"] = host
        try:
            async with httpx.AsyncClient(verify=False, timeout=8.0) as c:
                r = await c.post(url, headers=headers, content=payload)
                if r.status_code == 200:
                    resp = data_pb2.response()
                    resp.ParseFromString(r.content)
                    xp = 0
                    for fd, val in resp.ListFields():
                        if fd.number == 36 and isinstance(val, int): xp = val; break
                    return xp
        except Exception: continue
    return None

async def _guild_xp_poller():
    """Every 60s, poll guild XP for all tracked clans."""
    _load_guild_history()
    while True:
        await asyncio.sleep(60)
        try:
            with open("token_cache.json", "r") as f:
                tc = json.load(f)
            if not tc: continue
            for cid in list(_guild_history.keys()):
                try:
                    # guess region from any token
                    for _, cred in tc.items():
                        xp = await fetch_clan_xp_async(cid, cred.get("region", "IND"), tc)
                        if xp is not None and xp > 0:
                            track_guild_xp(cid, xp); break
                except Exception: pass
        except Exception: pass

# ─────────── CLAN INFO (for lookup) ───────────
def fetch_single_clan(clan_id, region, token_cache):
    server_base, host = get_server_info(region)
    url = f"{server_base}/GetClanInfoByClanID"
    try:
        my_data = encode_id_clan_pb2.MyData()
        my_data.field1 = int(clan_id); my_data.field2 = 1
        payload = encrypt_payload(my_data.SerializeToString())
    except Exception as e:
        return {"clan_id": clan_id, "error": f"Protobuf: {e}"}

    response = None
    for uid, creds in token_cache.items():
        token = creds.get('token')
        if not token: continue
        headers = GAME_HEADERS.copy()
        headers["Authorization"] = f"Bearer {token}" if not token.startswith("Bearer ") else token
        headers["Host"] = host
        try:
            res = requests.post(url, headers=headers, data=payload, verify=False, timeout=15.0)
            if res.status_code == 200: response = res; break
            elif "expired" in res.text.lower() or res.status_code == 401: continue
            else: response = res; break
        except Exception: continue

    if not response or response.status_code != 200:
        return {"clan_id": clan_id, "error": "All tokens failed"}

    full, resp = {}, None
    try:
        resp = data_pb2.response(); resp.ParseFromString(response.content)
        full = MessageToDict(resp, preserving_proto_field_name=True)
    except Exception: pass

    guild_xp = 0
    if resp:
        for fd, val in resp.ListFields():
            if fd.number == 36 and isinstance(val, int): guild_xp = val; break
    if guild_xp == 0 and full:
        guild_xp = int(full.get("field_36") or full.get("xp") or 0)

    track_guild_xp(str(clan_id), guild_xp)
    return {"captain_id": getattr(resp, "captain_id", 0) if resp else 0,
            "clan_id": int(clan_id),
            "clan_level": getattr(resp, "rank", 4) if resp else 4,
            "clan_name": getattr(resp, "special_code", None) if resp else f"Guild_{clan_id}",
            "current_members": full.get("current_members", 1),
            "max_members": full.get("max_members", 50),
            "Total guild xp": int(guild_xp),
            "region": region, "xp": int(guild_xp)}

async def request_clan_async(jwt_token, clan_id, region):
    server_url = get_server_url(region)
    msg = ReqCLan_pb2.MyMessage(); msg.field_1 = int(clan_id)
    payload = encrypt_payload(msg.SerializeToString())
    headers = GAME_HEADERS.copy()
    headers["Authorization"] = f"Bearer {jwt_token}" if not jwt_token.startswith("Bearer ") else jwt_token
    url = f"{server_url}/RequestJoinClan"
    async with httpx.AsyncClient(verify=False, timeout=8.0) as client:
        r = await client.post(url, headers=headers, content=payload)
        return r.status_code, r.text

# ─────────── USERS / SESSIONS / COUPONS ───────────
USERS_FILE = "users.json"; COUPONS_FILE = "coupons.json"
_sessions: Dict[str, dict] = {}; SESSION_TTL = 7 * 86400

def _load_users():
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE) as f: return json.load(f)
        except Exception: pass
    return {}

def _save_users(u):
    try:
        with open(USERS_FILE + ".tmp", "w") as f: json.dump(u, f, indent=2)
        os.replace(USERS_FILE + ".tmp", USERS_FILE)
    except Exception: pass

def _hash_password(pw, salt=None):
    if salt is None: salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac('sha256', pw.encode(), salt.encode(), 100_000)
    return f"{salt}${h.hex()}"

def _verify_password(pw, stored):
    try: return _hash_password(pw, stored.split('$', 1)[0]) == stored
    except Exception: return False

def _create_session(username, role='user'):
    token = secrets.token_urlsafe(32)
    _sessions[token] = {'username': username, 'role': role, 'created_at': time.time()}
    return token

def _get_session(request):
    token = request.cookies.get('session')
    if not token or token not in _sessions: return None
    sess = _sessions[token]
    if time.time() - sess['created_at'] > SESSION_TTL: _sessions.pop(token, None); return None
    return sess

def _norm(u): return str(u).strip().lower().replace(' ', '_')
def _disp(u): return str(u).replace('_', ' ').strip()

def _load_coupons():
    if os.path.exists(COUPONS_FILE):
        try:
            with open(COUPONS_FILE) as f: return json.load(f)
        except Exception: pass
    return {}

def _save_coupons(c):
    try:
        with open(COUPONS_FILE + ".tmp", "w") as f: json.dump(c, f, indent=2)
        os.replace(COUPONS_FILE + ".tmp", COUPONS_FILE)
    except Exception: pass

def _gen_coupon():
    a = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return '-'.join(''.join(secrets.choice(a) for _ in range(4)) for _ in range(3))

def _seed_admin():
    users = _load_users()
    if 'dg_drift' not in users:
        users['dg_drift'] = {'username': 'dg_drift', 'display_name': 'Admin',
                             'password_hash': _hash_password('Admin123'), 'role': 'admin',
                             'basic_credits': 1000, 'premium_credits': 100,
                             'created_at': time.time(), 'seeded': True}
        _save_users(users)

def _require_admin(request):
    s = _get_session(request)
    if not s: return web.json_response({"error": "Not authenticated"}, status=401)
    if s.get('role') != 'admin': return web.json_response({"error": "Admin required"}, status=403)
    return None

# ─────────── AUTH HANDLERS ───────────
async def handle_auth_signup(request):
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    u = _norm(data.get('username', '')); p = str(data.get('password', ''))
    if len(u) < 3: return web.json_response({"error": "Username too short"}, status=400)
    if len(p) < 6: return web.json_response({"error": "Password too short"}, status=400)
    users = _load_users()
    if u in users: return web.json_response({"error": "Username exists"}, status=400)
    users[u] = {'username': u, 'display_name': _disp(u), 'password_hash': _hash_password(p),
                'role': 'user', 'basic_credits': 0, 'premium_credits': 0,
                'created_at': time.time()}
    _save_users(users)
    return web.json_response({"success": True})

async def handle_auth_login(request):
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    u = _norm(data.get('username', '')); p = str(data.get('password', ''))
    users = _load_users(); user = users.get(u)
    if not user or not _verify_password(p, user.get('password_hash', '')):
        return web.json_response({"error": "Invalid credentials"}, status=401)
    token = _create_session(u, user.get('role', 'user'))
    resp = web.json_response({"success": True, "username": u,
                              "display_name": user.get('display_name', _disp(u)),
                              "role": user.get('role', 'user')})
    resp.set_cookie('session', token, httponly=True, max_age=SESSION_TTL, path='/', samesite='Lax')
    return resp

async def handle_auth_logout(request):
    token = request.cookies.get('session')
    if token and token in _sessions: _sessions.pop(token, None)
    resp = web.json_response({"success": True}); resp.del_cookie('session', path='/'); return resp

async def handle_auth_me(request):
    s = _get_session(request)
    if not s: return web.json_response({"error": "Not authenticated"}, status=401)
    users = _load_users(); user = users.get(s['username'], {})
    return web.json_response({"username": s['username'],
                              "display_name": user.get('display_name', _disp(s['username'])),
                              "role": s.get('role', 'user'),
                              "basic_credits": user.get('basic_credits', 0),
                              "premium_credits": user.get('premium_credits', 0)})

# ─────────── COUPON HANDLERS ───────────
async def handle_coupon_create(request):
    s = _get_session(request)
    if not s: return web.json_response({"error": "Not authenticated"}, status=401)
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    basic = int(data.get('basic_credits', 0) or 0); premium = int(data.get('premium_credits', 0) or 0)
    if basic == 0 and premium == 0: return web.json_response({"error": "Select ≥1 credit"}, status=400)
    users = _load_users(); user = users.get(s['username'])
    if not user: return web.json_response({"error": "User not found"}, status=404)
    if basic > user.get('basic_credits', 0) or premium > user.get('premium_credits', 0):
        return web.json_response({"error": "Insufficient credits"}, status=400)
    user['basic_credits'] -= basic; user['premium_credits'] -= premium
    _save_users(users)
    coupons = _load_coupons(); code = _gen_coupon()
    while code in coupons: code = _gen_coupon()
    coupons[code] = {'code': code, 'created_by': s['username'], 'basic_credits': basic,
                     'premium_credits': premium, 'status': 'active',
                     'created_at': time.time(), 'used_by': None, 'used_at': None}
    _save_coupons(coupons)
    return web.json_response({"success": True, "coupon": {"code": code, "basic_credits": basic,
                                                          "premium_credits": premium}})

async def handle_coupon_mine(request):
    s = _get_session(request)
    if not s: return web.json_response([])
    coupons = _load_coupons()
    mine = [c for c in coupons.values() if c.get('created_by') == s['username']]
    mine.sort(key=lambda x: x.get('created_at') or 0, reverse=True)
    return web.json_response(mine)

async def handle_coupon_redeem(request):
    s = _get_session(request)
    if not s: return web.json_response({"error": "Not authenticated"}, status=401)
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    code = str(data.get('code', '')).strip().upper()
    coupons = _load_coupons(); coupon = coupons.get(code)
    if not coupon: return web.json_response({"error": "Invalid coupon"}, status=404)
    if coupon.get('status') != 'active': return web.json_response({"error": "Already used"}, status=400)
    if coupon.get('created_by') == s['username']:
        return web.json_response({"error": "Cannot redeem own coupon"}, status=400)
    users = _load_users(); user = users.get(s['username'])
    if not user: return web.json_response({"error": "User not found"}, status=404)
    user['basic_credits'] = user.get('basic_credits', 0) + coupon.get('basic_credits', 0)
    user['premium_credits'] = user.get('premium_credits', 0) + coupon.get('premium_credits', 0)
    _save_users(users)
    coupon['status'] = 'used'; coupon['used_by'] = s['username']; coupon['used_at'] = time.time()
    _save_coupons(coupons)
    return web.json_response({"success": True, "basic_credits": coupon.get('basic_credits', 0),
                              "premium_credits": coupon.get('premium_credits', 0)})

async def handle_coupon_redeemed(request):
    s = _get_session(request)
    if not s: return web.json_response([])
    coupons = _load_coupons()
    r = [c for c in coupons.values() if c.get('used_by') == s['username']]
    r.sort(key=lambda x: x.get('used_at') or 0, reverse=True)
    return web.json_response(r)

async def handle_coupon_cancel(request):
    s = _get_session(request)
    if not s: return web.json_response({"error": "Not authenticated"}, status=401)
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    code = str(data.get('code', '')).strip().upper()
    coupons = _load_coupons(); coupon = coupons.get(code)
    if not coupon: return web.json_response({"error": "Not found"}, status=404)
    if coupon.get('created_by') != s['username']:
        return web.json_response({"error": "Not your coupon"}, status=403)
    if coupon.get('status') != 'active': return web.json_response({"error": "Cannot cancel"}, status=400)
    users = _load_users(); user = users.get(s['username'])
    if user:
        user['basic_credits'] = user.get('basic_credits', 0) + coupon.get('basic_credits', 0)
        user['premium_credits'] = user.get('premium_credits', 0) + coupon.get('premium_credits', 0)
        _save_users(users)
    coupon['status'] = 'cancelled'; _save_coupons(coupons)
    return web.json_response({"success": True, "new_basic": user['basic_credits'] if user else 0,
                              "new_premium": user['premium_credits'] if user else 0})

# ─────────── ADMIN ───────────
async def handle_admin_overview(request):
    err = _require_admin(request)
    if err: return err
    users = _load_users(); coupons = _load_coupons()
    return web.json_response({
        "total_users": len(users),
        "total_admins": sum(1 for u in users.values() if u.get('role') == 'admin'),
        "total_basic_credits": sum(u.get('basic_credits', 0) for u in users.values()),
        "total_premium_credits": sum(u.get('premium_credits', 0) for u in users.values()),
        "active_coupons": sum(1 for c in coupons.values() if c.get('status') == 'active'),
        "used_coupons": sum(1 for c in coupons.values() if c.get('status') == 'used'),
        "bot_accounts": len(bot_state.accounts),
        "bot_online": sum(1 for a in bot_state.accounts.values() if a.get('status') in ('ONLINE', 'IN_MATCH', 'IN_MATCH (TEAM)', 'SEARCHING')),
        "uptime": int(time.time() - bot_state.start_time)})

async def handle_admin_users(request):
    err = _require_admin(request)
    if err: return err
    users = _load_users(); r = []
    for un, u in users.items():
        acc = bot_state.accounts.get(un, {})
        r.append({"username": un, "display_name": u.get('display_name', _disp(un)),
                  "role": u.get('role', 'user'),
                  "basic_credits": u.get('basic_credits', 0),
                  "premium_credits": u.get('premium_credits', 0),
                  "created_at": u.get('created_at', 0),
                  "bot_status": acc.get('status', 'OFFLINE')})
    r.sort(key=lambda x: x.get('created_at') or 0, reverse=True)
    return web.json_response(r)

async def handle_admin_user_credits(request):
    err = _require_admin(request)
    if err: return err
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    target = _norm(data.get('username', '')); basic = int(data.get('basic_credits', 0) or 0)
    premium = int(data.get('premium_credits', 0) or 0); mode = str(data.get('mode', 'add'))
    users = _load_users()
    if target not in users: return web.json_response({"error": "Not found"}, status=404)
    if mode == 'set':
        users[target]['basic_credits'] = max(0, basic)
        users[target]['premium_credits'] = max(0, premium)
    else:
        users[target]['basic_credits'] = max(0, users[target].get('basic_credits', 0) + basic)
        users[target]['premium_credits'] = max(0, users[target].get('premium_credits', 0) + premium)
    _save_users(users)
    return web.json_response({"success": True, "basic_credits": users[target]['basic_credits'],
                              "premium_credits": users[target]['premium_credits']})

async def handle_admin_user_create(request):
    err = _require_admin(request)
    if err: return err
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    u = _norm(data.get('username', '')); p = str(data.get('password', ''))
    role = str(data.get('role', 'user')).lower()
    if len(u) < 3 or len(p) < 6: return web.json_response({"error": "Invalid"}, status=400)
    users = _load_users()
    if u in users: return web.json_response({"error": "Exists"}, status=400)
    users[u] = {'username': u, 'display_name': _disp(u), 'password_hash': _hash_password(p),
                'role': role, 'basic_credits': int(data.get('basic_credits', 0) or 0),
                'premium_credits': int(data.get('premium_credits', 0) or 0),
                'created_at': time.time()}
    _save_users(users)
    return web.json_response({"success": True, "username": u})

async def handle_admin_user_delete(request):
    err = _require_admin(request)
    if err: return err
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    t = _norm(data.get('username', '')); s = _get_session(request)
    if t == s['username']: return web.json_response({"error": "Cannot delete self"}, status=400)
    users = _load_users()
    if t not in users: return web.json_response({"error": "Not found"}, status=404)
    del users[t]; _save_users(users)
    return web.json_response({"success": True})

async def handle_admin_user_role(request):
    err = _require_admin(request)
    if err: return err
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    t = _norm(data.get('username', '')); role = str(data.get('role', 'user')).lower()
    if role not in ('user', 'admin'): return web.json_response({"error": "Bad role"}, status=400)
    users = _load_users()
    if t not in users: return web.json_response({"error": "Not found"}, status=404)
    users[t]['role'] = role; _save_users(users)
    return web.json_response({"success": True})

async def handle_admin_all_coupons(request):
    err = _require_admin(request)
    if err: return err
    coupons = _load_coupons()
    r = list(coupons.values())
    r.sort(key=lambda x: x.get('created_at') or 0, reverse=True)
    return web.json_response(r)

async def handle_admin_coupon_create(request):
    err = _require_admin(request)
    if err: return err
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    basic = int(data.get('basic_credits', 0) or 0); premium = int(data.get('premium_credits', 0) or 0)
    s = _get_session(request); users = _load_users()
    target = _norm(data.get('for_user', '')) or s['username']
    if target not in users: return web.json_response({"error": "User not found"}, status=404)
    user = users[target]
    if basic > user.get('basic_credits', 0) or premium > user.get('premium_credits', 0):
        return web.json_response({"error": "Insufficient"}, status=400)
    user['basic_credits'] -= basic; user['premium_credits'] -= premium
    _save_users(users)
    coupons = _load_coupons(); code = _gen_coupon()
    while code in coupons: code = _gen_coupon()
    coupons[code] = {'code': code, 'created_by': target, 'basic_credits': basic,
                     'premium_credits': premium, 'status': 'active',
                     'created_at': time.time(), 'used_by': None, 'used_at': None}
    _save_coupons(coupons)
    return web.json_response({"success": True, "coupon": {"code": code}})

async def handle_admin_coupon_revoke(request):
    err = _require_admin(request)
    if err: return err
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    code = str(data.get('code', '')).strip().upper()
    coupons = _load_coupons(); coupon = coupons.get(code)
    if not coupon: return web.json_response({"error": "Not found"}, status=404)
    if coupon.get('status') != 'active': return web.json_response({"error": "Not active"}, status=400)
    users = _load_users(); creator = coupon.get('created_by')
    if creator and creator in users:
        users[creator]['basic_credits'] = users[creator].get('basic_credits', 0) + coupon.get('basic_credits', 0)
        users[creator]['premium_credits'] = users[creator].get('premium_credits', 0) + coupon.get('premium_credits', 0)
        _save_users(users)
    coupon['status'] = 'cancelled'; _save_coupons(coupons)
    return web.json_response({"success": True})

async def handle_admin_bot_accounts(request):
    err = _require_admin(request)
    if err: return err
    r = []
    for uid, acc in bot_state.accounts.items():
        r.append({"uid": acc.get('uid', uid), "nickname": acc.get('nickname', ''),
                  "region": acc.get('region', ''), "level": acc.get('level', 0),
                  "exp": acc.get('current_exp', 0), "gained_exp": acc.get('gained_exp', 0),
                  "status": acc.get('status', 'OFFLINE')})
    r.sort(key=lambda x: x.get('gained_exp', 0), reverse=True)
    return web.json_response(r)

async def handle_admin_join_clan(request):
    err = _require_admin(request)
    if err: return err
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    clan_id = str(data.get('clan_id', '')).strip()
    region = str(data.get('region', 'IND')).upper()
    if not clan_id: return web.json_response({"error": "clan_id required"}, status=400)
    try:
        from clan_join import join_clan_bulk
        return web.json_response(await join_clan_bulk(clan_id, region))
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

# ─────────── GLORY LAUNCH ───────────
async def handle_client_launch(request):
    s = _get_session(request)
    if not s: return web.json_response({"error": "Not authenticated"}, status=401)
    try: data = await request.json()
    except Exception: return web.json_response({"error": "Invalid JSON"}, status=400)
    clan_id = str(data.get('clan_id', '')).strip()
    region = str(data.get('region', 'IND')).upper()
    if not clan_id or not clan_id.isdigit():
        return web.json_response({"error": "numeric clan_id required"}, status=400)
    users = _load_users(); user = users.get(s['username'])
    if not user: return web.json_response({"error": "User not found"}, status=404)
    credit_type = 'premium' if region in ('ME', 'BR', 'US', 'NA', 'SAC') else 'basic'
    if user.get(f'{credit_type}_credits', 0) < 1:
        return web.json_response({"error": f"Insufficient {credit_type} credits"}, status=400)
    user[f'{credit_type}_credits'] -= 1; _save_users(users)
    bot_state.pending_clans[region] = {"clan_id": clan_id, "region": region,
                                        "launched_by": s['username'],
                                        "launched_at": time.time(), "joined": False}
    bot_state.log(f"🏰 Launch: clan {clan_id} → {region} by {s['username']}", "success")
    asyncio.create_task(_async_join_group(clan_id, region))
    return web.json_response({"success": True, "clan_id": clan_id, "region": region,
                              "remaining_credits": user[f'{credit_type}_credits'],
                              "message": f"Bots joining clan {clan_id}"})

async def _async_join_group(clan_id, region):
    try:
        from clan_join import join_clan_bulk
        res = await join_clan_bulk(clan_id, region)
        joined = res.get('success', 0); total = res.get('total', 0)
        if joined > 0:
            entry = bot_state.pending_clans.get(region)
            if entry: entry['joined'] = True; entry['joined_at'] = time.time()
            bot_state.log(f"✅ {joined}/{total} joined clan {clan_id}", "success")
        else:
            bot_state.log(f"❌ Join failed: {res.get('error')}", "warning")
    except Exception as e:
        bot_state.log(f"❌ Join error: {e}", "error")

# ─────────── GUILD APIs ───────────
async def handle_api_guild_info(request):
    try:
        clan_id = request.query.get('guild_id') or request.query.get('clan_id')
        region = (request.query.get('region') or 'IND').upper()
        if request.query.get('key') != "XEROX_MODS":
            return web.json_response({"error": "Unauthorized"}, status=401)
        if not clan_id: return web.json_response({"error": "guild_id required"}, status=400)
        try:
            with open("token_cache.json") as f: tc = json.load(f)
        except Exception: return web.json_response({"error": "Cache fail"}, status=503)
        if not tc: return web.json_response({"error": "Empty cache"}, status=503)
        info = fetch_single_clan(clan_id, region, tc)
        if "error" in info: return web.json_response(info, status=500)
        # attach glory stats
        gs = get_glory_stats(clan_id)
        info["glory_stats"] = gs
        return web.json_response({"status": "success", "requested_region": region,
                                  "queried_clan_id": clan_id,
                                  "api_response": {"DEVLOVER": "CODEX",
                                                   "TIME": time.strftime("%I:%M %p"),
                                                   "clan_basic_info": info}})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

async def handle_glory_stats(request):
    """Live glory stats for all tracked clans + match history."""
    clan_id = request.query.get('clan_id')
    if clan_id:
        return web.json_response({"clan_id": clan_id, "stats": get_glory_stats(clan_id),
                                  "matches": [m for m in _match_history if m["clan_id"] == str(clan_id)][-30:]})
    all_stats = {}
    for cid in _guild_history.keys():
        all_stats[cid] = get_glory_stats(cid)
    return web.json_response({
        "session_start": int(_session_start),
        "uptime": int(time.time() - _session_start),
        "total_matches": len(_match_history),
        "clans": all_stats,
        "match_history": _match_history[-50:],
        "bots": [{"uid": a.get("uid"), "nickname": a.get("nickname"),
                  "status": a.get("status"), "level": a.get("level", 0),
                  "exp": a.get("current_exp", 0), "gained_exp": a.get("gained_exp", 0),
                  "region": a.get("region")} for a in bot_state.accounts.values()]
    })

# ─────────── BOT APIs ───────────
async def handle_api_clan_join(request):
    try:
        data = await request.json()
        clan_id = data.get('clan_id')
        if not clan_id: return web.json_response({'error': 'clan_id required'}, status=400)
        try:
            with open("token_cache.json") as f: tc = json.load(f)
        except Exception as e: return web.json_response({'error': f'cache: {e}'}, status=500)
        success = 0; total = 0
        for uid, creds in tc.items():
            token = creds.get('token'); region = creds.get('region', 'IND')
            if not token: continue
            total += 1
            try:
                sc, _ = await request_clan_async(token, clan_id, region)
                if sc == 200: success += 1
            except Exception: pass
        return web.json_response({'message': f'Sent {success}/{total} joins'})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def handle_get_stats(request):
    accounts_data = sorted(bot_state.accounts.values(), key=lambda x: x.get("gained_exp", 0), reverse=True)
    uptime = max(1, int(time.time() - bot_state.start_time))
    for acc in accounts_data:
        acc["is_paused"] = bot_state.is_paused(str(acc.get("uid", "")))
    return web.json_response({
        "total_accounts": len(bot_state.accounts),
        "total_matches": bot_state.total_matches,
        "total_matches_started": bot_state.total_matches_started,
        "total_active_matches": sum(acc.get("active_matches", 0) for acc in accounts_data),
        "total_gained_exp": bot_state.total_gained_exp,
        "exp_per_hour": int(bot_state.total_gained_exp / uptime * 3600),
        "accounts": accounts_data,
        "logs": bot_state.logs[-80:],
        "uptime": uptime,
        "pending_clans": list(bot_state.pending_clans.keys())})

async def handle_add_account(request):
    try:
        data = await request.json()
        existing = []
        if os.path.exists("accounts.json"):
            try:
                with open("accounts.json") as f: existing = json.load(f)
            except Exception: existing = []
        if "uid" in data and "password" in data:
            uid = str(data["uid"]).strip(); pwd = str(data["password"]).strip()
            if not uid or not pwd: return web.json_response({"status": "error", "error": "UID+Password required"})
            if uid in bot_state.account_workers:
                try: bot_state.account_workers[uid].cancel()
                except Exception: pass
                bot_state.account_workers.pop(uid, None)
            existing = [a for a in existing if str(a.get("uid", "")) != uid]
            existing.append({"uid": uid, "password": pwd})
            ident = uid
        elif "token" in data:
            token = str(data["token"]).strip()
            if not token: return web.json_response({"status": "error", "error": "Token required"})
            existing = [a for a in existing if a.get("token", "") != token]
            existing.append({"token": token})
            ident = f"Token_{token[:8]}"
        else:
            return web.json_response({"status": "error", "error": "Invalid payload"})
        with open("accounts.json", "w") as f: json.dump(existing, f, indent=2)
        bot_state.log(f"New account: {ident}", "success")
        if "on_account_added" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_account_added"](data))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})

async def handle_delete_account(request):
    try:
        data = await request.json()
        req_uid = str(data.get("uid", "")).strip()
        if not req_uid: return web.json_response({"status": "error", "error": "UID required"})
        if os.path.exists("accounts.json"):
            with open("accounts.json") as f: existing = json.load(f)
            existing = [a for a in existing if str(a.get("uid", "")) != req_uid]
            with open("accounts.json", "w") as f: json.dump(existing, f, indent=2)
        bot_state.accounts.pop(req_uid, None)
        bot_state.account_credentials.pop(req_uid, None)
        if req_uid in bot_state.account_workers:
            try: bot_state.account_workers[req_uid].cancel()
            except Exception: pass
            bot_state.account_workers.pop(req_uid, None)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})

async def handle_refresh_account(request):
    try:
        data = await request.json(); uid = str(data.get("uid", "")).strip()
        if "on_refresh_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_refresh_account"](uid))
        return web.json_response({"status": "ok"})
    except Exception as e: return web.json_response({"status": "error", "error": str(e)})

async def handle_restart_account(request):
    try:
        data = await request.json(); uid = str(data.get("uid", "")).strip()
        if "on_restart_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_restart_account"](uid))
        return web.json_response({"status": "ok"})
    except Exception as e: return web.json_response({"status": "error", "error": str(e)})

async def handle_clear_logs(request):
    bot_state.logs.clear(); return web.json_response({"status": "ok"})

async def handle_toggle_pause(request):
    try:
        data = await request.json(); uid = str(data.get("uid", "")).strip()
        if not uid: return web.json_response({"status": "error", "error": "UID required"})
        return web.json_response({"status": "ok", "is_paused": bot_state.toggle_pause(uid)})
    except Exception as e: return web.json_response({"status": "error", "error": str(e)})

async def handle_toggle_pause_all(request):
    try: return web.json_response({"status": "ok", "all_paused": bot_state.toggle_pause_all()})
    except Exception as e: return web.json_response({"status": "error", "error": str(e)})

async def handle_api_logs(request):
    logs = []
    for e in bot_state.logs[-80:]:
        if isinstance(e, dict):
            color = "#34d399" if e.get("level") == "success" else ("#f87171" if e.get("level") in ("warning", "error") else "#38bdf8")
            logs.append(f'<span style="color:{color}">[{e.get("time","")}] {e.get("message","")}</span>')
        else: logs.append(str(e))
    return web.json_response({"logs": logs})

async def handle_empty_list(request): return web.json_response([])
async def handle_null(request): return web.json_response(None)
async def handle_pricing(request):
    return web.json_response({"basic_credit_usd": 1.70, "premium_credit_usd": 20.00})

# ─────────── BOT STATE ───────────
class BotState:
    def __init__(self):
        self.accounts = {}; self.logs = []; self.max_logs = 200
        self.total_matches = 0; self.total_matches_started = 0
        self.total_gained_exp = 0; self.start_time = time.time()
        self.account_workers = {}; self.account_token_map = {}
        self.auth_to_game_id = {}; self.game_to_auth_id = {}
        self.paused_accounts = set(); self.refresh_callbacks = {}
        self.account_credentials = {}; self.active_writers = {}
        self.pending_clans = {}

    def log(self, message, level="info", uid=None):
        entry = {"time": time.strftime("%H:%M:%S"), "level": level, "message": message,
                 "uid": str(uid) if uid else None}
        self.logs.append(entry)
        if len(self.logs) > self.max_logs: self.logs.pop(0)

    def register_account(self, uid, nickname, region, level, exp, likes=0, token=None, auth_uid=None):
        uid = str(uid); lvl = level or 1
        if uid not in self.accounts:
            self.accounts[uid] = {
                "uid": uid, "auth_uid": str(auth_uid) if auth_uid else "",
                "nickname": nickname or f"Player_{uid[:6]}",
                "region": region or "BD", "level": lvl,
                "initial_exp": exp, "current_exp": exp, "gained_exp": 0,
                "likes": likes or 0, "status": "ONLINE",
                "matches_played": 0, "active_matches": 0,
                "last_match_time": None, "token": token or "",
                "start_time": time.time(), "is_paused": False,
                "last_updated": time.strftime("%H:%M:%S")}
        else:
            acc = self.accounts[uid]
            if nickname: acc["nickname"] = nickname
            if region: acc["region"] = region
            if level: acc["level"] = level
            acc["current_exp"] = exp
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["last_updated"] = time.strftime("%H:%M:%S")
        self.total_gained_exp = sum(a.get("gained_exp", 0) for a in self.accounts.values())

    def is_paused(self, uid): return str(uid) in self.paused_accounts

    def toggle_pause(self, uid):
        uid = str(uid); is_p = not self.is_paused(uid)
        if is_p:
            self.paused_accounts.add(uid)
            if uid in self.accounts:
                self.accounts[uid]["is_paused"] = True
                self.accounts[uid]["status"] = "PAUSED"
        else:
            self.paused_accounts.discard(uid)
            if uid in self.accounts:
                self.accounts[uid]["is_paused"] = False
                self.accounts[uid]["status"] = "ONLINE"
        return is_p

    def toggle_pause_all(self):
        any_active = any(not self.is_paused(k) for k in self.accounts.keys())
        for k in list(self.accounts.keys()):
            if any_active and not self.is_paused(k): self.toggle_pause(k)
            elif not any_active and self.is_paused(k): self.toggle_pause(k)
        return any_active

    def update_exp(self, uid, exp, level=None):
        uid = str(uid)
        if uid in self.accounts:
            acc = self.accounts[uid]
            old = acc["current_exp"]; acc["current_exp"] = exp
            if level: acc["level"] = level
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["last_updated"] = time.strftime("%H:%M:%S")
            if exp > old:
                self.log(f"★ {uid} gained +{exp-old:,} EXP", "success", uid)
            self.total_gained_exp = sum(a.get("gained_exp", 0) for a in self.accounts.values())

    def update_status(self, uid, status, active_matches=None):
        uid = str(uid)
        if uid in self.accounts:
            self.accounts[uid]["status"] = status
            if active_matches is not None:
                self.accounts[uid]["active_matches"] = active_matches
            self.accounts[uid]["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match(self, uid):
        uid = str(uid); self.total_matches += 1
        if uid in self.accounts:
            self.accounts[uid]["matches_played"] = self.accounts[uid].get("matches_played", 0) + 1
            self.accounts[uid]["last_match_time"] = time.strftime("%H:%M:%S")

bot_state = BotState()

# ─────────── STATIC ───────────
async def serve_spa(request):
    for base in (ELITE_ARENA_DIR, BASE_DIR):
        for name in ("index.html", "ffglory.html", "client.html"):
            p = os.path.join(base, name)
            if os.path.isfile(p): return web.FileResponse(p)
    return web.Response(status=404, text="Place index.html in ELITE_ARENA/ or project root")

# ─────────── START ───────────
async def start_web_dashboard(host="0.0.0.0", port=5000):
    _seed_admin()
    app = web.Application()

    for path in ("/", "/bot", "/panel", "/login", "/signup", "/client", "/admin", "/ffglory", "/index.html"):
        app.router.add_get(path, serve_spa)

    app.router.add_post("/api/auth/signup", handle_auth_signup)
    app.router.add_post("/api/auth/login", handle_auth_login)
    app.router.add_post("/api/auth/logout", handle_auth_logout)
    app.router.add_get("/api/me", handle_auth_me)

    app.router.add_post("/api/coupons/create", handle_coupon_create)
    app.router.add_get("/api/coupons/mine", handle_coupon_mine)
    app.router.add_post("/api/coupons/redeem", handle_coupon_redeem)
    app.router.add_get("/api/coupons/redeemed", handle_coupon_redeemed)
    app.router.add_post("/api/coupons/cancel", handle_coupon_cancel)

    app.router.add_get("/api/admin/overview", handle_admin_overview)
    app.router.add_get("/api/admin/users", handle_admin_users)
    app.router.add_post("/api/admin/user/credits", handle_admin_user_credits)
    app.router.add_post("/api/admin/user/create", handle_admin_user_create)
    app.router.add_post("/api/admin/user/delete", handle_admin_user_delete)
    app.router.add_post("/api/admin/user/role", handle_admin_user_role)
    app.router.add_get("/api/admin/coupons", handle_admin_all_coupons)
    app.router.add_post("/api/admin/coupon/create", handle_admin_coupon_create)
    app.router.add_post("/api/admin/coupon/revoke", handle_admin_coupon_revoke)
    app.router.add_get("/api/admin/bot-accounts", handle_admin_bot_accounts)
    app.router.add_post("/api/admin/join-clan", handle_admin_join_clan)

    app.router.add_post("/api/client/launch", handle_client_launch)
    app.router.add_get("/api/pricing", handle_pricing)
    app.router.add_get("/api/transactions", handle_empty_list)
    app.router.add_get("/api/client/groups", handle_empty_list)
    app.router.add_get("/api/client/activity-log", handle_empty_list)
    app.router.add_get("/api/inbox/messages", handle_empty_list)
    app.router.add_get("/api/notifications", handle_empty_list)
    app.router.add_get("/api/client/active-alert", handle_null)
    app.router.add_get("/api/vapid-public-key", handle_null)

    app.router.add_post("/api/clanjoin", handle_api_clan_join)
    app.router.add_get("/api/guildinfo", handle_api_guild_info)
    app.router.add_get("/api/glory_stats", handle_glory_stats)   # NEW!
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_get("/api/logs", handle_api_logs)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    app.router.add_post("/api/account/restart", handle_restart_account)
    app.router.add_post("/api/account/pause", handle_toggle_pause)
    app.router.add_post("/api/account/pause_all", handle_toggle_pause_all)
    app.router.add_post("/api/logs/clear", handle_clear_logs)

    app.router.add_get('/{filename:.*}', serve_spa)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"[WEB] Guild Glory Bot → http://localhost:{port}")

    # start XP poller
    asyncio.create_task(_guild_xp_poller())
    return runner