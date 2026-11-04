# patch_app.py — app.py তে ৪টা ফিক্স অটো বসায়
import re, sys, os

TARGET = "app.py"
if not os.path.exists(TARGET):
    print(f"[✗] {TARGET} not found"); sys.exit(1)

src = open(TARGET, "r", encoding="utf-8").read()
original = src
patches_applied = 0

# ═══════════ PATCH 1: _get_region_url (BD fix) ═══════════
OLD1 = '''def _get_region_url(region):
    region = region.upper()
    if region == "IND": return "https://client.ind.freefiremobile.com"
    if region in ["BR","US","NA"]: return "https://client.us.freefiremobile.com"
    return "https://clientbp.ggpolarbear.com"'''

NEW1 = '''def _get_region_url(region):
    r = str(region).upper()
    if r == "IND": return "https://client.ind.freefiremobile.com"
    if r == "BD":  return "https://clientbp.ggwhitehawk.com"
    if r in ("BR", "US", "NA"): return "https://client.us.freefiremobile.com"
    if r == "SAC": return "https://client.br.freefiremobile.com"
    if r == "TW":  return "https://client.tw.freefiremobile.com"
    if r == "CIS": return "https://client.cis.freefiremobile.com"
    if r in ("ME", "PK"): return "https://clientbp.ggblueshark.com"
    return "https://clientbp.ggpolarbear.com"'''

if OLD1 in src:
    src = src.replace(OLD1, NEW1); patches_applied += 1
    print("[✓] Patch 1: _get_region_url fixed (BD host)")
else:
    print("[!] Patch 1: pattern not found — check manually")

# ═══════════ PATCH 2: start_web_server (SPA on port 25561) ═══════════
# Find the whole old function and replace
pattern2 = r'async def start_web_server\(\):.*?return runner\n'
NEW2 = '''async def start_web_server():
    app = aiohttp.web.Application(middlewares=[_req_logger])

    async def serve_spa(_req):
        for base in (ELITE_ARENA_DIR, BASE_DIR):
            for name in ("index.html", "ffglory.html", "client.html"):
                p = os.path.join(base, name)
                if os.path.isfile(p):
                    return aiohttp.web.FileResponse(p)
        return aiohttp.web.Response(status=404, text="index.html not found in ELITE_ARENA/ or project root")

    for path in ("/", "/index.html", "/bot", "/panel", "/login", "/signup", "/admin", "/client", "/ffglory"):
        app.router.add_get(path, serve_spa)

    app.router.add_get("/api/ping", api_ping)
    app.router.add_get("/api/logs", api_logs)
    app.router.add_post("/api/clanjoin", api_clanjoin)
    app.router.add_get("/api/guildinfo", api_guildinfo)
    app.router.add_get("/api/stats", api_stats)
    app.router.add_get("/api/state", api_state)
    app.router.add_post("/api/create_team", api_create_team)
    app.router.add_post("/api/account/add", api_account_add)
    app.router.add_post("/api/account/delete", api_account_delete)
    app.router.add_post("/api/account/refresh", api_account_refresh)
    app.router.add_post("/api/account/restart", api_account_restart)
    app.router.add_post("/api/account/pause", api_account_pause)
    app.router.add_post("/api/account/pause_all", api_account_pause_all)
    app.router.add_post("/api/logs/clear", api_logs_clear)
    app.router.add_get("/api/progress", api_progress)
    app.router.add_get("/api/glory_stats", api_glory_stats)

    app.router.add_get("/{tail:.*}", serve_spa)

    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()
    print_success(f"[WEB] Dashboard → http://localhost:{WEB_PORT}")
    return runner
'''
match2 = re.search(pattern2, src, re.DOTALL)
if match2:
    src = src[:match2.start()] + NEW2 + src[match2.end():]
    patches_applied += 1
    print("[✓] Patch 2: start_web_server SPA-ified")
else:
    print("[!] Patch 2: pattern not found — check manually")

# ═══════════ PATCH 3: api_glory_stats endpoint ═══════════
# Insert BEFORE start_web_server
GLORY_ENDPOINT = '''
async def api_glory_stats(request):
    """Live glory stats — proxied from dashboard_server if loaded."""
    try:
        import dashboard_server as dash
        clan_id = request.query.get('clan_id')
        if clan_id:
            return aiohttp.web.json_response({
                "clan_id": clan_id,
                "stats": dash.get_glory_stats(clan_id),
                "matches": [m for m in dash._match_history if m["clan_id"] == str(clan_id)][-30:],
            })
        all_stats = {cid: dash.get_glory_stats(cid) for cid in dash._guild_history.keys()}
        return aiohttp.web.json_response({
            "session_start": int(dash._session_start),
            "uptime": int(time.time() - dash._session_start),
            "total_matches": len(dash._match_history),
            "clans": all_stats,
            "match_history": dash._match_history[-50:],
            "bots": [{"uid": a.get("uid"), "nickname": a.get("nickname"),
                      "status": a.get("status"), "level": a.get("level", 0),
                      "exp": a.get("current_exp", 0), "gained_exp": a.get("gained_exp", 0),
                      "region": a.get("region")} for a in bot_state.accounts.values()],
        })
    except Exception as e:
        return aiohttp.web.json_response({"error": str(e), "clans": {}, "bots": [],
                                          "match_history": [], "total_matches": 0,
                                          "uptime": 0})


'''
if "async def api_glory_stats" not in src and "async def start_web_server" in src:
    src = src.replace("async def start_web_server", GLORY_ENDPOINT + "async def start_web_server", 1)
    patches_applied += 1
    print("[✓] Patch 3: api_glory_stats endpoint added")
elif "async def api_glory_stats" in src:
    print("[i] Patch 3: already present, skipping")

# ═══════════ PATCH 4: Solo prevention + guild XP tracker ═══════════
# 4a — Solo prevention in _team_coordinator
SOLO_OLD = '''            other_uids = [u for u in sq["all_uids"] if u != uid_str]
            print_success(f"[TEAM] 👑 {uid_str} | {squad_id} | members={len(sq['all_uids'])} invite={len(other_uids)}")'''

SOLO_NEW = '''            # ═══ SOLO PREVENTION ═══
            wait_start = time.time()
            while len(sq["all_uids"]) < 2:
                if time.time() - wait_start > 180:
                    print_warning(f"[TEAM] {squad_id} only 1 member after 3min — waiting")
                    await asyncio.sleep(5)
                    sq = await _get_squad_info(squad_id)
                    if not sq: return
                    continue
                await asyncio.sleep(2)
                sq = await _get_squad_info(squad_id)
                if not sq: return
            print_success(f"[TEAM] {squad_id} has {len(sq['all_uids'])} members — playing as {'squad' if len(sq['all_uids'])>=4 else 'duo/trio'}")

            other_uids = [u for u in sq["all_uids"] if u != uid_str]
            print_success(f"[TEAM] 👑 {uid_str} | {squad_id} | members={len(sq['all_uids'])} invite={len(other_uids)}")'''

if SOLO_OLD in src:
    src = src.replace(SOLO_OLD, SOLO_NEW, 1); patches_applied += 1
    print("[✓] Patch 4a: solo prevention added")
else:
    print("[!] Patch 4a: pattern not found")

# 4b — Guild XP tracker task in main()
TRACKER_OLD = '''    asyncio.create_task(_no_match_watchdog())'''
TRACKER_NEW = '''    asyncio.create_task(_no_match_watchdog())

    # ═══ GUILD XP TRACKER — polls every 60s for live glory dashboard ═══
    async def _guild_xp_tracker():
        try:
            import dashboard_server as dash
        except Exception as e:
            print_warning(f"[GLORY-TRACKER] dashboard_server not loaded: {e}")
            return
        while True:
            await asyncio.sleep(60)
            try:
                with open("token_cache.json", "r") as f:
                    tc = json.load(f)
                for cid in list(dash._guild_history.keys()):
                    for _, cred in tc.items():
                        xp = await dash.fetch_clan_xp_async(cid, cred.get("region", "IND"), tc)
                        if xp and xp > 0:
                            dash.track_guild_xp(cid, xp); break
            except Exception: pass
    asyncio.create_task(_guild_xp_tracker())'''

if TRACKER_OLD in src:
    src = src.replace(TRACKER_OLD, TRACKER_NEW, 1); patches_applied += 1
    print("[✓] Patch 4b: guild XP tracker task added")
else:
    print("[!] Patch 4b: pattern not found")

# ═══════════ WRITE ═══════════
if src != original:
    backup = TARGET + ".bak"
    open(backup, "w", encoding="utf-8").write(original)
    open(TARGET, "w", encoding="utf-8").write(src)
    print(f"\n[✓] {patches_applied} patches applied → {TARGET}")
    print(f"[✓] Backup saved → {backup}")
    print(f"\n[!] Now restart: python app.py")
else:
    print("\n[!] No changes made — patterns didn't match, check the file manually")