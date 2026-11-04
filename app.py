# -*- coding: utf-8 -*-
# ============================================================
#   CGU — FREE FIRE 4-BOT SQUAD + AUTO CREATE + MOVEMENT
#   + PARALLEL SQUADS (N accounts = N/4 squads)
#   + FIXED MOVEMENT: 24-byte compact packet (matches live capture)
#   + match_requested guard (prevents duplicate start_match)
#   + Scale-tuned for 40 accounts
#   + MAJOR LOGIN: CLEAN REWRITE (packet-template aligned)
# ============================================================
import sys, os, json, time, uuid, random, asyncio, base64, struct, socket
import itertools, traceback, ssl, hmac, hashlib, string, re, subprocess, math
import httpx
import aiohttp
import aiohttp.web
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception: pass

from google_play_scraper import app as play_scraper
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from protobuf_decoder.protobuf_decoder import Parser
from message_ids import MESSAGE_ID_TO_NAME
import thunderFF_pb2

try:
    import StartMatch_pb2
except Exception:
    StartMatch_pb2 = None

try:
    import reqClan_pb2
    _CLAN_PB_AVAILABLE = True
except Exception:
    try:
        import ReqCLan_pb2 as reqClan_pb2
        _CLAN_PB_AVAILABLE = True
    except Exception:
        _CLAN_PB_AVAILABLE = False

# ==================== CONFIG ====================
WEB_HOST = "0.0.0.0"
WEB_PORT = 25561
ACCOUNTS_FILE = "accounts.json"
TOKEN_CACHE_FILE = "token_cache.json"
DEVICES_FILE = "devices.json"
TOKEN_CACHE_TTL = 1200
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ELITE_ARENA_DIR = os.path.join(BASE_DIR, "ELITE_ARENA")

START_MATCH_INTERVAL = 3.0
NEW_MATCH_DELAY = 2.0
MAX_MATCH_DURATION = 1500

MATCH_IDLE_TIMEOUT  = 35.0

NO_MATCH_RESTART_ENABLED  = True
NO_MATCH_RESTART_TIMEOUT  = 90.0
NO_MATCH_RESTART_GRACE    = 60.0

_accept_friend_request = True
DEBUG_ALL_PACKETS = False

TEAM_MODE = True
TEAM_SIZE = 4
TEAM_READY_DELAY = 1.5
TEAM_INVITE_WAIT = 1.0
SQUAD_FILL_TIMEOUT = 120.0
CLAN_ID_DEFAULT = "3105712089"
DEV_UID =""

# ==================== GAME MODE (BR / CS) ====================
# Run:  GAME_MODE=CS python app.py      (or just change the default below)
GAME_MODE = os.environ.get("GAME_MODE", "BR").strip().upper()   # "BR" or "CS"
CS_MODE_ID = 15                      # verified from a real CS capture (UDP cmd 448 + match-found field 5.7)
MODE_IDS = {"BR": 1, "CS": CS_MODE_ID}
MAP_IDS  = {"BR": 1, "CS": 1}
# DEBUG_PACKETS=1 -> every plaintext server packet is appended to packets_debug.log
DEBUG_PACKETS = os.environ.get("DEBUG_PACKETS", "0") == "1"
# SNIFF_MODE=1 -> single-account lobby test: the bot does NOT build its own squad / start matches,
# it auto-accepts any squad invite (e.g. from your phone) and logs every server packet.
SNIFF_MODE = os.environ.get("SNIFF_MODE", "0") == "1"
if SNIFF_MODE: DEBUG_PACKETS = True

# ---- Real CS movement (cmd 2001, plaintext, 10 Hz) --------------------------------------
# Taken from a real CS match capture: 100 consecutive frames (~10 s) of a player running in a
# closed loop. Each frame = [dx, dy, dz, f04..f22]; dx/dy/dz are offsets from the loop start,
# so the loop is replayed around the bot's own spawn position (read from the server's own
# cmd-2001 state echo). Timestamp (f23) = ms since the first echo. Used only when GAME_MODE=CS.
CS_REAL_MOVE = os.environ.get("REAL_MOVE", "1") == "1"   # real movement packets for BR and CS (REAL_MOVE=0 -> old packets)
CS_MOVE_START_DELAY = 11.0     # the real client starts moving ~11 s after joining (buy phase)
CS_MOVE_LOOP = json.loads('''[[0,0,0,52,0,193,52,193,280,0,1055,0,2,1048737255,3181511923,3212228704,0,0,601,0,0,0],[-254,0,-10,34,0,195,34,195,188,0,1075,0,2,1043276074,3179517723,3212554496,0,0,601,0,0,0],[-478,0,-18,34,0,195,34,195,188,0,1075,0,2,1043276070,3179517723,3212554494,0,0,601,0,0,0],[-702,0,-26,34,0,195,34,195,188,0,1075,0,2,1043276069,3179517724,3212554494,0,0,601,0,0,0],[-914,0,-34,34,0,195,34,195,172,0,985,0,2,1043276068,3179517725,3212554494,0,0,601,0,0,0],[-1188,0,-46,34,0,195,34,195,172,0,985,0,2,1043276068,3179517727,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517729,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517733,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517735,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517735,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517737,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517739,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517741,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517742,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517744,3212554494,0,0,413,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517746,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517748,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517750,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517752,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517754,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517756,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517758,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517760,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517761,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517763,3212554494,0,0,222,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517765,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517767,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517768,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517770,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517771,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517773,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517775,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,172,0,985,0,2,1043276068,3179517777,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,188,0,1075,0,2,1043276068,3179517782,3212554494,0,0,31,0,0,0],[-1302,0,-50,34,0,195,34,195,188,0,1075,0,2,1043276068,3179517784,3212554494,0,0,44,0,0,0],[-1302,0,-50,34,0,195,34,195,188,0,1075,0,2,1043276068,3179517786,3212554494,0,0,44,0,0,0],[-1302,0,-50,34,0,195,0,0,0,0,0,0,0,1043276068,3179517788,3212554494,0,0,44,0,0,0],[-1302,446,-50,34,0,195,181,80,119,546,54,1,11,1043276068,3179517789,3212554494,0,0,44,0,0,0],[-1062,956,54,183,0,78,183,76,485,352,204,1,11,1043276068,3179517789,3212554494,0,0,44,0,0,0],[-454,1272,282,189,0,62,195,41,755,158,188,1,11,1061847116,3176784714,3206284906,0,0,44,0,0,0],[304,1398,418,197,0,24,195,35,777,35,56,1,11,1062679982,3173891192,3205117590,0,0,44,0,0,0],[1076,1332,452,199,0,4,199,11,779,229,6,1,11,1062679982,3173891192,3205117588,0,0,44,0,0,0],[2106,942,460,199,0,2,199,14,779,423,12,1,11,1062679982,3173891192,3205117588,0,0,44,0,0,0],[2620,620,480,199,0,10,197,30,777,617,46,1,11,1062679982,3173891192,3205117588,0,0,44,0,0,0],[3390,0,534,199,0,16,197,26,777,875,64,1,0,1062679982,3173891192,3205117588,0,0,40,0,0,0],[4416,0,620,199,0,16,197,21,777,1069,64,1,0,1063131182,3188122053,3203763024,0,0,40,0,0,0],[5128,0,638,195,0,33,175,91,597,0,235,0,0,1063325885,3197064347,3200610480,0,0,40,0,0,0],[5674,0,286,151,0,129,135,145,465,0,505,0,0,1063288772,3197395810,3200534400,0,0,40,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316247,3197395810,3200407960,0,0,40,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316248,3197395808,3200407960,0,0,40,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316248,3197395807,3200407960,0,0,40,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316248,3197395806,3200407960,0,0,40,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316248,3197395806,3200407960,0,0,40,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[5824,0,220,135,0,145,0,0,0,0,0,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[5824,0,220,135,0,145,198,28,87,0,147,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[5872,0,222,23,0,197,200,18,32,0,337,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[5716,0,216,98,0,173,200,12,332,0,377,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[5250,0,126,182,0,83,200,10,644,0,145,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[4550,0,118,200,0,10,200,10,780,0,40,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[3778,0,158,200,0,10,200,10,780,0,40,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[3006,0,196,200,0,10,200,10,780,0,40,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[2236,0,232,200,0,8,200,2,772,0,4,0,0,1063316248,3197395806,3200407960,0,0,30,0,0,0],[1474,0,202,200,0,13,200,19,768,0,77,0,0,1063316248,3197395806,3200407960,0,0,41,0,0,0],[728,0,72,194,0,47,190,59,728,0,231,0,0,1062092329,3197746615,3204627294,0,0,41,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061498431,3197746618,3205499270,0,0,41,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061419570,3197746617,3205606290,0,0,41,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061419570,3197746617,3205606290,0,0,41,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061354156,3197746618,3205693674,0,0,41,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061354155,3197746617,3205693674,0,0,41,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061354155,3197746617,3205693674,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061354155,3197746617,3205693674,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061354155,3197746617,3205693674,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1061354155,3197746617,3205693674,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1060717569,3197577206,3206525030,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1058326754,3197199552,3208854164,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797673,3209708562,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,53,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,36,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0],[248,0,-58,188,0,67,0,0,0,0,0,0,0,1057199125,3196797674,3209708562,0,0,55,0,0,0]]''')

_ECHO_BY_SERVER = {}   # (ip, port) of the match server -> (spawn pos, time); shared by all bots in that match

def _uleb_sync(n):
    n = int(n) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b = n & 0x7F; n >>= 7
        if n: out.append(b | 0x80)
        else: out.append(b); break
    return bytes(out)

# ==================== ANTI-AFK MOVEMENT ====================
ANTI_AFK_ENABLED        = True
ANTI_AFK_TICK_HZ        = 6.0
ANTI_AFK_SPAWN_DELAY    = 3.0
ANTI_AFK_RADIUS         = 800
ANTI_AFK_SPEED          = 55
ANTI_AFK_SPRINT_SPEED   = 95
ANTI_AFK_IDLE_CHANCE    = 0.08
ANTI_AFK_SPRINT_CHANCE  = 0.35
ANTI_AFK_DECISION_SEC   = 2.5

ANTI_AFK_PING_EVERY     = 0.5
ANTI_AFK_JUMP_EVERY     = 20.0

ANALOG_DIRECTIONS = [
    (0.0,  1.0, "maju"), (0.0, -1.0, "mundur"),
    (-1.0, 0.0, "kiri"), (1.0,  0.0, "kanan"),
    (-0.7, 0.7, "maju-kiri"), (0.7,  0.7, "maju-kanan"),
    (-0.7,-0.7, "mundur-kiri"), (0.7, -0.7, "mundur-kanan"),
    (0.0,  0.0, "idle"),
]

# ==================== SPEED MOVEMENT ====================
ENABLE_SPEED_MOVEMENT       = True
MOVEMENT_SPEED_MIN          = 1.0
MOVEMENT_SPEED_MAX          = 1.4
MOVEMENT_PATTERN            = "circle"
MOVEMENT_TICK_MS            = 150
MOVEMENT_RADIUS             = 500
MOVEMENT_UPDATE_SPEED_EVERY = 4.0

MOVEMENT_STATE_IDLE_CHANCE   = 0.10
MOVEMENT_STATE_WALK_CHANCE   = 0.28
MOVEMENT_STATE_RUN_CHANCE    = 0.32
MOVEMENT_STATE_SPRINT_CHANCE = 0.10
MOVEMENT_STATE_JUMP_CHANCE   = 0.08
MOVEMENT_STATE_CROUCH_CHANCE = 0.06
MOVEMENT_STATE_PRONE_CHANCE  = 0.06

PLAYER_STATE_MAP = {
    "idle":   0, "walk": 1, "run": 1, "sprint": 1,
    "jump":   2, "crouch": 3, "prone": 4,
}
MOVE_STATE_MAP = {
    "idle":   0, "walk": 1, "run": 2, "sprint": 3,
    "jump":   4, "crouch": 1, "prone": 1,
}
STATE_SPEED = {
    "idle":   0, "walk": 45, "run": 70, "sprint": 95,
    "jump":   55, "crouch": 25, "prone": 15,
}

# ==================== NEWBIE CHARACTER (DISABLED) ====================
ENABLE_NEWBIE_CHARACTER     = False
NEWBIE_SELECT_BEFORE_TEAM   = False
NEWBIE_SELECT_DELAY         = 1.5
NEWBIE_SELECT_RETRY         = 2
NEWBIE_SELECT_WAIT_AFTER    = 2.0

NEWBIE_ITEM_ID_1            = 102000011
NEWBIE_ITEM_ID_2            = 102000026
NEWBIE_ITEM_ID_3            = 102000037
NEWBIE_ITEM_ID_EXTRA        = 102000023

NEWBIE_SUB4_FIELD_3         = 50
NEWBIE_SUB4_FIELD_5_2       = 4806
NEWBIE_SUB4_FIELD_8         = 1
NEWBIE_SUB4_FIELD_11        = 11679599800712232

AES_KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
AES_IV  = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
KEYSTREAM = bytes([0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30])
HEX_KEY = "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3"
API_KEY = HEX_KEY.encode()

REGION_LANG = {"ME":"ar","IND":"hi","ID":"id","VN":"vi","TH":"th","BD":"bn",
               "PK":"ur","TW":"zh","RU":"ru","SAC":"es","BR":"pt","NA":"pt",
               "SG":"pt","US":"pt"}

# ==================== STATE ====================
class BotState:
    def __init__(self):
        self.accounts: Dict[str, dict] = {}
        self.account_credentials: Dict[str, dict] = {}
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.auth_to_game_id: Dict[str, str] = {}
        self.game_to_auth_id: Dict[str, str] = {}
        self.account_token_map: Dict[str, str] = {}
        self.paused: set = set()
        self.writers: Dict[str, list] = {}
        self.logs: List[dict] = []
        self.total_matches = 0
        self.matches_started = 0
        self.total_exp_gained = 0
        self.create_progress: List[str] = []
        self.creating = False
        self.start_time = time.time()
        self._writer_lock = asyncio.Lock()

    def log(self, msg, level="info"):
        try:
            t = datetime.now().strftime("%H:%M:%S")
            self.logs.append({"t": time.time(), "time": t, "message": str(msg), "level": level})
            if len(self.logs) > 500:
                self.logs = self.logs[-500:]
        except Exception: pass

    def register_account(self, uid, nickname, region="ID", level=1, exp=0,
                         likes=0, token=None, auth_uid=None):
        uid = str(uid)
        existing = self.accounts.get(uid, {})
        self.accounts[uid] = {
            "uid": uid, "nickname": nickname, "region": region,
            "level": level, "exp": exp, "likes": likes,
            "token": token, "auth_uid": auth_uid,
            "status": "ONLINE", "current_exp": exp,
            "initial_exp": existing.get("initial_exp", exp),
            "gained_exp": existing.get("gained_exp", 0),
            "matches": existing.get("matches", 0),
            "active_matches": 0,
            "uptime_seconds": existing.get("uptime_seconds", 0),
            "last_match_time": existing.get("last_match_time", "Running..."),
            "is_paused": False,
        }

    def update_status(self, uid, status, matches=0):
        uid = str(uid)
        if uid in self.accounts:
            self.accounts[uid]["status"] = status
            self.accounts[uid]["active_matches"] = matches
            self.accounts[uid]["is_paused"] = (status == "PAUSED")

    def update_exp(self, uid, exp, level=None):
        uid = str(uid)
        if uid in self.accounts:
            a = self.accounts[uid]
            old = a.get("current_exp", 0)
            if exp > old:
                a["gained_exp"] = a.get("gained_exp", 0) + (exp - old)
                self.total_exp_gained += (exp - old)
            a["current_exp"] = exp; a["exp"] = exp
            if level: a["level"] = level

    def increment_match(self, uid):
        self.total_matches += 1
        uid = str(uid)
        if uid in self.accounts:
            self.accounts[uid]["matches"] = self.accounts[uid].get("matches", 0) + 1
            self.accounts[uid]["last_match_time"] = datetime.now().strftime("%H:%M:%S")

    def increment_match_started(self): self.matches_started += 1

    def register_writer(self, uid, w): self.writers.setdefault(str(uid), []).append(w)
    def unregister_writer(self, uid, w):
        try: self.writers.get(str(uid), []).remove(w)
        except Exception: pass
    def close_writers_for_account(self, uid):
        for w in list(self.writers.get(str(uid), [])):
            try: w.close()
            except Exception: pass
        self.writers[str(uid)] = []
    def is_paused(self, uid): return str(uid) in self.paused
    def toggle_pause(self, uid, state):
        uid = str(uid)
        if state:
            self.paused.add(uid)
            if uid in self.accounts:
                self.accounts[uid]["is_paused"] = True
                self.accounts[uid]["status"] = "PAUSED"
        else:
            self.paused.discard(uid)
            if uid in self.accounts:
                self.accounts[uid]["is_paused"] = False
                if self.accounts[uid].get("status") == "PAUSED":
                    self.accounts[uid]["status"] = "ONLINE"

bot_state = BotState()

# ==================== TEAM / SQUAD STATE ====================
_team_state = {
    "peers": {},
    "lock": asyncio.Lock(),
}

_squads: Dict[str, dict] = {}
_account_squad: Dict[str, str] = {}
_squad_lock = asyncio.Lock()

async def _get_or_assign_squad(uid_str: str) -> str:
    async with _squad_lock:
        if uid_str in _account_squad:
            sid = _account_squad[uid_str]
            if sid in _squads:
                return sid
            _account_squad.pop(uid_str, None)

        for sid, sq in _squads.items():
            if sq.get("started"): continue
            if len(sq["all_uids"]) < TEAM_SIZE and uid_str not in sq["all_uids"]:
                sq["all_uids"].append(uid_str)
                _account_squad[uid_str] = sid
                print_info(f"[SQUAD] {uid_str} → {sid} ({len(sq['all_uids'])}/{TEAM_SIZE})")
                return sid

        sid = f"squad_{len(_squads)+1}"
        _squads[sid] = {
            "leader": uid_str,
            "all_uids": [uid_str],
            "team_code": None,
            "started": False,
            "match_requested": False,
            "match_requested_at": 0,
            "created_at": time.time(),
        }
        _account_squad[uid_str] = sid
        print_success(f"[SQUAD] {sid} created | leader={uid_str} | 1/{TEAM_SIZE}")
        return sid

async def _get_squad_info(sid: str) -> Optional[dict]:
    async with _squad_lock:
        sq = _squads.get(sid)
        if not sq: return None
        return {**sq, "all_uids": list(sq["all_uids"])}

async def _mark_squad_started(sid: str):
    async with _squad_lock:
        if sid in _squads:
            _squads[sid]["started"] = True
            _squads[sid]["match_requested"] = False

async def _set_squad_team_code(sid: str, code: str):
    async with _squad_lock:
        if sid in _squads:
            _squads[sid]["team_code"] = code

# ==================== RESTART STATE ====================
_restart_state = {
    "lock": asyncio.Lock(), "finished_bots": set(),
    "target": None, "restarting": False,
}

async def _mark_bot_match_finished(account_id):
    async with _restart_state["lock"]:
        if _restart_state["restarting"]: return
        _restart_state["finished_bots"].add(str(account_id))
        n_done = len(_restart_state["finished_bots"])
        if _restart_state["target"] is None:
            active_ids = {uid for uid, a in bot_state.accounts.items()
                          if a.get("status") not in ("OFFLINE",)}
            _restart_state["target"] = max(1, len(active_ids)) if active_ids else TEAM_SIZE
        target = _restart_state["target"]
        print_success(f"[RESTART] {account_id} → {n_done}/{target}")
        if n_done >= target:
            _restart_state["restarting"] = True
            print_warning(f"[RESTART] ✅ All {target} bots done! Restart in 3s...")
            asyncio.create_task(_schedule_restart())

# ==================== NO-MATCH WATCHDOG ====================
_no_match_state = {"lock": asyncio.Lock(), "global_last_match": None, "restart_triggered": False}

async def _mark_match_found(account_id=None):
    async with _no_match_state["lock"]:
        _no_match_state["global_last_match"] = time.time()

_ALLOWED_RESTART_STATUSES = {"ONLINE", "SEARCHING"}

def _all_bots_are_online_or_searching() -> bool:
    active_statuses = []
    for uid, acc in bot_state.accounts.items():
        status = str(acc.get("status", "")).upper().strip()
        if status in ("", "OFFLINE"): continue
        active_statuses.append(status)
    if not active_statuses: return False
    for st in active_statuses:
        if st not in _ALLOWED_RESTART_STATUSES: return False
    return True

def _any_bot_in_match() -> bool:
    for uid, acc in bot_state.accounts.items():
        status = str(acc.get("status", "")).upper().strip()
        if "IN_MATCH" in status: return True
    return False

async def _no_match_watchdog():
    if not NO_MATCH_RESTART_ENABLED: return
    await asyncio.sleep(NO_MATCH_RESTART_GRACE)
    async with _no_match_state["lock"]:
        if _no_match_state["global_last_match"] is None:
            _no_match_state["global_last_match"] = time.time()
    print_info(f"[NO-MATCH] watchdog aktif timeout={NO_MATCH_RESTART_TIMEOUT:.0f}s")
    while True:
        await asyncio.sleep(10.0)
        try:
            if _restart_state["restarting"]: return
            if _no_match_state["restart_triggered"]: return
            if _any_bot_in_match():
                async with _no_match_state["lock"]:
                    _no_match_state["global_last_match"] = time.time()
                continue
            if not _all_bots_are_online_or_searching():
                async with _no_match_state["lock"]:
                    _no_match_state["global_last_match"] = time.time()
                continue
            async with _no_match_state["lock"]:
                last = _no_match_state["global_last_match"] or 0
            elapsed = time.time() - last
            if elapsed >= NO_MATCH_RESTART_TIMEOUT:
                async with _no_match_state["lock"]:
                    if _no_match_state["restart_triggered"]: continue
                    _no_match_state["restart_triggered"] = True
                msg = f"[NO-MATCH] No match for {elapsed:.0f}s → restart!"
                print_warning(msg); bot_state.log(msg, "warning")
                async with _restart_state["lock"]:
                    _restart_state["restarting"] = True
                await _schedule_restart(); return
        except Exception as e:
            print_warning(f"[NO-MATCH] error: {e}")

async def _schedule_restart():
    await asyncio.sleep(3.0)
    print_warning("[RESTART] 🔄 Executing full process restart...")
    try: sys.stdout.flush(); sys.stderr.flush()
    except Exception: pass
    try: os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        print_error(f"[RESTART] execv failed: {e}")
        try:
            subprocess.Popen([sys.executable] + sys.argv); os._exit(0)
        except Exception as e2:
            print_error(f"[RESTART] subprocess: {e2}"); os._exit(1)

# ==================== PRINT HELPERS ====================
class C:
    GREEN='\033[92m'; FAIL='\033[91m'; WARNING='\033[93m'
    CYAN='\033[96m'; MAGENTA='\033[95m'; WHITE='\033[97m'; ENDC='\033[0m'

def print_success(t): print(f"{C.GREEN}[+] {t}{C.ENDC}"); bot_state.log(t, "success")
def print_error(t):   print(f"{C.FAIL}[-] {t}{C.ENDC}");   bot_state.log(t, "error")
def print_warning(t): print(f"{C.WARNING}[!] {t}{C.ENDC}"); bot_state.log(t, "warning")
def print_info(t):    print(f"{C.CYAN}[i] {t}{C.ENDC}");   bot_state.log(t, "info")

GGRE_RAW = bytes.fromhex(
    "47475245010101006d020000b260ee08e1f86b4c57f9c70f86bba26ed5d1436bcf52e142db3249d905eded757764991ca31a8373cdd26eab91b80f3f1f6f262f4f10b895d7b6937ddba30a27197453890e9da49373f736c679b8254e2f8e1623e91084a5fdd5374fe478ff99e010834553fddeb0bec4018d142e49df9bb236675b67e852f92a43586f7a7d4d0d7a197a1d4da32714dab4d069ad53214e2c33b3877420a12459738c4c619cbd815fa878bbd104776bb3e1ac7818d5397b04a666ac57f682763ff2df31bc2f846ddc7904f3dce1bda0c4b01f9698e9166f98c84f92640abe3f6317834a42e5c12573b46842c1a6ea8fe6ca9c006dca48a2087e1f983dfb5692771e4a0b15b337ad669b1d08b40862b176bace5331b49a767375b1a8469012aa70a67b55fe71b72478201d0b3ac7269c064c960361e92ee7ba8a42cc6b582bf9b965fb388fa9172ad44c4b073ac23c02080a6bcd106b691ffdf4c7cf71e42f2063fcf196e9bddc5e85be1fe5048eb1b31b460efbb46d76195eef9904c4cba326f2e17ff51fec17dd965aa06dadd4ab07d6966e4a7c38e8afd66dfde56c872bb87516a8f7313c797e4d80e5ad4a5f7afad95c1e0449254adae052e71a3fb98399f93ab30848e0d23252dd45e6fd41bc5fa7303dfb846a8fd713a0032a0b0ae96dfba1bbe41d20abd8099e2cb7fccc329d25bd153029139ef05d090a5093ea557693c0a8d491395b8a23ca844b3887dd5dfc29ac06f4b9be87883a793211b973465e2644c4f5de02b8ab01571401203fc2740423826858cc6da0194c195d27aac4ec4b9d23d506c1501d06440aca3fc180926d75d4a004d3dff21eb4ea1e8c86e6c9627248eff953e1d192d5c8efc006a7e512388ef2ebdb72ee3ad42d719f28e8f994e12ebf4c79f2ffe3abd7408ccd2236a7b89b2606247a732c10c4"
)

def _encode_proto_std(fields):
    out = b""
    for f in sorted(fields.keys()):
        v = fields[f]
        if isinstance(v, bool):
            out += encode_varint((f << 3) | 0) + encode_varint(int(v))
        elif isinstance(v, int):
            out += encode_varint((f << 3) | 0) + encode_varint(v)
        elif isinstance(v, (str, bytes)):
            if isinstance(v, str): v = v.encode()
            out += encode_varint((f << 3) | 2) + encode_varint(len(v)) + v
    return out

# ==================== DEVICE ====================
def _generate_new_device() -> dict:
    device_list = [
        ("Samsung","SM-G998B","Adreno (TM) 660","Android OS 12 / API-31"),
        ("Xiaomi","2201122G","Adreno (TM) 730","Android OS 13 / API-33"),
        ("Realme","RMX3700","Mali-G710","Android OS 14 / API-34"),
        ("OnePlus","CPH2451","Adreno (TM) 740","Android OS 13 / API-33"),
        ("OPPO","CPH2611","Adreno (TM) 720","Android OS 14 / API-34"),
    ]
    brand, model, gpu, os_ver = random.choice(device_list)
    return {"unique_device_id":f"Google|{uuid.uuid4()}","brand":brand,"model":model,
            "gpu_renderer":gpu,"system_software":os_ver,
            "screen_width":random.choice([1080,1440,720]),
            "screen_height":random.choice([2400,3200,1600]),
            "screen_dpi":str(random.randint(300,420)),
            "memory":random.randint(2800,6500)}

def get_device_for_account(acc_key: str) -> dict:
    devices = {}
    if os.path.exists(DEVICES_FILE):
        try:
            with open(DEVICES_FILE,"r",encoding="utf-8") as f:
                devices = json.load(f)
                if not isinstance(devices, dict): devices = {}
        except Exception: devices = {}
    if acc_key in devices: return devices[acc_key]
    nd = _generate_new_device(); devices[acc_key] = nd
    try:
        with open(DEVICES_FILE,"w",encoding="utf-8") as f:
            json.dump(devices, f, indent=2)
    except Exception: pass
    return nd

def encode_f14(s):
    ks = [0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,
          0x30,0x30,0x30,0x30,0x32,0x30,0x31,0x37,
          0x30,0x30,0x30,0x30,0x30,0x32,0x30,0x31,
          0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30]
    return bytes(ord(c) ^ ks[i % len(ks)] for i, c in enumerate(s))

# ==================== DNS ====================
_DNS_CACHE: Dict[str, Tuple[str,float]] = {}
_DNS_CACHE_TTL = 300.0

async def resolve_host_cloudflare(hostname: str) -> str:
    if not hostname: return hostname
    parts = hostname.split('.')
    if len(parts)==4 and all(p.isdigit() for p in parts): return hostname
    now = time.time()
    if hostname in _DNS_CACHE:
        ip, exp = _DNS_CACHE[hostname]
        if now < exp: return ip
    try:
        loop = asyncio.get_running_loop()
        info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
        if info:
            ip = info[0][4][0]
            _DNS_CACHE[hostname] = (ip, now + _DNS_CACHE_TTL); return ip
    except Exception: pass
    return hostname

def optimize_tcp_socket(sock):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    except Exception: pass

async def safe_close_writer(writer):
    if not writer: return
    try:
        if not writer.is_closing(): writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=1.5)
    except Exception: pass

def optimize_udp_socket(sock):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    except Exception: pass

# ==================== UDP COMPAT ====================
def _udp_sendto(sock, data, addr):
    try:
        return sock.sendto(data, addr)
    except (BlockingIOError, OSError):
        return 0

async def _udp_recvfrom(loop, sock, n):
    if hasattr(loop, "sock_recvfrom"):
        try:
            return await loop.sock_recvfrom(sock, n)
        except (AttributeError, NotImplementedError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
    fut = loop.create_future()
    fd = sock.fileno()
    def _on_readable():
        if fut.done(): return
        try:
            data = sock.recvfrom(n)
            if not fut.done(): fut.set_result(data)
        except BlockingIOError: return
        except Exception as e:
            if not fut.done(): fut.set_exception(e)
        finally:
            try: loop.remove_reader(fd)
            except Exception: pass
    loop.add_reader(fd, _on_readable)
    try: return await fut
    finally:
        try: loop.remove_reader(fd)
        except Exception: pass

# ==================== HTTP CLIENT ====================
client = httpx.AsyncClient(verify=False, timeout=15.0,
    limits=httpx.Limits(max_connections=300, max_keepalive_connections=150))
_LOGIN_SEMAPHORE = asyncio.Semaphore(10)

# ==================== CRYPTO / PROTOBUF ====================
CRC7_TABLE = bytes([
    0,9,18,27,36,45,54,63,72,65,90,83,108,101,126,119,
    25,16,11,2,61,52,47,38,81,88,67,74,117,124,103,110,
    50,59,32,41,22,31,4,13,122,115,104,97,94,87,76,69,
    43,34,57,48,15,6,29,20,99,106,113,120,71,78,85,92,
    100,109,118,127,64,73,82,91,44,37,62,55,8,1,26,19,
    125,116,111,102,89,80,75,66,53,60,39,46,17,24,3,10,
    86,95,68,77,114,123,96,105,30,23,12,5,58,51,40,33,
    79,70,93,84,107,98,121,112,7,14,21,28,35,42,49,56,
    65,72,83,90,101,108,119,126,9,0,27,18,45,36,63,54,
    88,81,74,67,124,117,110,103,16,25,2,11,52,61,38,47,
    115,122,97,104,87,94,69,76,59,50,41,32,31,22,13,4,
    106,99,120,113,78,71,92,85,34,43,48,57,6,15,20,29,
    37,44,55,62,1,8,19,26,109,100,127,118,73,64,91,82,
    60,53,46,39,24,17,10,3,116,125,102,111,80,89,66,75,
    23,30,5,12,51,58,33,40,95,86,77,68,123,114,105,96,
    14,7,28,21,42,35,56,49,70,79,84,93,98,107,112,121,
])
_DELTA = 0x9E3779B9
_ROUNDS = 16
_FIELD_SIZES = {0:1,1:2,2:2,3:1,4:2}
_FIELD_NAMES = {0:"sendOption",1:"cmd",2:"orderId",3:"flags",4:"length"}

def crc7_sync(data: bytes) -> int:
    c = 0
    for b in data:
        c = CRC7_TABLE[((2*(c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F

def encode_varint(n: int) -> bytes:
    out = bytearray()
    if n < 0: n &= 0xFFFFFFFFFFFFFFFF
    while True:
        b = n & 0x7F; n >>= 7
        if n: b |= 0x80
        out.append(b)
        if not n: break
    return bytes(out)

def _pb_varint(fn, val): return encode_varint((fn<<3)|0) + encode_varint(val)
def _pb_length(fn, val):
    enc = val.encode('utf-8') if isinstance(val,str) else val
    return encode_varint((fn<<3)|2) + encode_varint(len(enc)) + enc

def create_proto(fields) -> bytes:
    packet = bytearray()
    for field, value in fields.items():
        if isinstance(field, str): field = int(field)
        if isinstance(value, dict):
            packet.extend(_pb_length(field, bytes(create_proto(value))))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    packet.extend(_pb_length(field, bytes(create_proto(item))))
                elif isinstance(item, str):
                    packet.extend(_pb_length(field, item))
                elif isinstance(item, (bytes, bytearray)):
                    packet.extend(_pb_length(field, bytes(item)))
                elif isinstance(item, bool):
                    packet.extend(_pb_varint(field, 1 if item else 0))
                elif isinstance(item, int):
                    packet.extend(_pb_varint(field, item))
        elif isinstance(value, bool):
            packet.extend(_pb_varint(field, 1 if value else 0))
        elif isinstance(value, int):
            packet.extend(_pb_varint(field, value))
        elif isinstance(value, str):
            packet.extend(_pb_length(field, value))
        elif isinstance(value, (bytes, bytearray)):
            packet.extend(_pb_length(field, bytes(value)))
    return bytes(packet)

def encrypt_aes(hex_data: str) -> str:
    cipher = AES.new(AES_KEY, AES.MODE_CBC, AES_IV)
    return cipher.encrypt(pad(bytes.fromhex(hex_data), AES.block_size)).hex()

def decode_varint(data, offset):
    result = 0; shift = 0
    while offset < len(data):
        byte = data[offset]
        result |= (byte & 0x7F) << shift
        offset += 1
        if not (byte & 0x80): return result, offset
        shift += 7
    return None, offset

def decode_protobuf_2(data):
    result = {}; offset = 0; data_len = len(data)
    while offset < data_len:
        header, offset = decode_varint(data, offset)
        if header is None: break
        fn = header >> 3; wt = header & 0x7
        if wt == 0:
            v, offset = decode_varint(data, offset)
            if v is not None: result[fn] = v
        elif wt == 2:
            length, offset = decode_varint(data, offset)
            if length is None: break
            value = data[offset:offset+length]; offset += length
            try:
                s = value.decode('utf-8')
                result[fn] = s if s else value.hex()
            except UnicodeDecodeError:
                nested = decode_protobuf_2(value)
                result[fn] = nested if nested else value.hex()
        elif wt == 1: offset += 8
        elif wt == 3: offset += 4
        else: break
    return result

def extract_raw_fields(data: bytes) -> dict:
    result = {}; offset = 0; data_len = len(data)
    while offset < data_len:
        header, offset = decode_varint(data, offset)
        if header is None: break
        fn = header >> 3; wt = header & 0x7
        if wt == 0:
            v, offset = decode_varint(data, offset)
            if v is not None: result[fn] = v
        elif wt == 2:
            length, offset = decode_varint(data, offset)
            if length is None: break
            if offset + length > data_len: break
            result[fn] = bytes(data[offset:offset+length]); offset += length
        elif wt == 1: offset += 8
        elif wt == 3: offset += 4
        else: break
    return result

def _parse_results_sync(results):
    out = {}
    for r in results:
        fd = {"wire_type": r.wire_type}
        if r.wire_type == "varint": fd["data"] = r.data
        elif r.wire_type in ("string","bytes"): fd["data"] = r.data
        elif r.wire_type == "length_delimited":
            if hasattr(r.data, "results"): fd["data"] = _parse_results_sync(r.data.results)
            elif isinstance(r.data, list): fd["data"] = _parse_results_sync(r.data)
            else: fd["data"] = str(r.data)
        out[str(r.field)] = fd
    return out

def decode_protobuf_dict(data: bytes) -> dict:
    try:
        parsed = Parser().parse(data)
        return _parse_results_sync(parsed)
    except Exception: return {}

async def decode_protobuf_async(data):
    try:
        parsed = Parser().parse(data)
        return json.dumps(_parse_results_sync(parsed))
    except Exception: return "{}"

# ==================== ZIGZAG / ULEB ====================
async def has_ssan_zig(n: int) -> bytes:
    try: n = int(n)
    except Exception: n = 0
    z = (n << 1) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while z >= 0x80:
        out.append((z & 0x7F) | 0x80); z >>= 7
    out.append(z); return bytes(out)

async def uleb_encode(n: int) -> bytes:
    try: n = int(n)
    except Exception: n = 0
    if n < 0: n &= 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b = n & 0x7F; n >>= 7
        if n: b |= 0x80
        out.append(b)
        if not n: break
    return bytes(out)

async def zigzag_encode_signed(n: int) -> bytes:
    try: n = int(n)
    except Exception: n = 0
    zz = (n << 1) ^ (n >> 63)
    if zz < 0: zz = -zz
    zz &= 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b = zz & 0x7F; zz >>= 7
        if zz: b |= 0x80
        out.append(b)
        if not zz: break
    return bytes(out)

def create_packet(Pk, N, K, V):
    if isinstance(K, str): K = bytes.fromhex(K) if len(K)==32 else K.encode()
    if isinstance(V, str): V = bytes.fromhex(V) if len(V)==32 else V.encode()
    PkEnc = AES.new(K, AES.MODE_CBC, V).encrypt(pad(bytes.fromhex(Pk),16)).hex()
    L = len(PkEnc)//2
    hexlen = hex(L)[2:]
    if len(hexlen)%2 != 0: hexlen = '0'+hexlen
    total_pad = 12 - len(N) - len(hexlen)
    if total_pad < 0: total_pad = 0
    return bytes.fromhex(N + ('0'*total_pad) + hexlen + PkEnc)

# ==================== MOVEMENT — 24-byte compact ====================
async def build_movement_packet_2001(key_bytes, msg_key, player_state, x, y, z,
                                     tick, counter, move_state, account_uid,
                                     room_code, phase="game"):
    x_i = max(-32768, min(32767, int(x))) & 0xFFFF
    y_i = max(-32768, min(32767, int(y))) & 0xFFFF

    body = bytes([
        int(player_state) & 0xFF,
        x_i & 0xFF, (x_i >> 8) & 0xFF,
        y_i & 0xFF, (y_i >> 8) & 0xFF,
        int(move_state) & 0xFF,
    ])

    padded = await build_padded(body)
    enc_body = await tea_cbc_encrypt(padded, key_bytes)

    k0 = key_bytes[0]
    k1 = key_bytes[1] if len(key_bytes) > 1 else (k0 ^ 0x02)
    k2 = key_bytes[2] if len(key_bytes) > 2 else (k0 ^ 0x03)
    k3 = key_bytes[3] if len(key_bytes) > 3 else k0
    k4 = key_bytes[4] if len(key_bytes) > 4 else (k0 ^ 0x01)
    k5 = key_bytes[5] if len(key_bytes) > 5 else (k0 ^ 0x01)
    hkey = bytes([k0, k1, k2, k3, k4, k5])

    hdr = bytearray(6)
    hdr[0] = 0x00 ^ hkey[0]
    v = (3 ^ ((hkey[1] << 8) | hkey[2])) & 0xFFFF
    hdr[1] = v & 0xFF; hdr[2] = (v >> 8) & 0xFF
    hdr[3] = 0x01 ^ hkey[3]
    v = (len(enc_body) ^ ((hkey[4] << 8) | hkey[5])) & 0xFFFF
    hdr[4] = v & 0xFF; hdr[5] = (v >> 8) & 0xFF

    pkt = bytearray(24)
    pkt[0] = msg_key & 0xFF
    pkt[1] = 0x00
    pkt[2:8] = hdr
    pkt[8:24] = enc_body
    pkt[1] = await crc7_buff(0, bytes(pkt[2:])) & 0x7F
    return bytes(pkt)

# ==================== NEWBIE (DISABLED) ====================
async def send_newbie_character_tcp(writer, account_id, key, iv):
    if not ENABLE_NEWBIE_CHARACTER:
        return False
    print_info("[👤] Newbie select disabled")
    return False

# ==================== MOVEMENT CONTROLLER ====================
class MovementController:
    def __init__(self, account_id, account_region, key, iv,
                 tcp_writer=None, udp_sock=None, udp_server=None,
                 udp_key_bytes=None, match_code=None):
        self.account_id = int(account_id) if str(account_id).isdigit() else 0
        self.region = str(account_region).upper()
        self.key = key; self.iv = iv
        self.tcp_writer = tcp_writer
        self.udp_sock = udp_sock
        self.udp_server = udp_server
        self.udp_key = udp_key_bytes
        self.match_code = match_code
        self.x = 0.0; self.y = 0.0; self.z = 0.0
        self.tick = 0; self.counter = 0
        self.angle = random.uniform(0, 6.28)
        self.analog_x = 0.0; self.analog_y = 0.0
        self.state = "walk"
        self.speed_mul = random.uniform(MOVEMENT_SPEED_MIN, MOVEMENT_SPEED_MAX)
        self._stop_evt = asyncio.Event()
        self._task = None
        self._last_decision = 0.0
        self._last_speed_change = 0.0
        self._last_ping_t = 0.0
        self._last_jump_t = 0.0
        self._move_interval = 0.10
        self._idle_interval = 0.50
        self._real = CS_REAL_MOVE
        self.srv_pos = None; self._base = None; self._ri = 0; self._t0 = None
        self._born = time.time()
        self._via = None; self._sent_real = 0; self._warned = False

    def note_server_state(self, content):
        """Server echoes our own state in cmd 2001: 3 prefix bytes, then x, y, z varints."""
        try:
            i = 3; vals = []
            for _ in range(3):
                r = 0; sh = 0
                while True:
                    c = content[i]; i += 1; r |= (c & 0x7F) << sh
                    if not c & 0x80: break
                    sh += 7
                vals.append(r)
            self.srv_pos = tuple(vals)
            if self._t0 is None: self._t0 = time.time()
            if self._via != "squad": self._via = "echo"
            _ECHO_BY_SERVER[self.udp_server] = (self.srv_pos, time.time())
        except Exception: pass

    async def _send_real(self):
        if not (self.udp_sock and self.udp_server and self.udp_key): return False
        if self.srv_pos is None:
            # no own echo (yet): borrow the spawn of another bot in the same match (squad spawns together)
            e = _ECHO_BY_SERVER.get(self.udp_server)
            if e and time.time() - self._born > 3.0 and time.time() - e[1] < 120:
                self.srv_pos = e[0]; self._t0 = self._born; self._via = "squad"
        if self.srv_pos is None or self._t0 is None:
            if not self._warned and time.time() - self._born > 15:
                self._warned = True
                print_warning(f"[🚶] uid={self.account_id} no spawn position from server yet (waiting)")
            return False
        n = len(CS_MOVE_LOOP)
        if self._ri % n == 0 or self._base is None: self._base = self.srv_pos
        fr = CS_MOVE_LOOP[self._ri % n]; self._ri += 1
        bx, by, bz = self._base
        ts = int((time.time() - self._t0) * 1000)
        vals = [16777217, bx + fr[0], by + fr[1], bz + fr[2]] + list(fr[3:]) + [ts]
        body = b"".join(_uleb_sync(v) for v in vals)
        nr = (await layouts_from_mask(self.match_code or [1, 3, 4]))[1]
        self.counter = (self.counter + 1) & 0xFFFF
        pkt = await sv_frame(0x64 + (self.counter % 10), nr, 0, 2001, 0, 0, body,
                             self.udp_key, encrypted=False)
        _udp_sendto(self.udp_sock, pkt, self.udp_server)
        self._sent_real += 1
        if self._sent_real == 1:
            print_success(f"[🚶] uid={self.account_id} REAL movement started (spawn from {self._via}) pos={self.srv_pos}")
        return True

    def _pick_state(self) -> str:
        if self._real: return "run"
        r = random.random(); c = MOVEMENT_STATE_IDLE_CHANCE
        if r < c: return "idle"
        c += MOVEMENT_STATE_WALK_CHANCE
        if r < c: return "walk"
        c += MOVEMENT_STATE_RUN_CHANCE
        if r < c: return "run"
        c += MOVEMENT_STATE_SPRINT_CHANCE
        if r < c: return "sprint"
        c += MOVEMENT_STATE_JUMP_CHANCE
        if r < c: return "jump"
        c += MOVEMENT_STATE_CROUCH_CHANCE
        if r < c: return "crouch"
        return "prone"

    def _update_circle_analog(self, dt):
        self.angle += (2.0 * math.pi / 6.0) * dt
        tx = 0.9 * math.cos(self.angle); ty = 0.9 * math.sin(self.angle)
        self.analog_x = max(-1.0, min(1.0, self.analog_x * 0.5 + tx * 0.5))
        self.analog_y = max(-1.0, min(1.0, self.analog_y * 0.5 + ty * 0.5))

    def _update_random_analog(self):
        ax, ay, _ = random.choice(ANALOG_DIRECTIONS[:-1])
        self.analog_x = ax * random.uniform(0.7, 1.0)
        self.analog_y = ay * random.uniform(0.7, 1.0)

    def _step_position(self, dt):
        base = STATE_SPEED.get(self.state, 40)
        speed = base * self.speed_mul
        if speed <= 0 or (self.analog_x == 0.0 and self.analog_y == 0.0): return
        mag = (self.analog_x ** 2 + self.analog_y ** 2) ** 0.5
        nx = self.analog_x / mag if mag > 0 else 0.0
        ny = self.analog_y / mag if mag > 0 else 0.0
        self.x += nx * speed * dt; self.y += ny * speed * dt
        dist = (self.x ** 2 + self.y ** 2) ** 0.5
        if dist > MOVEMENT_RADIUS:
            ratio = MOVEMENT_RADIUS / dist
            self.x *= ratio; self.y *= ratio

    async def _send_udp(self):
        if not self.udp_sock or not self.udp_server or not self.udp_key: return False
        if self._real:
            try:
                ok = await self._send_real()
                if ok or self.srv_pos is not None or time.time() - self._born < 25:
                    return ok
                # no server state echo for 25 s (unknown mode) -> fall back to the legacy packet below
            except Exception: return False
        try:
            self.tick = (self.tick + 1) & 0xFFFF
            self.counter = (self.counter + 1) & 0xFFFF
            p_state = PLAYER_STATE_MAP.get(self.state, 1)
            m_state = MOVE_STATE_MAP.get(self.state, 1)
            msg_key = 0x64 + (self.tick % 10)
            pkt = await build_movement_packet_2001(
                self.udp_key, msg_key, p_state,
                int(self.x), int(self.y), int(self.z),
                self.tick, self.counter, m_state,
                self.account_id, self.match_code or [], phase="game")
            _udp_sendto(self.udp_sock, pkt, self.udp_server)
            return True
        except Exception: return False

    async def _send_ping(self):
        if not self.udp_sock or not self.udp_server or not self.udp_key: return
        try:
            nr = (await layouts_from_mask(self.match_code or [1,3,4]))[1]
            ping_pkt = await build_packet(
                0x6D, nr, 0, 3, None, 0,
                struct.pack("<I", int(time.time()*1000) & 0xFFFFFFFF) + b"\x00\x00\x00",
                self.udp_key, encrypted=False)
            _udp_sendto(self.udp_sock, ping_pkt, self.udp_server)
        except Exception: pass

    async def _do_jump(self):
        old = self.state
        self.state = "run"
        await self._send_udp()
        await asyncio.sleep(0.1)
        self.state = "jump"
        try:
            await self._send_udp()
        finally:
            self.state = old

    async def _loop(self):
        await asyncio.sleep(CS_MOVE_START_DELAY if (self._real and GAME_MODE == "CS") else ANTI_AFK_SPAWN_DELAY)
        print_success(f"[🚶] Anti-AFK START | uid={self.account_id} pattern={MOVEMENT_PATTERN}")
        last_t = asyncio.get_event_loop().time()
        sent = 0; fail = 0
        while not self._stop_evt.is_set():
            now = asyncio.get_event_loop().time()
            dt = now - last_t; last_t = now

            if now - self._last_ping_t >= ANTI_AFK_PING_EVERY:
                self._last_ping_t = now
                await self._send_ping()

            if now - self._last_jump_t >= ANTI_AFK_JUMP_EVERY:
                self._last_jump_t = now
                await self._do_jump()
                print_info(f"[🚶] jump (anti-AFK) uid={self.account_id}")

            if now - self._last_decision >= ANTI_AFK_DECISION_SEC:
                self.state = self._pick_state()
                self._last_decision = now
                print_info(f"[🚶] state={self.state} speed_mul={self.speed_mul:.2f}")

            if now - self._last_speed_change >= MOVEMENT_UPDATE_SPEED_EVERY:
                self.speed_mul = random.uniform(MOVEMENT_SPEED_MIN, MOVEMENT_SPEED_MAX)
                self._last_speed_change = now

            if self.state not in ("idle","prone"):
                if MOVEMENT_PATTERN == "circle":
                    self._update_circle_analog(dt)
                elif MOVEMENT_PATTERN == "zigzag":
                    if int(now) % 2 == 0:
                        self.analog_x = 0.8 * (1 if int(now) % 4 < 2 else -1)
                        self.analog_y = 0.9
                else:
                    if now - self._last_decision < 0.1:
                        self._update_random_analog()
            else:
                self.analog_x = 0.0; self.analog_y = 0.0

            self._step_position(dt)
            self.z += random.uniform(-0.2, 0.2); self.z = max(-30, min(30, self.z))

            ok = await self._send_udp()
            if ok: sent += 1; fail = 0
            else:
                fail += 1
                if fail % 30 == 0: print_warning(f"[🚶] UDP gagal x{fail}")

            interval = self._idle_interval if self.state in ("idle","prone") else self._move_interval
            try: await asyncio.wait_for(self._stop_evt.wait(), timeout=interval)
            except asyncio.TimeoutError: pass
        print_info(f"[🚶] Anti-AFK STOP | sent={sent}")

    def start(self):
        if self._task and not self._task.done(): return
        self._stop_evt.clear()
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._stop_evt.set()
        if self._task:
            try: await asyncio.wait_for(self._task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

# ==================== TEAM PACKETS ====================
def create_team(key, iv, region="ID", game_version="1.132.9"):
    region = (str(region).upper() if region else "ID")
    fields = {
        1: 1,
        2: {
            2: bytes.fromhex("01"), 3: 1, 4: 3, 5: "en",
            8: {1: "IDC3", 2: 123, 3: region},
            9: 1, 10: bytes.fromhex("01090a0b1219202729"),
            11: 2, 13: 1,
            14: {
                1: "08FBA33B4BABE2790205390333330000009700050092005F72697EB2134DCBB2467625142201040e38e848dc0e748c3f6aa04d81000000ff1113090dcacfa16d",
                2: 478, 3: "7e585d55", 4: "y[Z]", 6: 12,
                7: "15686677735a7506101c",
                8: str(game_version), 9: 3, 10: 1,
                11: "03626253513637754174",
            },
            19: 329, 21: bytes.fromhex("374f521955"),
            24: {1: 21}, 27: "a_6534489873065906362",
        },
    }
    return create_packet(create_proto(fields).hex(), "0515", key, iv)

def team_fresh(account_id, key, iv):
    return create_packet(create_proto({1: 72, 2: {1: int(account_id)}}).hex(), "0515", key, iv)

def invite_player_in_team(target_uid, key, iv, region="ID"):
    region = (str(region).upper() if region else "ID")
    return create_packet(
        create_proto({1: 2, 2: {1: int(target_uid), 2: region, 4: 1}}).hex(),
        "0515", key, iv)

def accept_team_invite(target_uid, group_id, key, iv, region="ID", game_version="1.132.9"):
    region = (str(region).upper() if region else "ID")
    fields = {
        1: 4,
        2: {
            1: int(target_uid), 3: int(target_uid),
            4: bytes.fromhex("0107090a0b120f19202729"),
            8: 1,
            9: {
                1: "08FBA33B0522052002041B0222220007000C00010004000547E265AE1080779546762514110104076fa2e8770e748c3f6a68ed75000000ff08080500cacfa16d",
                2: 235, 3: {14: 80, 11: 86},
                4: "{Y\\R", 6: 13, 7: {2: 4},
                8: str(game_version), 9: 2, 10: 1, 11: "0362625351",
            },
            10: str(group_id),
            11: {1: "IDC3", 2: 107, 3: region},
            13: "en", 16: "374f5219",
            20: {1: 21}, 23: "a_6534489873065906362",
            24: "https://dl-sg-production.freefiremobile.com/",
            27: {1: 2, 2: 4},
        },
    }
    return create_packet(create_proto(fields).hex(), "0515", key, iv)

def start_match_team(account_id, key, iv, region="ID"):
    region = (str(region).upper() if region else "ID")
    fields = {
       1: 9, 2: {1: int(account_id), 7: {1: "IDC3", 2: 410, 3: region}}
    }
    return create_packet(create_proto(fields).hex(), "0515", key, iv)

def ready_for_game(uid, key, iv):
    fields = {1: 15, 2: {1: uid, 2: 1}}
    return create_packet(create_proto(fields).hex(), "0519", key, iv)

# ==================== KEY EXTRACT ====================
class _Fixed64:
    def __init__(self, value): self.value = value

async def _cs_enc_varint(n): return encode_varint(n)

async def _cs_build_proto(fields):
    packet = bytearray()
    for field, value in fields.items():
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    nested = await _cs_build_proto(item)
                    packet.extend(await _cs_enc_varint((field << 3) | 2))
                    packet.extend(await _cs_enc_varint(len(nested)))
                    packet.extend(nested)
                elif isinstance(item, _Fixed64):
                    packet.extend(await _cs_enc_varint((field << 3) | 1))
                    packet.extend(struct.pack("<Q", item.value))
                elif isinstance(item, int):
                    packet.extend(await _cs_enc_varint((field << 3) | 0))
                    packet.extend(await _cs_enc_varint(item))
                elif isinstance(item, (str, bytes)):
                    enc_item = item.encode() if isinstance(item, str) else item
                    packet.extend(await _cs_enc_varint((field << 3) | 2))
                    packet.extend(await _cs_enc_varint(len(enc_item)))
                    packet.extend(enc_item)
        elif isinstance(value, dict):
            nested = await _cs_build_proto(value)
            packet.extend(await _cs_enc_varint((field << 3) | 2))
            packet.extend(await _cs_enc_varint(len(nested)))
            packet.extend(nested)
        elif isinstance(value, _Fixed64):
            packet.extend(await _cs_enc_varint((field << 3) | 1))
            packet.extend(struct.pack("<Q", value.value))
        elif isinstance(value, int):
            packet.extend(await _cs_enc_varint((field << 3) | 0))
            packet.extend(await _cs_enc_varint(value))
        elif isinstance(value, (str, bytes)):
            encoded = value.encode() if isinstance(value, str) else value
            packet.extend(await _cs_enc_varint((field << 3) | 2))
            packet.extend(await _cs_enc_varint(len(encoded)))
            packet.extend(encoded)
    return bytes(packet)

def _extract_key_iv(res_json):
    if "__RAW_HEX__" in res_json and "__OFFSET__" in res_json:
        try:
            raw = bytes.fromhex(res_json["__RAW_HEX__"])
            off = int(res_json["__OFFSET__"])
            rf = extract_raw_fields(raw[off:])
            for kf, vf in [(22,23),(24,25),(6,7)]:
                k = rf.get(kf); v = rf.get(vf)
                if isinstance(k,(bytes,bytearray)) and len(k)==16 and \
                   isinstance(v,(bytes,bytearray)) and len(v)==16:
                    return bytes(k), bytes(v)
        except Exception: pass
    found = {}
    for k, v in res_json.items():
        if str(k).startswith("__"): continue
        if not isinstance(v, dict): continue
        data = v.get("data")
        if data is None: continue
        raw = None
        if isinstance(data, (bytes,bytearray)) and len(data)==16: raw = bytes(data)
        elif isinstance(data, str) and len(data)==32:
            try:
                c = bytes.fromhex(data)
                if len(c)==16: raw = c
            except Exception: pass
        if raw and len(raw)==16:
            try: found[int(k)] = raw
            except Exception: pass
    if len(found) < 2: return None, None
    for kf, vf in [(22,23),(24,25),(6,7)]:
        if kf in found and vf in found: return found[kf], found[vf]
    sk = sorted(found.keys())
    return found[sk[0]], found[sk[1]]

async def aes_encrypt(payload, key, iv):
    return AES.new(key, AES.MODE_CBC, iv).encrypt(pad(payload, AES.block_size))

# ==================== VERSION / LOGIN ====================
def get_app_version():
    try: return play_scraper('com.dts.freefireth').get('version')
    except Exception: return "1.132.9"

async def version_config(region="ID"):
    app_version = get_app_version()
    lang = REGION_LANG.get(region.upper(), "en")
    url = (f"https://version.ggwhitehawk.com/live/ver.php?version={app_version}"
           f"&lang={lang}&device=android&channel=android&appstore=googleplay"
           f"&region={region.upper()}&whitelist_version=1.3.0&whitelist_sp_version=1.0.0")
    try:
        r = await client.get(url)
        j = r.json()
        server_url = j.get("server_url")
        remote_version = j.get("remote_version")
        latest_release_version = j.get("latest_release_version")
        gops = [u.strip() for u in j.get("gop_url","").split(";") if u.strip()]
        gop_1 = gops[0] if gops else "https://ffmconnect.ppmainecoonghj.com"
        if not server_url: return None
        return latest_release_version, remote_version, server_url, gop_1, app_version
    except Exception as e:
        print_error(f"[VERCONFIG] {e}"); return None

async def get_access_token(uid, password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    hdrs = {"Host":"100067.connect.garena.com",
        "User-Agent":"Dalvik/2.1.0 (Linux; U; Android 12; SM-G998B)",
        "Content-Type":"application/x-www-form-urlencoded",
        "Accept-Encoding":"gzip, deflate, br","Connection":"close"}
    data = {"uid":str(uid),"password":password,"response_type":"token","client_type":"2",
        "client_secret":HEX_KEY,"client_id":"100067"}
    for _ in range(4):
        try:
            r = await client.post(url, headers=hdrs, data=data)
            if r.status_code == 200:
                j = r.json()
                oid = j.get("open_id"); at = j.get("access_token"); pf = j.get("platform",4)
                if oid and at: return oid, at, pf
        except Exception: pass
        await asyncio.sleep(0.7)
    return None

def generate_signature(payload_str):
    return hmac.new(API_KEY, payload_str.encode(), hashlib.sha256).hexdigest()

async def register_account(password, gop_url, app_version):
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    rp = {"app_id":100067,"client_type":2,"password":password,"source":2}
    payload = json.dumps(rp, separators=(',',':'))
    sig = generate_signature(payload)
    headers = {
        "User-Agent": f"GarenaMSDK/4.0.44(SM-E135F ;Android 14;en;GB;app {app_version} 2019118525;)",
        "Authorization": f"Signature {sig}",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8"}
    try:
        async with aiohttp.ClientSession(connector=connector) as s:
            async with s.post(f"{gop_url}/api/v2/oauth/guest:register",
                              headers=headers, data=payload,
                              timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = json.loads(await resp.text())
                if data.get("code") == 0: return data["data"]["uid"]
    except Exception as e: print_error(f"[REGISTER] {e}")
    return None

async def grant_token(uid, password, gop_url, app_version):
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    gp = {"client_id":100067,"client_secret":HEX_KEY,"client_type":2,
          "device_id":f"02-{uuid.uuid4()}","password":password,
          "response_type":"token","uid":int(uid)}
    payload = json.dumps(gp, separators=(',',':'))
    sig = generate_signature(payload)
    headers = {
        "User-Agent": f"GarenaMSDK/4.0.44(SM-E135F ;Android 14;en;GB;app {app_version} 2019118525;)",
        "Authorization": f"Signature {sig}",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8"}
    try:
        async with aiohttp.ClientSession(connector=connector) as s:
            async with s.post(f"{gop_url}/api/v2/oauth/guest/token:grant",
                              headers=headers, data=payload,
                              timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = json.loads(await resp.text())
                if data.get("code") == 0: return data["data"]
    except Exception as e: print_error(f"[GRANT] {e}")
    return None

# ==================== PAYLOAD ====================
async def get_payload(access_token, open_id, platform, version, lang_code="en"):
    device_id = f"Google|{uuid.uuid4()}"
    major_login = {
        "3": str(datetime.now())[:-7], "4": "free fire", "5": 1,
        "7": f"{version}",
        "8": "Android OS 14 / API-34 (UP1A.231005.007/E135FXXSEEZE2)",
        "9": "Handheld", "11": "CarrierDataNetwork",
        "12": 1672, "13": 750, "14": "338",
        "15": "ARMv7 VFPv3 NEON | 2002 | 8",
        "16": 3702, "17": "Mali-G52",
        "18": "OpenGL ES 3.2 v1.r38p1-01bet0-mbs2v41_0.6d20ec041e51b2f2d25dfc265586ebe8",
        "19": device_id, "20": "104.28.197.151", "21": lang_code,
        "22": f"{open_id}", "23": f"{platform}", "24": "Handheld",
        "25": "samsung SM-E135F", "26": "ID",
        "29": f"{access_token}", "30": 1, "42": "Cellular",
        "57": "7428b253defc164018c604a1ebbfebdf",
        "60": 52037, "61": 5355, "62": 3723, "63": 3,
        "64": 5483, "65": 52037, "66": 5483, "67": 52037,
        "73": 2,
        "74": "/data/app/~~yACsV8QOk9OexgajWQP30A==/com.dts.freefireth-Bi_wPVrrhYM2nYncc29q7Q==/lib/arm",
        "76": 1,
        "77": "b8e0cd5e295eee42f5860d3c86e483dd|/data/app/~~yACsV8QOk9OexgajWQP30A==/com.dts.freefireth-Bi_wPVrrhYM2nYncc29q7Q==/base.apk",
        "78": 3, "79": 1, "81": "32", "83": "2019121229",
        "86": "OpenGLES2", "87": 8191, "88": int(platform),
        "92": 16091, "93": "android",
        "94": "KqsHT3Yeqo3RLRl8efNiL3aL/1SHwlSrtK0DwNAqdkNBQz70TeVy+icaIwuR3UK4b65Uf3IG5xBFEv0uu+jQZI+rOYCmNCsXjJT4QcZaYgoaaUGG",
        "95": 111107,
        "96": "{\"cur_rate\":null,\"support_etc2\":false}",
        "97": 1, "98": 1,
        "99": f"{platform}", "100": f"{platform}",
        "102": "4503454457550c0766",
        "104": 53792, "105": 1,
        "106": "https://dl-bs.ggpolarbear.com/live/ABHotUpdates/|https://core-bs.ggpolarbear.com/live/ABHotUpdates/|6b2078db9d22dd98f8e9386a39af8462",
        "107": "c8e41b7a93f02d56e1a94c7b8203f5d1",
    }
    get_login_data = dict(major_login)
    get_login_data["90"] = "Khulna"
    get_login_data["91"] = "D"
    get_login_data["99"] = "0"
    get_login_data["103"] = 1
    proto1 = create_proto(major_login).hex()
    proto2 = create_proto(get_login_data).hex()
    return bytes.fromhex(encrypt_aes(proto1)), bytes.fromhex(encrypt_aes(proto2))


# ═══════════════════════════════════════════════════════════════════
#  ⭐ MAJOR LOGIN — CLEAN REWRITE (packet-template aligned)
# ═══════════════════════════════════════════════════════════════════
#  Reference: PACKET 02 (request) + PACKET 03 (response) in template dump.
#
#  Flow:
#     AES-CBC(payload) ──► POST /MajorLogin ──► scan raw protobuf response
#                     ──► pick best candidate ──► attach __RAW_HEX__ / __OFFSET__
#
#  Callers expect:
#     • dict containing "8" (JWT token) + "10" (url), with __RAW_HEX__ / __OFFSET__
#     • None on any failure
#
#  Response fields consumed by app.py (see thunderFF_pb2.MajorLoginRes):
#     field 1  account_id
#     field 2  region
#     field 8  token        (JWT — REQUIRED)
#     field 10 url          (game server URL)
#     field 21 server_time
#     field 22 aes_ak       (AES key — 16 bytes)
#     field 23 iv_i         (AES IV  — 16 bytes)
# ═══════════════════════════════════════════════════════════════════

_MAJOR_LOGIN_BASE_HEADERS = {
    "Accept": "*/*",
    "Authorization": "Bearer ",
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "UnityPlayer/2018.4.12f1",
    "X-GA": "v1 1",
    "X-Unity-Version": "2018.4.12f1",
}

# Offsets tried, in order, when scanning the raw response for a valid message.
_MAJOR_LOGIN_OFFSETS = [64, 0, 48, 32, 80, 16, 128, 8, 96, 60, 56, 40, 24, 20, 12, 4]

# Minimum acceptable response length before we bother decoding.
_MAJOR_LOGIN_MIN_LEN = 200

# Per-request timeout (seconds).
_MAJOR_LOGIN_TIMEOUT = 15.0


def _major_login_decode_at(raw: bytes, offset: int) -> Optional[dict]:
    """Try to decode raw[offset:] as a protobuf message. Returns dict or None."""
    if offset >= len(raw):
        return None
    body = raw[offset:]

    # Fast path — dedicated protobuf decoder
    try:
        decoded = decode_protobuf_dict(body)
        if decoded:
            return decoded
    except Exception:
        pass

    # Fallback — manual wire-format decoder
    try:
        decoded = decode_protobuf_2(body)
        if decoded:
            return {str(k): {"data": v} for k, v in decoded.items()}
    except Exception:
        pass

    return None


def _major_login_score(candidate: dict) -> int:
    """Score higher for responses with more fields, especially JWT (8) + URL (10)."""
    keys = {str(k) for k in candidate.keys()}
    score = len(candidate)
    if "8" in keys:
        score += 100
    if "10" in keys:
        score += 50
    return score


async def major_login(payload: bytes, login_url: str, release_version: str) -> Optional[dict]:
    """
    POST an AES-CBC encrypted payload to <login_url>/MajorLogin and decode
    the protobuf response.

    Returns the best-scoring decoded dict with __RAW_HEX__ and __OFFSET__
    attached (so _extract_key_iv() can pull aes_ak / iv_i), or None on failure.
    The returned dict is guaranteed to contain field "8" (JWT token).
    """
    url = f"{login_url.rstrip('/')}/MajorLogin"

    # Build request headers (ReleaseVersion + X-GA-SV are per-request)
    headers = dict(_MAJOR_LOGIN_BASE_HEADERS)
    headers["ReleaseVersion"] = str(release_version)
    headers["X-GA-SV"] = str(int(time.time()))

    # SSL context — game server cert is not trusted by default
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_MAJOR_LOGIN_TIMEOUT)
        ) as session:
            async with session.post(
                url, headers=headers, data=payload, ssl=ssl_ctx
            ) as resp:
                raw = await resp.read()
                print_info(f"[MAJOR-LOGIN] status={resp.status} len={len(raw)}")

                if resp.status != 200 or len(raw) < _MAJOR_LOGIN_MIN_LEN:
                    print_warning(
                        f"[MAJOR-LOGIN] rejected "
                        f"(status={resp.status}, len={len(raw)})"
                    )
                    return None

                # Scan every candidate offset — pick the best-scoring decode
                best: Optional[dict] = None
                best_score = -1
                best_off = 0

                for off in _MAJOR_LOGIN_OFFSETS:
                    cand = _major_login_decode_at(raw, off)
                    if not cand:
                        continue

                    cand_keys = {str(k) for k in cand.keys()}
                    score = _major_login_score(cand)

                    if score > best_score:
                        best, best_score, best_off = cand, score, off

                    # Perfect hit — both JWT and URL present, stop scanning
                    if "8" in cand_keys and "10" in cand_keys:
                        break

                if not best:
                    print_warning("[MAJOR-LOGIN] no valid protobuf body found")
                    return None

                best_keys = {str(k) for k in best.keys()}
                if "8" not in best_keys:
                    print_warning("[MAJOR-LOGIN] response missing token field (8)")
                    return None

                # Attach raw bytes so _extract_key_iv() can pull aes_ak / iv_i
                best["__RAW_HEX__"] = raw.hex()
                best["__OFFSET__"] = best_off

                print_success(
                    f"[MAJOR-LOGIN] OK | off={best_off} "
                    f"fields={len([k for k in best if not str(k).startswith('__')])}"
                )
                return best

    except asyncio.CancelledError:
        raise
    except Exception as e:
        print_error(f"[MAJOR-LOGIN] {e}")
        return None


# ==================== GETLOGINDATA ====================
async def get_login_data(payload, jwt_token, server_url, release_version):
    url = f"{server_url.rstrip('/')}/GetLoginData"
    hdrs = {"Accept":"*/*","Authorization":f"Bearer {jwt_token}",
        "Content-Type":"application/x-www-form-urlencoded",
        "ReleaseVersion":f"{release_version}",
        "User-Agent":"UnityPlayer/2018.4.12f1","X-GA":"v1 1",
        "X-GA-SV":str(int(time.time())),"X-Unity-Version":"2018.4.12f1"}
    try:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.post(url, headers=hdrs, data=payload, ssl=ssl_ctx) as r:
                if r.status != 200: return None
                content = await r.read()
                for off in [0,64,48,32,80,16,128,8,96]:
                    if off >= len(content): continue
                    try:
                        c = decode_protobuf_dict(content[off:])
                        if c and len(c) >= 3: return c
                    except Exception: pass
                for off in [0,64,48,32,80]:
                    if off >= len(content): continue
                    c = decode_protobuf_2(content[off:])
                    if c and len(c) >= 3:
                        return {str(k):{"data":v} for k,v in c.items()}
        return None
    except Exception: return None

async def get_player_personal_show(target_uid, jwt_token, server_url, release_version):
    url = f"{server_url}/GetPlayerPersonalShow"
    payload = bytes.fromhex(encrypt_aes(create_proto({"1":int(target_uid)}).hex()))
    hdrs = {"authorization":f"Bearer {jwt_token}",
        "host":server_url.replace("https://","").split("/")[0],
        "x-ga":"v1 1","content-type":"application/x-www-form-urlencoded",
        "releaseversion":f"{release_version}",
        "user-agent":"UnityPlayer/2022.3.47f1","accept":"*/*",
        "x-unity-version":"2022.3.47f1"}
    try:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as s:
            async with s.post(url, headers=hdrs, data=payload, ssl=ssl_ctx) as r:
                if r.status != 200: return None
                return decode_protobuf_2(await r.read())
    except Exception: return None

# ==================== CLAN ====================
def _get_region_from_jwt(jwt_token):
    try:
        p = jwt_token.split('.')[1]
        p += '=' * (-len(p) % 4)
        d = json.loads(base64.urlsafe_b64decode(p))
        return (d.get('lock_region') or d.get('noti_region') or 'BD').upper()
    except Exception: return "ID"

def _get_region_url(region):
    """Map region code → correct FreeFire server host."""
    r = str(region).upper()
    if r == "IND": return "https://client.ind.freefiremobile.com"
    if r == "BD":  return "https://clientbp.ggwhitehawk.com"
    if r in ("BR", "US", "NA"): return "https://client.us.freefiremobile.com"
    if r == "SAC": return "https://client.br.freefiremobile.com"
    if r == "TW":  return "https://client.tw.freefiremobile.com"
    if r == "CIS": return "https://client.cis.freefiremobile.com"
    if r in ("ME", "PK"): return "https://clientbp.ggblueshark.com"
    return "https://clientbp.ggpolarbear.com"

def _create_clan_payload(clan_id):
    if not _CLAN_PB_AVAILABLE: return None
    try:
        msg = reqClan_pb2.MyMessage()
        msg.field_1 = int(clan_id)
        raw = msg.SerializeToString()
        return AES.new(AES_KEY, AES.MODE_CBC, AES_IV).encrypt(pad(raw, AES.block_size))
    except Exception as e:
        print_error(f"[CLAN] payload: {e}"); return None

async def join_clan(jwt_token, clan_id):
    if not _CLAN_PB_AVAILABLE: return False
    try:
        region = _get_region_from_jwt(jwt_token)
        base_url = _get_region_url(region)
        url = base_url.rstrip("/") + "/RequestJoinClan"
        host = base_url.replace("https://","")
        payload = _create_clan_payload(clan_id)
        if not payload: return False
        hdrs = {"Authorization":f"Bearer {jwt_token}",
            "X-Unity-Version":"2018.4.11f1","X-GA":"v1 1","ReleaseVersion":"OB55",
            "Content-Type":"application/octet-stream","Host":host,
            "Connection":"Keep-Alive","Accept-Encoding":"gzip"}
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.post(url, headers=hdrs, data=payload, ssl=ssl_ctx) as r:
                if r.status == 200:
                    print_success(f"[CLAN] ✅ Join sent for {clan_id}"); return True
                print_warning(f"[CLAN] Join status {r.status}"); return False
    except Exception as e:
        print_error(f"[CLAN] {e}"); return False

# ==================== TCP ====================
async def build_tcp_startup_packet(account_id, token, server_time, key, iv, region="ID", typ='OnLine'):
    uid_hex = f"{int(account_id):016x}"
    ts_hex = f"{int(server_time):08x}"
    enc = (await aes_encrypt(token.encode(), key, iv)).hex()
    enc_len = f"{len(enc)//2:08x}"
    reg = str(region).upper() if region else "ID"
    if typ == 'OnLine':
        pfx = '7219' if reg == 'BD' else ('7214' if reg == 'IND' else '7215')
        return f"{pfx}{uid_hex}{ts_hex}00000000{enc_len}{enc}"
    pfx = '8119' if reg == 'BD' else ('8114' if reg == 'IND' else '8115')
    return f"{pfx}{uid_hex}{ts_hex}{enc_len}{enc}"

async def send_keep_alive(region="ID"):
    reg = str(region).upper() if region else "ID"
    return bytes.fromhex("0219" if reg == "ID" else ("0214" if reg == "IND" else "0215"))

async def start_game_battle_royale(region, client_version, writer, key, iv):
    packet = bytes.fromhex("0314000001e0a8c1becfcdd1a311df1b23e218f8b6ae1df958f76dfeb6bfd0399d153d1f9687174c189de3900d11f3eb8fd92cd831c482b06adec14632935703a0423b76051274d67f1381e5794cc6256079d1514ad9f9fa864733257d8e5910e9ba2f048dbe5008ba23d7e4db609ceb46ab823a50a15d0692d44c73d1f6c8e9ee33462952f2685ef8b32faea409a9b61ea091f3a36a727773dbb8316c01dfa6512dbe5b3a3ce6c8310783386c9c70c8fe143f6bc39854fdd1fffd157e52ef006a9e1e944b252a0072154aa34a45b7a5f080d7043eaf0ec6dd884b62b906781cdecca9d49f50f521c4a57dcefc4e5c274f242dcad6489dae6795c2173803fb1d343f64d47699164508e95709a43f018efca4eab791ff4010aab17d1926f7d5e11983249879940d90d5f257ad66dc0e193716addb278510de0afa48825d24ec20724382d731a2f2258ace8a4e614bfb620dc90ee9c3a87e5deaed1d44efdb561e320bc9faab91aea632d060f4ffa268067ab588edd6342c086059accb30d036b3cf766858f54b434d446935912e0e9c21b7e22ace9f17852366bd2390988f82545239e54baafcfd7bcd75d8a69a327019f8b608d80a49cbb6a65b31eec3fcab74afee3da4a9d59e367edd3f32d8eb0b8b516bcfbc1e104bd8c8bda5b76f8e5190ec1898d004dc")
    try:
        proto = thunderFF_pb2.StartMatch()
        proto.ParseFromString(packet)
        if hasattr(proto.main, 'region_list') and len(proto.main.region_list) > 0:
            proto.main.region_list[0].region = region
            if len(proto.main.region_list) > 1:
                proto.main.region_list[1].region = region
        if hasattr(proto.main, 'client_version'):
            proto.main.client_version.remote_version = client_version
        p = proto.SerializeToString()
    except Exception:
        p = packet
    raw_proto_hex = p.hex()
    reg = str(region).upper() if region else "BD"
    pkt_prefix = '0319' if reg == 'BD' else ('0314' if reg == 'IND' else '0315')
    packet_bytes = bytes.fromhex(raw_proto_hex)
    encrypted_packet = (await aes_encrypt(packet_bytes, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = f"{packet_length:08x}"
    writer.write(bytes.fromhex(pkt_prefix + hex_length + encrypted_packet))
    await writer.drain()
    print_info(f"[⚔] CS Match search sent ({packet_length}B)")

# ==================== UDP HELPERS ====================
async def uleb_async(n): return await uleb_encode(n)

async def tea_enc(v0, v1, k0, k1, k2, k3):
    s = 0
    for _ in range(_ROUNDS):
        s = (s + _DELTA) & 0xFFFFFFFF
        v0 = (v0 + (((((v1<<4)&0xFFFFFFFF)+k0)&0xFFFFFFFF ^
                      ((v1+s)&0xFFFFFFFF) ^
                      (((v1>>5)+k1)&0xFFFFFFFF)))) & 0xFFFFFFFF
        v1 = (v1 + (((((v0<<4)&0xFFFFFFFF)+k2)&0xFFFFFFFF ^
                      ((v0+s)&0xFFFFFFFF) ^
                      (((v0>>5)+k3)&0xFFFFFFFF)))) & 0xFFFFFFFF
    return v0, v1

async def tea_dec(v0, v1, k0, k1, k2, k3):
    s = (_DELTA * _ROUNDS) & 0xFFFFFFFF
    for _ in range(_ROUNDS):
        v1 = (v1 - (((((v0<<4)&0xFFFFFFFF)+k2)&0xFFFFFFFF ^
                      ((v0+s)&0xFFFFFFFF) ^
                      (((v0>>5)+k3)&0xFFFFFFFF)))) & 0xFFFFFFFF
        v0 = (v0 - (((((v1<<4)&0xFFFFFFFF)+k0)&0xFFFFFFFF ^
                      ((v1+s)&0xFFFFFFFF) ^
                      (((v1>>5)+k1)&0xFFFFFFFF)))) & 0xFFFFFFFF
        s = (s - _DELTA) & 0xFFFFFFFF
    return v0, v1

async def tea_cbc_encrypt(padded, key_bytes):
    k0,k1,k2,k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0,4,8,12))
    out = bytearray(len(padded))
    pc = bytearray(8); pi = bytearray(8)
    for i in range(0, len(padded), 8):
        x = bytearray(8)
        for j in range(8): x[j] = padded[i+j] ^ pc[j]
        e0,e1 = await tea_enc(struct.unpack_from("<I", x, 0)[0],
                              struct.unpack_from("<I", x, 4)[0], k0,k1,k2,k3)
        enc = bytearray(8)
        struct.pack_into("<I", enc, 0, e0); struct.pack_into("<I", enc, 4, e1)
        for j in range(8): out[i+j] = enc[j] ^ pi[j]
        pc[:] = out[i:i+8]; pi[:] = x
    return bytes(out)

async def tea_cbc_decrypt(body, key_bytes):
    k0,k1,k2,k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0,4,8,12))
    out = bytearray(len(body))
    pi = bytearray(8); pc = bytearray(8)
    x = bytearray(8); dec = bytearray(8)
    for i in range(0, len(body), 8):
        for j in range(8): x[j] = body[i+j] ^ pi[j]
        d0,d1 = await tea_dec(struct.unpack_from("<I", x, 0)[0],
                              struct.unpack_from("<I", x, 4)[0], k0,k1,k2,k3)
        struct.pack_into("<I", dec, 0, d0); struct.pack_into("<I", dec, 4, d1)
        for j in range(8): out[i+j] = dec[j] ^ pc[j]
        pc[:] = body[i:i+8]; pi[:] = dec
    return bytes(out)

async def build_padded(content):
    pad_len = (8 - (len(content) + 10) % 8) % 8
    return bytes([pad_len,0,0]) + b"\x00"*pad_len + content + b"\x00"*7

async def encode_header(layout, so, cmd, oid, flags, length, k, v80):
    out = bytearray()
    for code in layout:
        val = {0:so,1:cmd,2:oid,3:flags,4:length}[code]
        if _FIELD_SIZES[code] == 1:
            out.append((val & 0xFF) ^ k)
        else:
            v = ((val & 0xFFFF) ^ v80) & 0xFFFF
            out.append(v & 0xFF); out.append((v >> 8) & 0xFF)
    return bytes(out)

async def crc7_buff(crc, buf):
    c = crc & 0x7F
    for b in buf:
        c = CRC7_TABLE[((2*(c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F

async def sv_frame(msg_key, layout, so, cmd, oid, flags, content, key, encrypted=True):
    k = key[0]; v80 = ((k<<8)|k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key,0]) + await encode_header(layout, so, cmd, oid, flags, len(body), k, v80)
    pkt = bytearray(hdr + body)
    pkt[1] = await crc7_buff(0, bytes(pkt[2:])) & 0x7F
    return bytes(pkt)

async def build_match_startup_packets(token, udp_key, match_code, account_id, block_val,
                                      server_ip="", region="ID", client_version="1.132.9",
                                      client_version_code="2019121229",
                                      access_token="", mode_id=1, map_id=1):
    token = token.strip()
    udp_key = bytes.fromhex(udp_key)
    match_code = [int(ch) for ch in str(match_code).strip()]
    _is_cs = int(mode_id) == CS_MODE_ID
    # CS match tokens are longer (~1100 chars). The real client always puts the LAST 40 chars in the
    # loading packet (cmd 448) and the rest in cmd 447. BR keeps the old fixed 660 split.
    _split = max(len(token) - 40, 0) if _is_cs else 660
    thunder_jwt = token[:_split] if len(token) > _split else token
    sharma_jwt = token[_split:] if len(token) > _split else ""
    eth = thunder_jwt.encode() if isinstance(thunder_jwt, str) else thunder_jwt
    esh = sharma_jwt.encode() if isinstance(sharma_jwt, str) else sharma_jwt
    garena420 = await has_ssan_zig(len(eth)) + eth
    reg = str(region).upper() if region else "ID"
    csoversea_block = bytes.fromhex(
        "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
        "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
        "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
        "00000800000100000000000100a8a2d7bebd8d8bdf110200")
    mid = bytes.fromhex('0000000000000102030101' if _is_cs else '0000000001000102030101') + await has_ssan_zig(len(reg)) + reg.encode()
    mid += bytes.fromhex('0001030003000004')
    mid += await has_ssan_zig(len(client_version)) + client_version.encode()
    mid += await has_ssan_zig(len(client_version_code)) + client_version_code.encode()
    mid += csoversea_block
    clean_ip = server_ip.split(':')[0] if server_ip else "0.0.0.0"
    mid += await has_ssan_zig(len(clean_ip)) + clean_ip.encode()
    clean_acc_tok = access_token.strip() if access_token else ""
    if clean_acc_tok:
        mid += await has_ssan_zig(len(clean_acc_tok)) + clean_acc_tok.encode()
    mid += await has_ssan_zig(len(esh)) + esh
    tg = (await uleb_encode(int(account_id)) + await uleb_encode(int(block_val)) +
          await uleb_encode(1) + await uleb_encode(int(mode_id)) +
          await uleb_encode(int(block_val)) + await uleb_encode(int(map_id)) + mid)
    process = await sv_frame(0x5E, match_code, 2, 447, 0, 1, garena420, udp_key)
    loading = await sv_frame(0x5A, match_code, 2, 448, 1, 1, tg, udp_key)
    return process.hex(), loading.hex()

async def build_hello_packet(text, key, layout):
    data = text.encode("utf-8")
    if len(data) > 25: raise ValueError("too long")
    content = b"\x10\x00\x00\x00" + data + b"\x00" * (29 - 4 - len(data))
    k = key[0]; v80 = ((k<<8)|k) & 0xFFFF
    layout = [int(c) for c in str(layout).strip()] if isinstance(layout, str) else list(layout)
    padded = await build_padded(content)
    enc_body = await tea_cbc_encrypt(padded, key)
    hbytes = await encode_header(layout, 1, 1, 0, 1, len(enc_body), k, v80)
    pkt = bytearray([0x63, 0x00]) + hbytes + enc_body
    pkt[1] = await crc7_buff(0, pkt[2:]) & 0x7F
    return bytes(pkt).hex()

async def classify(frame):
    cmd = frame["cmd"]
    msg_name = MESSAGE_ID_TO_NAME.get(cmd, f"UNKNOWN_{cmd}")
    if msg_name == "UDP_HELLO": return "HELLO"
    if msg_name == "UDP_ACK": return "ACK"
    if msg_name == "UDP_PING": return "PING"
    if msg_name == "RUDP_JOIN_MATCH": return "JOIN_MATCH"
    if msg_name.startswith("RUDP_"): return msg_name
    if msg_name.startswith("UDP_"): return msg_name
    return "DATA"

async def build_packet(msg_key, layout, so, cmd, oid, flags, content, key, encrypted=True):
    k = key[0]; v80 = ((k<<8)|k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0])
    for code in layout:
        val = {0:so,1:cmd,2:oid,3:flags,4:len(body)}[code]
        if _FIELD_SIZES[code] == 1: hdr.append((val & 0xFF) ^ k)
        else:
            v = ((val & 0xFFFF) ^ v80) & 0xFFFF
            hdr.append(v & 0xFF); hdr.append((v >> 8) & 0xFF)
    pkt = bytearray(hdr + body)
    pkt[1] = await crc7_buff(0, bytes(pkt[2:])) & 0x7F
    return bytes(pkt)

async def layouts_from_mask(mask):
    ru = [int(c) for c in str(mask).strip()]
    nr = [c for c in ru if c != 2]
    return ru, nr

async def reply_for(frame, key, mask, ack_key=0x68, ping_key=0x6D, hello_key=0x5B, ack_style="short"):
    ru, nr = await layouts_from_mask(mask)
    typ = await classify(frame)
    if typ == "HELLO":
        if ack_style == "echo":
            content = frame["content"] if frame["content"] else b"\x10\x00\x00\x00"
            return typ, await build_packet(hello_key, nr, 1, 1, None, 1, content, key)
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x01\x00", key)
    if typ == "ACK":
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1,
                                        frame["content"] or b"\x01\x00", key)
    if typ == "PING":
        c = frame["content"]
        counter = c[:4] if len(c) >= 4 else c
        return typ, await build_packet(ping_key, nr, 0, 3, None, 0,
                                        counter + b"\x00\x00\x00", key, encrypted=False)
    if typ == "JOIN_MATCH":
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x02\x00", key)
    return typ, None

async def keepalive_ping(sock, ip, port, key_bytes, mask, stop_event):
    nr = (await layouts_from_mask(mask))[1]
    ping_keys = [0x66, 0x6D, 0x69, 0x6C, 0x6B, 0x6E, 0x6F, 0x70]
    i = 0
    while not stop_event.is_set():
        pk = ping_keys[i % len(ping_keys)]
        counter = int(time.time() * 1000) & 0xFFFFFFFF
        pkt = await build_packet(pk, nr, 0, 3, None, 0,
                                 struct.pack("<I", counter) + b"\x00\x00\x00",
                                 key_bytes, encrypted=False)
        _udp_sendto(sock, pkt, (ip, port))
        i += 1
        try: await asyncio.wait_for(stop_event.wait(), timeout=3.0)
        except asyncio.TimeoutError: pass

async def try_header(buf, layout, k, v80):
    off = 2; out = {}
    for code in layout:
        size = _FIELD_SIZES[code]
        if off + size > len(buf): return None
        out[_FIELD_NAMES[code]] = (buf[off] ^ k) if size == 1 else \
            ((buf[off] | (buf[off+1] << 8)) ^ v80) & 0xFFFF
        off += size
    out["headerLen"] = off
    return out

async def oicq_unpad(padded):
    if not padded or len(padded) < 8: return None
    if not all(padded[-1-i] == 0 for i in range(7)): return None
    pad_len = padded[0] & 0x07
    s = 3 + pad_len; e = len(padded) - 7
    return padded[s:e] if s < e else b""

async def decode_packet(packet, key, mask=None):
    data = bytes(packet) if isinstance(packet, bytes) else bytes.fromhex(packet)
    if len(data) < 8: return None
    k = key[0]; v80 = ((k<<8)|k) & 0xFFFF
    crc_ok = (data[1] & 0x7F) == await crc7_buff(0, data[2:])
    candidates = []
    if mask:
        ru, nr = await layouts_from_mask(mask)
        layouts = [("RUDP", ru), ("nonRUDP", nr)]
    else:
        layouts = [("RUDP", list(p)) for p in itertools.permutations([0,1,2,3,4])]
        layouts += [("nonRUDP", list(p)) for p in itertools.permutations([0,1,3,4])]
    for kind, layout in layouts:
        f = await try_header(data, layout, k, v80)
        if not f: continue
        if f["flags"] > 7 or f["sendOption"] > 7: continue
        if f["length"] != len(data) - f["headerLen"]: continue
        body = data[f["headerLen"]:f["headerLen"]+f["length"]]
        content = None; padded = None
        if f["flags"] & 1:
            if len(body) < 8 or len(body) % 8 != 0: continue
            padded = await tea_cbc_decrypt(body, key)
            content = await oicq_unpad(padded)
            if content is None: continue
        else: content = body
        score = (1 if crc_ok else 0) + (1 if content is not None else 0)
        candidates.append({"kind":kind, "layout":layout, "headerLen":f["headerLen"],
            "msgKey":data[0], "cmd":f["cmd"], "flags":f["flags"],
            "sendOption":f["sendOption"], "orderId":f.get("orderId"),
            "length":f["length"], "content":content, "crcOk":crc_ok, "score":score})
    if not candidates: return None
    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates[0]

# ==================== MATCH COUNTERS ====================
_match_counters: Dict[str, int] = {}
_match_counter_lock = asyncio.Lock()

async def _inc_match(uid):
    async with _match_counter_lock:
        _match_counters[uid] = _match_counters.get(uid, 0) + 1
        return _match_counters[uid]

async def _dec_match(uid):
    async with _match_counter_lock:
        if uid in _match_counters and _match_counters[uid] > 0:
            _match_counters[uid] -= 1
        return _match_counters.get(uid, 0)

# ==================== TOKEN CACHE ====================
_token_cache_memo: Dict[str, Any] = {}
_token_cache_memo_time: float = 0.0
_TOKEN_CACHE_MEMO_TTL = 5.0

def _json_serializer(obj):
    if isinstance(obj, (bytes, bytearray)):
        return {"__bytes_hex__": bytes(obj).hex()}
    raise TypeError(f"Type {type(obj)} not serializable")

def _json_deserializer(obj):
    if isinstance(obj, dict):
        if "__bytes_hex__" in obj and len(obj) == 1:
            try: return bytes.fromhex(obj["__bytes_hex__"])
            except Exception: return b""
        return {k: _json_deserializer(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_json_deserializer(x) for x in obj]
    return obj

def _load_token_cache():
    global _token_cache_memo, _token_cache_memo_time
    now = time.time()
    if _token_cache_memo and (now - _token_cache_memo_time) < _TOKEN_CACHE_MEMO_TTL:
        return _token_cache_memo
    if not os.path.exists(TOKEN_CACHE_FILE): return {}
    try:
        with open(TOKEN_CACHE_FILE,"r",encoding="utf-8") as f: content = f.read().strip()
        if not content: return {}
        data = json.loads(content)
        parsed = _json_deserializer(data)
        _token_cache_memo = parsed; _token_cache_memo_time = now
        return parsed
    except Exception:
        try: os.remove(TOKEN_CACHE_FILE)
        except Exception: pass
        return {}

def _save_token_cache(cache):
    global _token_cache_memo, _token_cache_memo_time
    try:
        with open(TOKEN_CACHE_FILE,"w",encoding="utf-8") as f:
            json.dump(cache, f, indent=2, default=_json_serializer)
        _token_cache_memo = cache; _token_cache_memo_time = time.time()
    except Exception as e: print_error(f"Cache save: {e}")

def cache_get(uid):
    cache = _load_token_cache()
    e = cache.get(str(uid))
    if not e: return None
    if time.time() - e.get("cached_at",0) > TOKEN_CACHE_TTL:
        cache_invalidate(uid); return None
    if str(e.get("account_id","")).isdigit(): e["account_id"] = int(e["account_id"])
    if not isinstance(e.get("login_payload_data"), (bytes, bytearray)):
        cache_invalidate(uid); return None
    return e

def cache_set(uid, data):
    cache = _load_token_cache()
    e = dict(data); e["cached_at"] = time.time()
    cache[str(uid)] = e; _save_token_cache(cache)

def cache_invalidate(uid):
    cache = _load_token_cache()
    if str(uid) in cache:
        del cache[str(uid)]; _save_token_cache(cache)

# ==================== PLAY GAME ====================
async def play_game(server_ip_port, thunder, sharma, udp_key, match_code,
                    account_id, player_region, client_version, key, iv, match_index):
    match_start_time = time.time()
    ping_task = None; sock = None
    ping_stop = asyncio.Event()
    movement_ctrl = None
    uid_str = str(account_id)
    completed_cleanly = False
    try:
        ip, port = server_ip_port.split(":")
        port = int(port)
        resolved_ip = await resolve_host_cloudflare(ip)
        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try: sock.bind(('0.0.0.0', 0))
        except Exception: pass
        optimize_udp_socket(sock); sock.setblocking(False)
        udp_key_bytes = bytes.fromhex(udp_key)
        hello_packet = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
        _udp_sendto(sock, bytes.fromhex(hello_packet), (resolved_ip, port))

        ack_state = "waiting_for_hello_reply"
        thunder_sent = False
        join_match_received = False
        local_closed = False
        send_lock = asyncio.Lock()
        ping_task = asyncio.create_task(keepalive_ping(sock, resolved_ip, port, udp_key_bytes, match_code, ping_stop))
        last_activity = time.time()

        movement_ctrl = MovementController(
            account_id=account_id, account_region=player_region,
            key=key, iv=iv, tcp_writer=None,
            udp_sock=sock, udp_server=(resolved_ip, port),
            udp_key_bytes=udp_key_bytes, match_code=match_code)

        async def send_thunder_sharma_inline():
            nonlocal ack_state, thunder_sent
            if thunder_sent: return
            async with send_lock:
                if thunder_sent: return
                try:
                    _udp_sendto(sock, bytes.fromhex(thunder), (resolved_ip, port))
                    thunder_sent = True
                    await asyncio.sleep(0.3)
                    prepare_ack = await build_packet(0x68, (await layouts_from_mask(match_code))[1],
                                                     0, 2, None, 1, b"\x01\x00", udp_key_bytes)
                    _udp_sendto(sock, prepare_ack, (resolved_ip, port))
                    await asyncio.sleep(0.4)
                    _udp_sendto(sock, bytes.fromhex(sharma), (resolved_ip, port))
                    ack_state = "thunder_sharma_sent"
                    print_success(f"[MATCH #{match_index}] Playing in-game!")
                    movement_ctrl.start()
                except Exception as e:
                    print_error(f"[MATCH #{match_index}] startup: {e}")

        while not local_closed:
            if time.time() - match_start_time > MAX_MATCH_DURATION:
                completed_cleanly = True; break
            try:
                response, server_addr = await asyncio.wait_for(_udp_recvfrom(loop, sock, 65535), timeout=2.0)
                if response:
                    last_activity = time.time()
                    frame = await decode_packet(response, udp_key_bytes, match_code)
                    if frame:
                        ptype = await classify(frame)
                        if frame['cmd'] == 2001:
                            if movement_ctrl is not None:
                                try: movement_ctrl.note_server_state(frame.get('content') or b"")
                                except Exception: pass
                            continue
                        if frame['cmd'] in [103, 107]:
                            print_success(f"[★] Match #{match_index} END")
                            completed_cleanly = True; local_closed = True
                            continue
                        if frame['cmd'] == 101:
                            try:
                                ack = await build_packet(0x68, (await layouts_from_mask(match_code))[1],
                                                         0,2,None,1, b"\x01\x00", udp_key_bytes)
                                _udp_sendto(sock, ack, server_addr)
                            except Exception: pass
                            continue
                        if ptype in ["ACK","PING","HELLO","JOIN_MATCH"]:
                            if ptype == "HELLO" and ack_state == "waiting_for_hello_reply":
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code, ack_style="short")
                                if reply: _udp_sendto(sock, reply, server_addr)
                                ack_state = "ack_sent_waiting"
                            elif ptype == "ACK":
                                if ack_state == "waiting_for_hello_reply":
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply: _udp_sendto(sock, reply, server_addr)
                                    ack_state = "ready_to_send_thunder"
                                elif ack_state == "ack_sent_waiting":
                                    ack_state = "ready_to_send_thunder"
                                else:
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply: _udp_sendto(sock, reply, server_addr)
                            elif ptype == "PING":
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply: _udp_sendto(sock, reply, server_addr)
                            elif ptype == "JOIN_MATCH" and not join_match_received:
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    _udp_sendto(sock, reply, server_addr)
                                    join_match_received = True
            except asyncio.TimeoutError:
                if ack_state == "ready_to_send_thunder" and not thunder_sent:
                    await send_thunder_sharma_inline()
                elif ack_state == "waiting_for_hello_reply":
                    if (time.time() - last_activity) > 7.0:
                        try:
                            pkt = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
                            _udp_sendto(sock, bytes.fromhex(pkt), (resolved_ip, port))
                        except Exception: pass
                        last_activity = time.time()
                    if (time.time() - match_start_time) > 25.0:
                        print_warning(f"[MATCH #{match_index}] Handshake timeout"); break
                elif ack_state == "thunder_sharma_sent":
                    if (time.time() - last_activity) > MATCH_IDLE_TIMEOUT:
                        completed_cleanly = True; break
            except (BlockingIOError, OSError):
                await asyncio.sleep(0.5); continue
            except Exception:
                await asyncio.sleep(0.5); continue
            if ack_state == "ready_to_send_thunder" and not thunder_sent:
                await send_thunder_sharma_inline()
        return f"match #{match_index} finished"
    except Exception as e:
        print_error(f"[MATCH #{match_index}] {e}")
        return f"match #{match_index} error"
    finally:
        if movement_ctrl:
            try: await movement_ctrl.stop()
            except Exception: pass
        if completed_cleanly:
            try:
                bot_state.increment_match(uid_str)
                print_success(f"[★] Match #{match_index} Complete | UID: {uid_str}")
                try: await _mark_bot_match_finished(uid_str)
                except Exception: pass
                async def _post_exp(uid):
                    try:
                        for wait in (5.0, 8.0, 12.0):
                            await asyncio.sleep(wait)
                            await refresh_account_profile(uid)
                    except Exception: pass
                asyncio.create_task(_post_exp(uid_str))
            except Exception: pass
        ping_stop.set()
        if ping_task:
            ping_task.cancel()
            try: await ping_task
            except asyncio.CancelledError: pass
        if sock:
            try: sock.close()
            except Exception: pass
        remaining = await _dec_match(uid_str)
        try: bot_state.update_status(uid_str, "ONLINE" if remaining == 0 else "IN_MATCH", remaining)
        except Exception: pass

# ==================== FUNCTIONAL LONE WOLF ====================
async def functional_lone_wolf(addrs, starter_packet, account_region, client_version,
                                key, iv, account_id="", account_data=None, max_reconnects=10):
    reconnects = 0
    ip, port = addrs.split(":")
    play_matches: List[asyncio.Task] = []
    no_response_count = 0
    search_attempts = 0
    last_start_time = 0.0
    uid_str = str(account_id)

    current_token = starter_packet
    current_key = key; current_iv = iv
    current_account_data = account_data
    _writer_ref = {"w": None}
    _team_active = {"flag": True}
    _match_played = {"flag": False}
    _squad_id_ref = {"id": None}

    async def _process_social_packet(raw_bytes, tag="MAIN"):
        try:
            hex_data = raw_bytes.hex()
            candidates = []
            if len(raw_bytes) >= 6:
                plen = raw_bytes[4]
                if 0 < plen <= (len(raw_bytes) - 5):
                    try:
                        inner = bytes(raw_bytes[5:5 + plen])
                        d0 = decode_protobuf_dict(inner)
                        if not d0:
                            a0 = decode_protobuf_2(inner)
                            if a0: d0 = {str(k): {"data": v} for k, v in a0.items()}
                        if d0: candidates.append((5, d0))
                    except Exception: pass
            for off in [5, 6, 10, 8, 12, 14, 4, 16, 0]:
                if off >= len(hex_data): continue
                try: inner_bytes = bytes.fromhex(hex_data[off:])
                except Exception: continue
                decoded = None
                try:
                    d1 = decode_protobuf_dict(inner_bytes)
                    if d1: decoded = d1
                except Exception: pass
                if not decoded:
                    try:
                        d2 = decode_protobuf_2(inner_bytes)
                        if d2: decoded = {str(k): {"data": v} for k, v in d2.items()}
                    except Exception: pass
                if decoded: candidates.append((off, decoded))
            if not candidates: return
            def _score(item):
                _off, d = item
                s = len(d)
                if "0" in d or 0 in d: s -= 100
                if "5" in d: s += 6
                if "1" in d: s += 4
                if "2" in d: s += 2
                return s
            candidates.sort(key=_score, reverse=True)
            best_off, decoded = candidates[0]
            def _val(container, k):
                if not isinstance(container, dict): return None
                v = container.get(k) if k in container else container.get(str(k))
                if isinstance(v, dict) and "data" in v: return v["data"]
                return v
            f5wrap = decoded.get("5") or decoded.get(5)
            f5 = None
            if isinstance(f5wrap, dict) and "data" in f5wrap: f5 = f5wrap["data"]
            elif isinstance(f5wrap, dict): f5 = f5wrap
            target_uid = None
            if isinstance(f5, dict):
                for fk in [1,2,3,4,6]:
                    cand = _val(f5, fk)
                    if cand and str(cand).isdigit() and 6 <= len(str(cand)) <= 15:
                        target_uid = str(cand); break
            if not target_uid: return
            own_acc = None
            if current_account_data: own_acc = str(current_account_data.get('account_id', ''))
            if own_acc and target_uid == own_acc: return
            if not _accept_friend_request or not current_account_data: return
            jwt_l = current_account_data.get('token')
            srv_l = current_account_data.get('server_url')
            rel_l = current_account_data.get('release_version')
            own_id = current_account_data.get('account_id')
            if not (jwt_l and srv_l and rel_l and own_id): return
            url = f"{srv_l.rstrip('/')}/ConfirmFriendRequest"
            fields = {"1": int(target_uid), "2": int(own_id)}
            try:
                payload = bytes.fromhex(encrypt_aes(create_proto(fields).hex()))
                hdrs = {
                    "authorization": f"Bearer {jwt_l}",
                    "host": srv_l.replace("https://", "").replace("http://", "").split("/")[0],
                    "x-ga": "v1 1",
                    "content-type": "application/x-www-form-urlencoded",
                    "accept-encoding": "deflate, gzip",
                    "releaseversion": f"{rel_l}",
                    "user-agent": "UnityPlayer/2022.3.47f1",
                    "accept": "*/*", "x-unity-version": "2022.3.47f1",
                }
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as s:
                    async with s.post(url, headers=hdrs, data=payload, ssl=ssl_ctx) as r:
                        body = await r.read()
                        if r.status == 200 and len(body) > 0:
                            print_success(f"[FRIEND-{tag}] ✅ Accepted {target_uid}")
            except Exception as e:
                print_warning(f"[FRIEND-{tag}] {type(e).__name__}: {e}")
        except Exception as e:
            print_warning(f"[SOCIAL-{tag}] exception: {type(e).__name__}: {e}")

    async def _team_coordinator():
        try:
            squad_id = _squad_id_ref["id"]
            if not squad_id: return
            start = time.time()
            while True:
                sq = await _get_squad_info(squad_id)
                if not sq: return
                if len(sq["all_uids"]) >= TEAM_SIZE: break
                if time.time() - start >= SQUAD_FILL_TIMEOUT:
                    print_warning(f"[SQUAD] {squad_id} fill timeout ({len(sq['all_uids'])}/{TEAM_SIZE}) → proceeding")
                    break
                if bot_state.is_paused(uid_str): return
                await asyncio.sleep(0.25)

            sq = await _get_squad_info(squad_id)
            if not sq or sq["leader"] != uid_str: return
            await asyncio.sleep(TEAM_READY_DELAY)

            sq = await _get_squad_info(squad_id)
            if not sq or sq["leader"] != uid_str: return

            # ═══ SOLO PREVENTION ═══
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
            print_success(f"[TEAM] 👑 {uid_str} | {squad_id} | members={len(sq['all_uids'])} invite={len(other_uids)}")

            try:
                pkt = create_team(current_key, current_iv, region=account_region,
                                  game_version=client_version)
                if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                    _writer_ref["w"].write(pkt); await _writer_ref["w"].drain()
                    print_info(f"[TEAM] create_team sent ({len(pkt)}B)")
            except Exception as e: print_error(f"[TEAM] create_team: {e}")
            await asyncio.sleep(0.8)

            own_id_num = (current_account_data or {}).get('account_id') or int(uid_str)
            try:
                pkt = team_fresh(int(own_id_num), current_key, current_iv)
                if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                    _writer_ref["w"].write(pkt); await _writer_ref["w"].drain()
            except Exception as e: print_warning(f"[TEAM] team_fresh: {e}")
            await asyncio.sleep(0.5)

            for target_uid in other_uids:
                async with _team_state["lock"]:
                    peer_info = _team_state["peers"].get(target_uid)
                peer_acc_id = (peer_info or {}).get("data", {}).get("account_id") or target_uid
                try:
                    inv = invite_player_in_team(int(peer_acc_id), current_key, current_iv,
                                                 region=account_region)
                    if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                        _writer_ref["w"].write(inv); await _writer_ref["w"].drain()
                        print_info(f"[TEAM] invite → {peer_acc_id}")
                except Exception as e: print_warning(f"[TEAM] invite: {e}")
                await asyncio.sleep(0.25)

            await asyncio.sleep(TEAM_INVITE_WAIT)
            print_success(f"[TEAM] 🚀 {uid_str} squad {squad_id} ready → match loop")
            try: bot_state.update_status(uid_str, "SEARCHING", 0)
            except Exception: pass

            while _team_active["flag"] and not _match_played["flag"]:
                try:
                    if bot_state.is_paused(uid_str): break
                    sq = await _get_squad_info(squad_id)
                    if not sq or sq["leader"] != uid_str: break

                    if sq.get("match_requested"):
                        if time.time() - sq.get("match_requested_at", 0) > 20.0:
                            async with _squad_lock:
                                if squad_id in _squads:
                                    _squads[squad_id]["match_requested"] = False
                        else:
                            await asyncio.sleep(1.0)
                            continue

                    own_id = (current_account_data or {}).get('account_id', own_id_num)
                    try:
                        rdy = ready_for_game(int(own_id), current_key, current_iv)
                        if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                            _writer_ref["w"].write(rdy); await _writer_ref["w"].drain()
                    except Exception: pass
                    await asyncio.sleep(0.5)
                    for target_uid in other_uids:
                        async with _team_state["lock"]:
                            pi = _team_state["peers"].get(target_uid)
                        ta = (pi or {}).get("data", {}).get("account_id") or target_uid
                        try:
                            inv = invite_player_in_team(int(ta), current_key, current_iv,
                                                         region=account_region)
                            if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                                _writer_ref["w"].write(inv); await _writer_ref["w"].drain()
                        except Exception: pass
                    async with _squad_lock:
                        if squad_id in _squads:
                            _squads[squad_id]["match_requested"] = True
                            _squads[squad_id]["match_requested_at"] = time.time()
                    try:
                        sm = start_match_team(int(own_id), current_key, current_iv, region=account_region)
                        if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                            _writer_ref["w"].write(sm); await _writer_ref["w"].drain()
                    except Exception as e: print_warning(f"[TEAM] start_match: {e}")
                except Exception as e: print_warning(f"[TEAM] loop: {e}")
                try: await asyncio.wait_for(asyncio.sleep(4.0), timeout=4.5)
                except asyncio.TimeoutError: pass
        except asyncio.CancelledError: raise
        except Exception as e:
            print_error(f"[TEAM] coordinator {_squad_id_ref.get('id')}: {e}")

    async def _member_match_trigger():
        try:
            squad_id = _squad_id_ref["id"]
            if not squad_id: return
            await asyncio.sleep(TEAM_READY_DELAY + 3.0)
            sq = await _get_squad_info(squad_id)
            if not sq or sq["leader"] == uid_str: return
            try: bot_state.update_status(uid_str, "SEARCHING", 0)
            except Exception: pass
            my_id = (current_account_data or {}).get('account_id', uid_str)
            print_info(f"[TEAM] {uid_str} member ready loop (squad={squad_id})")
            while _team_active["flag"] and not _match_played["flag"]:
                try:
                    if bot_state.is_paused(uid_str): break
                    if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                        try:
                            rdy = ready_for_game(int(my_id), current_key, current_iv)
                            _writer_ref["w"].write(rdy); await _writer_ref["w"].drain()
                        except Exception: pass
                except Exception: pass
                try: await asyncio.wait_for(asyncio.sleep(2.5), timeout=3.0)
                except asyncio.TimeoutError: pass
        except asyncio.CancelledError: raise
        except Exception as e:
            print_warning(f"[TEAM] member {_squad_id_ref.get('id')}: {e}")

    try:
        while True:
            while bot_state.is_paused(uid_str):
                try: bot_state.update_status(uid_str, "PAUSED", 0)
                except Exception: pass
                await asyncio.sleep(1.0)
            writer = None
            gateway_ping_task = None
            team_coord_task = None
            member_trigger_task = None
            try:
                if current_account_data:
                    fresh = None
                    if current_account_data.get('auth_type') == 'guest' and current_account_data.get('auth_uid'):
                        fresh = cache_get(str(current_account_data['auth_uid']))
                    elif current_account_data.get('auth_type') == 'token' and current_account_data.get('auth_token'):
                        fresh = cache_get(f"tok_{current_account_data['auth_token'][:20]}")
                    if fresh:
                        current_account_data = fresh
                        current_key = fresh['aes_ak']; current_iv = fresh['iv_i']
                        current_token = await build_tcp_startup_packet(
                            fresh['account_id'], fresh['token'], fresh['server_time'],
                            current_key, current_iv,
                            region=fresh.get('region', account_region), typ='OnLine')
                    else:
                        raise ConnectionError("Cache expired")
                resolved_ip = await resolve_host_cloudflare(ip)
                reader, writer = await asyncio.open_connection(resolved_ip, int(port))
                bot_state.register_writer(uid_str, writer)
                _writer_ref["w"] = writer
                raw_sock = writer.get_extra_info('socket')
                if raw_sock: optimize_tcp_socket(raw_sock)
                writer.write(bytes.fromhex(current_token)); await writer.drain()
                try:
                    init_ka = await send_keep_alive(account_region)
                    if init_ka and not writer.is_closing():
                        writer.write(init_ka)
                        await asyncio.wait_for(writer.drain(), timeout=3)
                except Exception: pass

                async def func_gateway_keepalive():
                    ka_bytes = await send_keep_alive(account_region)
                    while True:
                        await asyncio.sleep(5)
                        try:
                            if writer and not writer.is_closing():
                                writer.write(ka_bytes); await writer.drain()
                        except Exception: break

                gateway_ping_task = asyncio.create_task(func_gateway_keepalive())
                print_success(f"[✓] TCP Gateway Connected | UID: {uid_str} | region={account_region}")
                reconnects = 0
                no_response_count = 0

                if ENABLE_NEWBIE_CHARACTER and NEWBIE_SELECT_BEFORE_TEAM:
                    print_warning("[👤] Newbie select diaktifkan — berisiko Connection lost!")

                squad_id = await _get_or_assign_squad(uid_str)
                _squad_id_ref["id"] = squad_id

                async with _team_state["lock"]:
                    _team_state["peers"][uid_str] = {
                        "writer": writer, "data": current_account_data,
                        "key": current_key, "iv": current_iv}

                async def send_solo_start_match():
                    nonlocal search_attempts
                    search_attempts += 1
                    try:
                        try: bot_state.update_status(uid_str, "SEARCHING", 0)
                        except Exception: pass
                        await asyncio.sleep(random.uniform(0.2, 0.4))
                        try:
                            own_id = (current_account_data or {}).get('account_id') or uid_str
                            rdy = ready_for_game(int(own_id), current_key, current_iv)
                            if writer and not writer.is_closing():
                                writer.write(rdy); await writer.drain()
                        except Exception: pass
                        await asyncio.sleep(1.0)
                        await start_game_battle_royale(account_region, client_version, writer, current_key, current_iv)
                    except Exception as e: print_warning(f"[!] Solo: {e}")

                if not _match_played["flag"]:
                    if not TEAM_MODE and not SNIFF_MODE: await send_solo_start_match()
                    if TEAM_MODE and not SNIFF_MODE:
                        team_coord_task = asyncio.create_task(_team_coordinator())
                        member_trigger_task = asyncio.create_task(_member_match_trigger())
                else:
                    print_info(f"[IDLE] {uid_str} resuming in idle mode")

                while True:
                    play_matches[:] = [m for m in play_matches if not m.done()]
                    if bot_state.is_paused(uid_str): break
                    if _match_played["flag"]:
                        try:
                            data = await asyncio.wait_for(reader.read(8192), timeout=1.0)
                        except asyncio.TimeoutError: continue
                        if not data: raise ConnectionError("Connection closed")
                        if len(data) > 20 and data.hex().startswith("0600"):
                            await _process_social_packet(data, tag="IDLE")
                        continue
                    now = asyncio.get_running_loop().time()
                    if not TEAM_MODE and len(play_matches) == 0 and (now - last_start_time >= START_MATCH_INTERVAL):
                        await send_solo_start_match()
                        last_start_time = now
                    try:
                        data = await asyncio.wait_for(reader.read(8192), timeout=0.5)
                    except asyncio.TimeoutError:
                        no_response_count += 1; continue
                    if not data: raise ConnectionError("Connection closed")
                    hex_data = data.hex()
                    packet_length = len(data)
                    no_response_count = 0
                    if DEBUG_PACKETS:
                        try:
                            with open("packets_debug.log", "a", encoding="utf-8") as _df:
                                _df.write(f"{time.time():.3f} {uid_str} S2C len={packet_length} {hex_data[:8000]}\n")
                        except Exception: pass
                    if hex_data.startswith("0600") and packet_length > 20:
                        await _process_social_packet(data, tag="MAIN"); continue
                    if hex_data.startswith("0500") and packet_length > 10:
                        try:
                            inner = bytes.fromhex(hex_data[10:])
                            dec = decode_protobuf_dict(inner)
                            if not dec:
                                alt = decode_protobuf_2(inner)
                                if alt: dec = {str(k): {"data": v} for k, v in alt.items()}
                            f5w = dec.get("5") or dec.get(5)
                            f5 = f5w.get("data") if isinstance(f5w, dict) and "data" in f5w else f5w
                            if not isinstance(f5, dict): f5 = dec
                            def _f5g(key):
                                if not isinstance(f5, dict): return None
                                v = f5.get(key) if key in f5 else f5.get(str(key))
                                if isinstance(v, dict) and "data" in v: return v["data"]
                                return v
                            tc = None
                            for fk in [11, 5, 1, 3]:
                                cand = _f5g(fk)
                                if cand and str(cand).isdigit() and len(str(cand)) == 7:
                                    tc = str(cand); break
                            if tc:
                                await _set_squad_team_code(squad_id, tc)
                                print_success(f"[TEAM] 📢 {squad_id} code: {tc}")
                            group_id = _f5g(8)
                            if group_id and isinstance(group_id, str) and 20 <= len(group_id) <= 40:
                                pass
                            else: group_id = None
                            own_acc = None
                            if current_account_data: own_acc = str(current_account_data.get('account_id'))
                            sq_now = await _get_squad_info(squad_id)
                            _ldr_uid = sq_now["leader"] if sq_now else None
                            inviter = None
                            async with _team_state["lock"]:
                                _ldr_info = _team_state["peers"].get(_ldr_uid) if _ldr_uid else None
                            if _ldr_info and not SNIFF_MODE:
                                _ldr_acc = (_ldr_info.get("data") or {}).get("account_id")
                                inviter = str(_ldr_acc) if _ldr_acc else _ldr_uid
                            else:
                                for fk in [1,2,3,4,6,7,13,15,20]:
                                    cand = _f5g(fk)
                                    if cand and str(cand).isdigit() and 6 <= len(str(cand)) <= 15:
                                        if str(cand) not in (uid_str, own_acc, tc):
                                            inviter = str(cand); break
                            if inviter and group_id and (SNIFF_MODE or (_ldr_uid and _ldr_uid != uid_str)):
                                print_success(f"[TEAM] 📨 Invite from {inviter} → accept (squad={squad_id})")
                                for _ in range(2):
                                    try:
                                        acc = accept_team_invite(int(inviter), group_id,
                                                                 current_key, current_iv,
                                                                 region=account_region,
                                                                 game_version=client_version)
                                        if writer and not writer.is_closing():
                                            writer.write(acc); await writer.drain()
                                    except Exception as e: print_warning(f"[TEAM] accept: {e}")
                                    await asyncio.sleep(0.4)
                        except Exception as e: print_warning(f"[TEAM] 0500 parse: {e}")
                        continue
                    if hex_data.startswith("0300") and 10 < packet_length < 30:
                        continue
                    if hex_data.startswith("0300") and packet_length >= 300:
                        print_success(f"[⚔] Match Found | UID: {uid_str} | region={account_region} | squad={squad_id}")
                        try: await _mark_match_found(uid_str)
                        except Exception: pass
                        try: await _mark_squad_started(squad_id)
                        except Exception: pass
                        for tk in (team_coord_task, member_trigger_task):
                            if tk and not tk.done(): tk.cancel()
                        team_coord_task = None
                        member_trigger_task = None
                        _team_active["flag"] = False
                        try:
                            res = json.loads(await decode_protobuf_async(hex_data[10:]))
                            token = udp_key = match_code = server_ip_port = None
                            match_account_id = block_val = None
                            if '42' in res and 'data' in res['42']: match_code = res['42']['data']
                            if '5' in res and 'data' in res['5']:
                                rf5 = res['5']['data']
                                server_ip_port = rf5.get('2', {}).get('data')
                                udp_key = rf5.get('3', {}).get('data')
                                token = rf5.get('4', {}).get('data')
                                if '42' in rf5: match_code = rf5['42']['data']
                            if '1' in res and 'data' in res['1']: match_account_id = res['1']['data']
                            if '5' in res and 'data' in res['5']:
                                block_val = res['5']['data'].get('1', {}).get('data')
                            effective_acc_id = match_account_id or account_id or "BD_BOT"
                            if token and udp_key and match_code and server_ip_port:
                                print_info(f"[⚔] Match server = {server_ip_port}")
                                acc_tok = (current_account_data or {}).get('access_token', '')
                                _mkey = GAME_MODE if GAME_MODE in MODE_IDS else "BR"
                                _mode_id, _map_id = MODE_IDS[_mkey], MAP_IDS[_mkey]
                                if _mkey == "CS":
                                    # server reports the real mode of the found match in field 5.7 (15 = CS).
                                    # If the lobby actually found a non-CS match, fall back to the BR values.
                                    try: _srv_mode = int(res['5']['data'].get('7', {}).get('data'))
                                    except Exception: _srv_mode = None
                                    if _srv_mode is not None and _srv_mode != CS_MODE_ID:
                                        print_warning(f"[⚔] GAME_MODE=CS but server match mode={_srv_mode} → using BR packets")
                                        _mode_id, _map_id = MODE_IDS["BR"], MAP_IDS["BR"]
                                print_info(f"[⚔] GAME_MODE={_mkey} mode_id={_mode_id} map_id={_map_id}")
                                thunder, sharma = await build_match_startup_packets(
                                    token, udp_key, match_code, effective_acc_id, block_val or 0,
                                    server_ip=server_ip_port, region=account_region,
                                    client_version=client_version, access_token=acc_tok,
                                    mode_id=_mode_id, map_id=_map_id)
                                match_index = await _inc_match(uid_str)
                                print_info(f"[⚔] Match #{match_index} → {server_ip_port}")
                                try:
                                    bot_state.increment_match_started()
                                    bot_state.update_status(uid_str, "IN_MATCH (TEAM)", 1)
                                except Exception: pass
                                new_match = asyncio.create_task(
                                    play_game(server_ip_port, thunder, sharma, udp_key,
                                              match_code, effective_acc_id, "ID",
                                              client_version, current_key, current_iv,
                                              match_index=match_index))
                                play_matches.append(new_match)

                                async def drain_gw():
                                    while not new_match.done():
                                        try:
                                            dg = await asyncio.wait_for(reader.read(4096), timeout=1.0)
                                            if not dg: break
                                            gh = dg.hex()
                                            if gh.startswith("0600") and len(dg) > 20:
                                                await _process_social_packet(dg, tag="DRAIN")
                                        except asyncio.TimeoutError: continue
                                        except Exception: break
                                drain_task = asyncio.create_task(drain_gw())
                                try: await new_match
                                except Exception as e: print_error(f"[MATCH #{match_index}] {e}")
                                finally:
                                    drain_task.cancel()
                                    try: await drain_task
                                    except asyncio.CancelledError: pass
                                play_matches[:] = [m for m in play_matches if not m.done()]
                                _match_played["flag"] = True
                                print_success(f"[IDLE] {uid_str} parked after match")
                                continue
                        except Exception as e: print_warning(f"[MATCH] {e}"); continue
                    if 30 <= packet_length <= 40:
                        continue
            except asyncio.CancelledError: raise
            except Exception as e:
                if "Cache expired" in str(e):
                    print_warning(f"[!] Token expired {uid_str}"); break
                reconnects += 1
                if reconnects > max_reconnects:
                    if current_account_data:
                        try:
                            if current_account_data.get('auth_uid'):
                                cache_invalidate(str(current_account_data['auth_uid']))
                            if current_account_data.get('auth_token'):
                                cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                        except Exception: pass
                    reconnects = 0; break
                print_warning(f"[{uid_str}] Reconnect #{reconnects}: {e}")
                await asyncio.sleep(min(reconnects * 0.5, 2.0))
            finally:
                _team_active["flag"] = False
                async with _team_state["lock"]:
                    _team_state["peers"].pop(uid_str, None)
                for tk in (team_coord_task, member_trigger_task):
                    if tk and not tk.done():
                        tk.cancel()
                        try: await tk
                        except asyncio.CancelledError: pass
                if gateway_ping_task: gateway_ping_task.cancel()
                if writer:
                    bot_state.unregister_writer(uid_str, writer)
                    await safe_close_writer(writer)
                if not _match_played["flag"]:
                    _team_active["flag"] = True
    except asyncio.CancelledError:
        print_info(f"[FUNCTIONAL] Task cancelled for {uid_str}")
        raise
    finally:
        for m in play_matches:
            if not m.done(): m.cancel()

# ==================== INFORMATIONAL ====================
async def informational(addrs, starter_packet, key, iv, region="ID",
                        account_id="", max_reconnects=3, account_data=None):
    uid_str = str(account_id); reconnects = 0
    ip, port = addrs.split(":")
    while True:
        while uid_str and bot_state.is_paused(uid_str):
            await asyncio.sleep(1.0)
        writer = None; ping_task = None
        try:
            resolved_ip = await resolve_host_cloudflare(ip)
            reader, writer = await asyncio.open_connection(resolved_ip, int(port))
            if uid_str: bot_state.register_writer(uid_str, writer)
            raw_sock = writer.get_extra_info('socket')
            if raw_sock: optimize_tcp_socket(raw_sock)
            writer.write(bytes.fromhex(starter_packet)); await writer.drain()
            reconnects = 0
            try:
                init_ka = await send_keep_alive(region)
                if init_ka and not writer.is_closing():
                    writer.write(init_ka)
                    await asyncio.wait_for(writer.drain(), timeout=3)
            except Exception: pass
            async def info_ka():
                ka = await send_keep_alive(region)
                while True:
                    await asyncio.sleep(5)
                    try:
                        if writer and not writer.is_closing():
                            writer.write(ka); await writer.drain()
                    except Exception: break
            ping_task = asyncio.create_task(info_ka())
            print_success(f"[INFO] Connected | UID: {uid_str} | region={region}")
            while True:
                if uid_str and bot_state.is_paused(uid_str):
                    if ping_task: ping_task.cancel()
                    if uid_str: bot_state.unregister_writer(uid_str, writer)
                    await safe_close_writer(writer); writer = None
                    while bot_state.is_paused(uid_str): await asyncio.sleep(1.0)
                    break
                try: data = await asyncio.wait_for(reader.read(8192), timeout=1.0)
                except asyncio.TimeoutError: continue
                if not data: raise ConnectionError("closed")
        except asyncio.CancelledError:
            if ping_task: ping_task.cancel()
            if uid_str: bot_state.unregister_writer(uid_str, writer)
            await safe_close_writer(writer); raise
        except Exception:
            if ping_task: ping_task.cancel()
            if uid_str: bot_state.unregister_writer(uid_str, writer)
            await safe_close_writer(writer)
            reconnects += 1
            if reconnects > max_reconnects:
                await asyncio.sleep(3); reconnects = 0
            else: await asyncio.sleep(1)

# ==================== ACCOUNT HELPERS ====================
def _register_credentials(ad):
    try:
        bot_state.account_credentials[str(ad['account_id'])] = ad
        if ad.get('auth_uid'): bot_state.account_credentials[str(ad['auth_uid'])] = ad
    except Exception: pass

async def refresh_account_profile(ad_or_uid):
    try:
        if isinstance(ad_or_uid, str):
            uid = str(ad_or_uid); ad = bot_state.account_credentials.get(uid)
        else:
            ad = ad_or_uid; uid = str(ad.get('account_id'))
        if not ad: return
        token = ad.get('token'); acc_id = ad.get('account_id')
        srv = ad.get('server_url'); rel = ad.get('release_version')
        if not (token and acc_id and srv and rel): return
        data = await get_player_personal_show(int(acc_id), token, srv, rel)
        if not data: return
        pd = data.get(1, {}) if isinstance(data.get(1), dict) else {}
        nickname = pd.get(3); lvl = pd.get(6); exp = pd.get(7)
        if exp is None: return
        try:
            exp = int(exp); lvl = int(lvl) if lvl else 1
        except Exception: return
        aid = str(acc_id)
        if aid not in bot_state.accounts:
            bot_state.register_account(aid, nickname or f"Player_{aid}",
                region=ad.get('region','BD'), level=lvl, exp=exp,
                auth_uid=ad.get('auth_uid'))
            return
        old = bot_state.accounts[aid].get("current_exp", 0)
        bot_state.update_exp(aid, exp, lvl)
        if nickname: bot_state.accounts[aid]["nickname"] = nickname
        if exp > old: print_success(f"[★] +{exp-old:,} EXP | {aid}")
    except Exception: pass

async def process_account_uid_pass(uid, password, region_override=None):
    cached = cache_get(uid)
    if cached:
        aid = str(cached['account_id'])
        nick = cached.get('nickname', f"Player_{aid}")
        lvl = cached.get('level', 1); expv = cached.get('exp', 0)
        print_success(f"[✓] Online (cache): {aid} | {nick} | region={cached.get('region','BD')}")
        bot_state.register_account(aid, nick, cached.get('region','BD'),
            lvl, expv, cached.get('likes',0), auth_uid=str(uid))
        _register_credentials(cached); return cached
    print_info(f"[LOGIN] UID: {uid}")
    try:
        async with _LOGIN_SEMAPHORE:
            vc = await version_config(region_override or "ID")
            if not vc: return None
            release_version, client_version, server_url, gop_1, app_version = vc
            tg = await get_access_token(uid, password)
            if not tg: return None
            open_id, access_token, platform = tg
            major_payload, login_payload = await get_payload(
                access_token, open_id, platform, client_version)
            res_json = await major_login(major_payload, server_url, release_version)
            if not res_json: return None
            def _gd(k):
                v = res_json.get(str(k))
                if isinstance(v, dict) and "data" in v: return v["data"]
                if k in res_json and not isinstance(res_json[k], dict): return res_json[k]
                if isinstance(v, dict): return v.get("data")
                return None
            acc_id_raw = _gd(1); region_raw = _gd(2) or "ID"
            jwt_token = _gd(8); server_url_raw = _gd(10); server_time = _gd(21)
            aes_ak, iv_i = _extract_key_iv(res_json)
            if not jwt_token or not server_url_raw or not aes_ak: return None
            getlogin_json = await get_login_data(
                login_payload, jwt_token, server_url_raw, release_version)
            if not getlogin_json: return None
        def _gf(f, d=None):
            v = getlogin_json.get(str(f), {}).get("data")
            return v if v is not None else d
        level = int(_gf(6,1) or 1); exp = int(_gf(7,0) or 0)
        likes = int(_gf(8,0) or 0)
        nickname = _gf(4, f"Player_{acc_id_raw}") or f"Player_{acc_id_raw}"
        region = region_raw or _gf(3,"ID") or "ID"
        acc_id = str(acc_id_raw)
        bot_state.register_account(acc_id, nickname, region, level, exp, likes, auth_uid=str(uid))
        print_success(f"[✓] Login: {acc_id} | {nickname} | Lvl {level} | region={region}")
        ad = {'account_id':acc_id_raw,'nickname':nickname,'region':region,
            'level':level,'exp':exp,'likes':likes,'open_id':open_id,
            'access_token':access_token,'platform':str(platform),
            'token':jwt_token,'server_time':server_time,
            'aes_ak':aes_ak,'iv_i':iv_i,
            'functional_addrs':_gf(14),'informational_addrs':_gf(32),
            'release_version':release_version,'client_version':client_version,
            'server_url':server_url_raw,'login_payload_data':login_payload,
            'auth_type':'guest','auth_uid':uid,'auth_password':password}
        _register_credentials(ad); cache_set(uid, ad); return ad
    except Exception as e:
        print_error(f"login err: {e}"); return None

async def run_account_worker(ad, label, region_override=None, clan_override=None):
    acc_id = str(ad['account_id'])
    info_task = None; exp_task = None
    try:
        reg = region_override or ad.get('region','BD')
        tcp_online = await build_tcp_startup_packet(
            ad['account_id'], ad['token'], ad['server_time'],
            ad['aes_ak'], ad['iv_i'], region=reg, typ='OnLine')
        tcp_chat = await build_tcp_startup_packet(
            ad['account_id'], ad['token'], ad['server_time'],
            ad['aes_ak'], ad['iv_i'], region=reg, typ='ChaT')
        if clan_override:
            try:
                await asyncio.sleep(1.0)
                await join_clan(ad['token'], clan_override)
            except Exception as e: print_warning(f"[CLAN] {e}")
        info_task = asyncio.create_task(informational(
            ad['informational_addrs'], tcp_chat, ad['aes_ak'], ad['iv_i'],
            region=reg, account_id=acc_id, account_data=ad))
        async def exp_refresher():
            while True:
                await asyncio.sleep(90 + random.uniform(-10,10))
                fresh = bot_state.account_credentials.get(acc_id)
                if fresh: await refresh_account_profile(fresh)
        exp_task = asyncio.create_task(exp_refresher())
        await functional_lone_wolf(
            ad['functional_addrs'], tcp_online, reg, ad['client_version'],
            ad['aes_ak'], ad['iv_i'], account_id=acc_id, account_data=ad)
    except asyncio.CancelledError: raise
    except Exception as e:
        print_error(f"worker err {label}: {e}")
    finally:
        for t in (info_task, exp_task):
            if t and not t.done(): t.cancel()
        for t in (info_task, exp_task):
            if t:
                try: await t
                except Exception: pass

async def account_loop_guest(uid, password, region_override=None, clan_override=None):
    uid_str = str(uid)
    bot_state.account_workers[uid_str] = asyncio.current_task()
    acc_id = None
    while True:
        try:
            bot_state.update_status(uid_str, "CONNECTING")
            ad = await process_account_uid_pass(uid_str, password, region_override)
            if not ad:
                print_error(f"Login failed {uid_str}. Retry 15s"); await asyncio.sleep(15); continue
            acc_id = str(ad['account_id'])
            bot_state.account_workers[acc_id] = asyncio.current_task()
            bot_state.auth_to_game_id[uid_str] = acc_id
            bot_state.game_to_auth_id[acc_id] = uid_str
            await run_account_worker(ad, uid_str, region_override, clan_override)
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            try:
                bot_state.update_status(uid_str, "OFFLINE")
                if acc_id: bot_state.update_status(acc_id, "OFFLINE")
            except Exception: pass
            break
        except Exception as e:
            print_error(f"err {uid_str}: {e}"); await asyncio.sleep(10)

# ==================== CREATE ACCOUNTS ====================
def _gf_field(ld, key, default=None):
    if not ld: return default
    v = ld.get(str(key), {})
    if isinstance(v, dict): return v.get("data", default)
    return v if v is not None else default

async def _send_friend_request(own_ad, target_uid):
    try:
        srv = own_ad.get('server_url')
        jwt = own_ad.get('token')
        rel = own_ad.get('release_version')
        own_id = own_ad.get('account_id')
        if not (srv and jwt and rel and own_id): return False
        url = f"{srv.rstrip('/')}/SendFriendRequest"
        fields = {"1": int(target_uid), "2": int(own_id)}
        payload = bytes.fromhex(encrypt_aes(create_proto(fields).hex()))
        hdrs = {
            "authorization": f"Bearer {jwt}",
            "host": srv.replace("https://", "").replace("http://", "").split("/")[0],
            "x-ga": "v1 1",
            "content-type": "application/x-www-form-urlencoded",
            "accept-encoding": "deflate, gzip",
            "releaseversion": f"{rel}",
            "user-agent": "UnityPlayer/2022.3.47f1",
            "accept": "*/*",
            "x-unity-version": "2022.3.47f1",
        }
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.post(url, headers=hdrs, data=payload, ssl=ssl_ctx) as r:
                body = await r.read()
                return (r.status == 200 and len(body) > 0)
    except Exception as e:
        print_warning(f"[FRIEND] send {target_uid}: {type(e).__name__}")
        return False

async def _batch_friend_requests(new_accounts):
    pool: Dict[str, dict] = {}
    for ad in bot_state.account_credentials.values():
        aid = str(ad.get('account_id'))
        if aid and aid not in pool: pool[aid] = ad
    for ad in new_accounts:
        aid = str(ad.get('account_id'))
        if aid: pool[aid] = ad
    if len(pool) < 2:
        print_info("[FRIEND] <2 accounts, skip batch")
        return
    targets = list(pool.values())
    ok = 0; total = 0
    for own in new_accounts:
        own_aid = str(own.get('account_id'))
        for other in targets:
            other_aid = str(other.get('account_id'))
            if own_aid == other_aid: continue
            total += 1
            if await _send_friend_request(own, other_aid):
                ok += 1
                print_info(f"[FRIEND] {own_aid} → {other_aid} ✅")
            await asyncio.sleep(0.12)
    print_success(f"[FRIEND] Batch done: {ok}/{total} sent")

async def choose_region(region, jwt_token, login_url, release_version):
    region_code = region.upper()
    url = f"{login_url.rstrip('/')}/ChooseRegion"
    proto = create_proto({1: region_code})
    payload = bytes.fromhex(encrypt_aes(proto.hex()))
    hdrs = {"Accept-Encoding": "gzip", "Authorization": f"Bearer {jwt_token}",
        "Connection": "Keep-Alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "ReleaseVersion": f"{release_version}",
        "User-Agent": "UnityPlayer/2018.4.12f1",
        "X-GA": "v1 1", "X-GA-SV": str(int(time.time())),
        "X-Unity-Version": "2018.4.12f1"}
    try:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.post(url, data=payload, headers=hdrs, ssl=ssl_ctx) as r:
                print_info(f"[CHOOSE-REGION] {region} → HTTP {r.status}")
                return r.status
    except Exception as e:
        print_error(f"[CHOOSE-REGION] {e}"); return None

async def major_register(release_v, access_token, open_id, name, game_version,
                          login_url, lang_code="en"):
    try:
        field14 = bytes(ord(c) ^ KEYSTREAM[i % 32] for i, c in enumerate(open_id))
        fields = {1: name, 2: access_token, 3: open_id, 5: 102000007,
                  6: 4, 7: 1, 13: 1, 14: field14, 15: lang_code,
                  16: 2, 20: "2.132.4", 21: 1, 22: GGRE_RAW}
        proto = _encode_proto_std(fields)
        payload = AES.new(AES_KEY, AES.MODE_CBC, AES_IV).encrypt(pad(proto, 16))
        url = f"{login_url.rstrip('/')}/MajorRegister"
        hdrs = {"Accept": "*/*", "Accept-Encoding": "deflate, gzip",
            "Authorization": "Bearer",
            "Content-Type": "application/x-www-form-urlencoded",
            "ReleaseVersion": f"{release_v}",
            "User-Agent": "UnityPlayer/2018.4.12f1",
            "X-GA": "v1 1", "X-GA-SV": str(int(time.time())),
            "X-Unity-Version": "2018.4.12f1"}
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
            async with s.post(url, headers=hdrs, data=payload, ssl=ssl_ctx) as resp:
                content = await resp.read()
                if resp.status != 200 or not content: return None
                for off in [0,64,48,32,80,16,8,96,128]:
                    if off >= len(content): continue
                    try:
                        r = decode_protobuf_2(content[off:])
                        if r and 3 in r:
                            return {str(k): {"data": v} for k, v in r.items()}
                    except Exception: pass
                if len(content) % 16 == 0:
                    try:
                        pt = AES.new(AES_KEY, AES.MODE_CBC, AES_IV).decrypt(content)
                        pad_len = pt[-1]
                        if 1 <= pad_len <= 16 and all(b == pad_len for b in pt[-pad_len:]):
                            pt = pt[:-pad_len]
                        r = decode_protobuf_2(pt)
                        if r and 3 in r:
                            return {str(k): {"data": v} for k, v in r.items()}
                    except Exception: pass
                return None
    except Exception as e:
        print_error(f"[MAJOR-REG] {e}"); return None

async def create_four_accounts(region: str, clan_id: str, count: int = 4, password: str = "ANON"):
    bot_state.creating = True
    bot_state.create_progress = []
    def _prog(s):
        try:
            print(f"[CREATE] {s}"); bot_state.log(s, "info")
        except Exception: pass
        bot_state.create_progress.append(s)
    try:
        _prog(f"⌛ Fetching version config for {region}...")
        vc = await version_config(region)
        if not vc:
            _prog("❌ version config failed"); return []
        release_v, remote_v, login_url, gop_1, app_version = vc
        if not login_url.endswith("/"): login_url += "/"
        lang = REGION_LANG.get(region.upper(), "en")
        game_version = remote_v or app_version
        _prog(f"✅ Version OK | release={release_v} | game={game_version}")
        created = []
        for i in range(count):
            _prog(f"⌛ Creating bot {i+1}/{count}...")
            try:
                uid = await register_account(password, gop_1, app_version)
                if not uid:
                    _prog(f"❌ bot {i+1}: register failed"); continue
                tok = await grant_token(uid, password, gop_1, app_version)
                if not tok:
                    _prog(f"❌ bot {i+1}: grant failed"); continue
                open_id = tok["open_id"]
                access_token = tok["access_token"]
                platform = tok.get("platform", 4)
                major_payload, login_payload = await get_payload(
                    access_token, open_id, platform, game_version, lang)
                nickname_final = "ANON•" + "".join(
                    random.choices(string.ascii_uppercase + string.digits, k=5))
                mr = await major_register(release_v, access_token, open_id,
                                          nickname_final, game_version, login_url, lang)
                if not mr:
                    _prog(f"❌ bot {i+1}: MajorRegister failed"); continue
                await asyncio.sleep(3.0)
                res_json = None
                for attempt in range(3):
                    mp, lp = await get_payload(
                        access_token, open_id, platform, game_version, lang)
                    res_json = await major_login(mp, login_url, release_v)
                    if res_json and (("8" in res_json) or (8 in res_json)):
                        login_payload = lp; break
                    res_json = None
                    await asyncio.sleep(2.0)
                if not res_json:
                    _prog(f"❌ bot {i+1}: MajorLogin #1 failed"); continue
                def _gd(k):
                    v = res_json.get(str(k))
                    if isinstance(v, dict) and "data" in v: return v["data"]
                    if k in res_json and not isinstance(res_json[k], dict): return res_json[k]
                    if isinstance(v, dict): return v.get("data")
                    return None
                jwt_token = _gd(8); server_url_raw = _gd(10)
                server_time = _gd(21); acc_id_raw = _gd(1)
                if not jwt_token:
                    _prog(f"❌ bot {i+1}: no JWT"); continue
                if not server_url_raw:
                    _prog(f"   → ChooseRegion({region})...")
                    cr_status = await choose_region(region, jwt_token, login_url, release_v)
                    if cr_status != 200:
                        _prog(f"❌ bot {i+1}: ChooseRegion failed"); continue
                    await asyncio.sleep(2.0)
                    res_json2 = None
                    for attempt in range(3):
                        mp2, lp2 = await get_payload(
                            access_token, open_id, platform, game_version, lang)
                        res_json2 = await major_login(mp2, login_url, release_v)
                        if res_json2 and (("10" in res_json2) or (10 in res_json2)):
                            res_json = res_json2; login_payload = lp2; break
                        res_json2 = None
                        await asyncio.sleep(2.0)
                    if not res_json2:
                        _prog(f"❌ bot {i+1}: MajorLogin #2 failed"); continue
                    res_json = res_json2
                    jwt_token = _gd(8); server_url_raw = _gd(10)
                    server_time = _gd(21); acc_id_raw = _gd(1)
                aes_ak, iv_i = _extract_key_iv(res_json)
                if not aes_ak or not iv_i:
                    _prog(f"❌ bot {i+1}: AES key/IV fail"); continue
                ld = await get_login_data(login_payload, jwt_token, server_url_raw, release_v)
                level, exp, likes = 1, 0, 0
                if ld:
                    def _gf(f, d=None):
                        v = ld.get(str(f), {}).get("data")
                        return v if v is not None else d
                    level = int(_gf(6, 1) or 1)
                    exp = int(_gf(7, 0) or 0)
                    likes = int(_gf(8, 0) or 0)
                    nickname_final = _gf(4, nickname_final) or nickname_final
                ad = {
                    'account_id': acc_id_raw, 'nickname': nickname_final,
                    'region': region, 'level': level, 'exp': exp, 'likes': likes,
                    'open_id': open_id, 'access_token': access_token,
                    'platform': str(platform), 'token': jwt_token,
                    'server_time': server_time, 'aes_ak': aes_ak, 'iv_i': iv_i,
                    'functional_addrs': _gf_field(ld, 14),
                    'informational_addrs': _gf_field(ld, 32),
                    'release_version': release_v, 'client_version': game_version,
                    'server_url': server_url_raw, 'login_payload_data': login_payload,
                    'auth_type': 'guest', 'auth_uid': uid, 'auth_password': password,
                }
                _register_credentials(ad); cache_set(uid, ad)
                created.append(ad)
                _prog(f"✅ bot {i+1}: {nickname_final} | UID {acc_id_raw} | region={region}")
                await asyncio.sleep(0.3)
            except Exception as e:
                _prog(f"❌ bot {i+1}: {e}")
                traceback.print_exc()
        if created:
            try:
                existing = []
                if os.path.exists(ACCOUNTS_FILE):
                    try:
                        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                            existing = json.load(f) or []
                            if not isinstance(existing, list): existing = []
                    except Exception: existing = []
                existing_uids = {str(a.get("uid")) for a in existing if isinstance(a, dict)}
                for a in created:
                    u = str(a['auth_uid'])
                    if u not in existing_uids:
                        existing.append({
                            "uid": u, "password": a['auth_password'],
                            "region": region, "clan_id": clan_id})
                with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                    json.dump(existing, f, indent=2)
                _prog(f"📦 {len(created)} accounts saved")
            except Exception as e:
                _prog(f"⚠️ save accounts: {e}")
        if created:
            _prog(f"🤝 Sending friend requests between {len(created)} new + existing...")
            try:
                await _batch_friend_requests(created)
                _prog(f"✅ Friend batch complete")
            except Exception as e:
                _prog(f"⚠️ friend batch: {e}")
            _prog(f"🚀 Starting {len(created)} bot workers...")
            for i, a in enumerate(created):
                try:
                    task = asyncio.create_task(account_loop_guest(
                        str(a['auth_uid']), a['auth_password'],
                        region_override=region, clan_override=clan_id))
                    bot_state.account_workers[str(a['auth_uid'])] = task
                    _prog(f"   → worker {i+1}/{len(created)} started")
                    await asyncio.sleep(0.25)
                except Exception as e:
                    _prog(f"   ❌ worker {i+1}: {e}")
            _prog(f"✅ All {len(created)} bots launched.")
        else:
            _prog("❌ No accounts created.")
        return created
    except Exception as e:
        _prog(f"❌ create_four_accounts error: {e}")
        traceback.print_exc(); return []
    finally:
        bot_state.creating = False

# ==================== AUTO-LOAD ====================
async def _autoload_accounts():
    if not os.path.exists(ACCOUNTS_FILE):
        print_info("[BOOT] No accounts.json"); return
    try:
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            accounts = json.load(f) or []
    except Exception as e:
        print_error(f"[BOOT] Load fail: {e}"); return
    if not isinstance(accounts, list) or not accounts:
        print_info("[BOOT] accounts.json empty"); return
    print_success(f"[BOOT] Auto-loading {len(accounts)} account(s)")
    for i, a in enumerate(accounts):
        try:
            if not isinstance(a, dict): continue
            uid = str(a.get("uid", "")).strip()
            password = str(a.get("password", "")).strip()
            region = str(a.get("region", "")).strip() or None
            clan_id = str(a.get("clan_id", "")).strip() or CLAN_ID_DEFAULT
            if not uid or not password: continue
            task = asyncio.create_task(account_loop_guest(
                uid, password, region_override=region, clan_override=clan_id))
            bot_state.account_workers[uid] = task
            print_info(f"[BOOT] worker {uid} ({i+1}/{len(accounts)}) region={region}")
            await asyncio.sleep(0.25)
        except Exception as e:
            print_error(f"[BOOT] worker {i+1}: {e}")

# ==================== HTTP SERVER ====================
@aiohttp.web.middleware
async def _req_logger(request, handler):
    print(f"\033[90m[REQ] {request.method} {request.path}\033[0m")
    try:
        resp = await handler(request)
        print(f"\033[90m[RES] {request.method} {request.path} → {resp.status}\033[0m")
        return resp
    except aiohttp.web.HTTPException as e:
        if request.path.startswith("/api/"):
            return aiohttp.web.json_response(
                {"ok": False, "status": "error", "error": e.reason or str(e)},
                status=e.status)
        raise
    except Exception as e:
        traceback.print_exc()
        if request.path.startswith("/api/"):
            return aiohttp.web.json_response(
                {"ok": False, "status": "error", "error": str(e)}, status=500)
        raise

async def serve_index(request):
    raise aiohttp.web.HTTPFound("/login")

async def serve_ui_file(request):
    name = request.match_info.get("name", "") or os.path.basename(request.path)
    name = {"login": "login.html", "signup": "signup.html", "admin": "admin.html",
            "client": "client.html", "ffglory": "ffglory.html"}.get(name, name)
    allowed = {"admin.html", "client.html", "ffglory.html",
               "login.html", "signup.html", "login.js", "shared.css", "shared.js", "sw.js"}
    if name not in allowed:
        raise aiohttp.web.HTTPNotFound()
    path = os.path.join(ELITE_ARENA_DIR, name)
    if not os.path.isfile(path):
        raise aiohttp.web.HTTPNotFound()
    content_type = "text/html" if name.endswith(".html") else (
        "text/javascript" if name.endswith(".js") else (
        "text/css" if name.endswith(".css") else "application/javascript"))
    with open(path, "r", encoding="utf-8") as f:
        return aiohttp.web.Response(text=f.read(), content_type=content_type)

async def api_logs(request):
    return aiohttp.web.json_response({
        "logs": [f"[{l.get('time','')}] {l.get('message','')}" for l in bot_state.logs[-200:]]
    })

async def api_clanjoin(request):
    try:
        body = await request.json()
        clan_id = str(body.get("clan_id", CLAN_ID_DEFAULT)).strip()
        if not clan_id:
            return aiohttp.web.json_response({"status": "error", "error": "clan_id required"})
        jobs = []
        for acc in list(bot_state.accounts.values()):
            token = acc.get("token")
            if token:
                jobs.append(asyncio.create_task(join_clan(token, clan_id)))
        if not jobs:
            return aiohttp.web.json_response({"status": "error", "error": "no authenticated accounts"})
        results = await asyncio.gather(*jobs, return_exceptions=True)
        return aiohttp.web.json_response({"status": "ok", "attempted": len(results)})
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "error": str(e)})

async def api_guildinfo(request):
    return aiohttp.web.json_response({"error": "guild info provider is not configured"})

async def api_ping(request):
    return aiohttp.web.json_response({"pong": True, "app": "ELITE ARENA", "version": "2.2"})

async def api_stats(request):
    try:
        accs = list(bot_state.accounts.values())
        online = sum(1 for a in accs if a.get("status") in ("ONLINE","IN_MATCH","SEARCHING"))
        active_matches = sum(a.get("active_matches", 0) for a in accs)
        uptime = max(1, int(time.time() - bot_state.start_time))
        uptime_h = max(0.016, uptime / 3600.0)
        exp_rate = int(bot_state.total_exp_gained / uptime_h) if uptime_h > 0 else 0
        matches_rate = int(bot_state.matches_started / uptime_h) if uptime_h > 0 else 0
        logs_out = []
        for l in bot_state.logs[-200:]:
            try:
                logs_out.append({"time": str(l.get("time","")),
                                 "message": str(l.get("message","")),
                                 "level": str(l.get("level","info"))})
            except Exception: continue
        squads_out = [
            {"id": sid, "leader": sq["leader"], "members": len(sq["all_uids"]),
             "team_code": sq["team_code"], "started": sq["started"],
             "member_uids": list(sq["all_uids"])}
            for sid, sq in _squads.items()
        ]
        data = {
            "accounts": accs, "logs": logs_out,
            "total_accounts": len(accs), "total_online": online,
            "total_matches": bot_state.total_matches,
            "total_matches_started": bot_state.matches_started,
            "total_active_matches": active_matches,
            "total_gained_exp": bot_state.total_exp_gained,
            "uptime": uptime, "exp_per_hour": exp_rate,
            "matches_per_hour": matches_rate,
            "creating": bool(bot_state.creating),
            "create_progress": [str(x) for x in bot_state.create_progress[-40:]],
            "squads": squads_out,
            "team_size": len(_team_state["peers"]),
            "restart_finished": len(_restart_state["finished_bots"]),
            "restart_target": _restart_state["target"],
            "restarting": _restart_state["restarting"],
        }
        return aiohttp.web.Response(text=json.dumps(data, default=str, ensure_ascii=False),
                                    content_type="application/json", charset="utf-8")
    except Exception as e:
        traceback.print_exc()
        return aiohttp.web.Response(text=json.dumps({"error": str(e), "accounts": [],
            "logs": [], "create_progress": [], "creating": False, "squads": [],
            "total_accounts": 0, "total_matches_started": 0, "total_matches": 0,
            "total_active_matches": 0, "total_gained_exp": 0, "uptime": 1,
            "exp_per_hour": 0}), content_type="application/json", charset="utf-8")

async def api_state(request):
    try:
        squads_out = [
            {"id": sid, "leader": sq["leader"], "members": len(sq["all_uids"]),
             "team_code": sq["team_code"], "started": sq["started"]}
            for sid, sq in _squads.items()
        ]
        return aiohttp.web.Response(text=json.dumps({
            "creating": bool(bot_state.creating),
            "create_progress": [str(x) for x in bot_state.create_progress[-40:]],
            "accounts": list(bot_state.accounts.values()),
            "squads": squads_out,
            "team_size": len(_team_state["peers"]),
        }, default=str, ensure_ascii=False), content_type="application/json", charset="utf-8")
    except Exception as e:
        return aiohttp.web.Response(text=json.dumps({"error": str(e), "creating": False,
                                    "create_progress": [], "squads": []}),
                                    content_type="application/json", charset="utf-8")

async def api_progress(request):
    try:
        lines = [str(x) for x in bot_state.create_progress[-60:]]
        text = '{"creating":' + ("true" if bot_state.creating else "false") + \
               ',"create_progress":' + json.dumps(lines) + '}'
        return aiohttp.web.Response(text=text, content_type="application/json")
    except Exception:
        return aiohttp.web.Response(text='{"creating":false,"create_progress":[]}',
                                    content_type="application/json")

async def api_create_team(request):
    try:
        body = await request.json()
        region = str(body.get("region", "ID")).upper()
        clan_id = str(body.get("clan_id", "")).strip()
        count = int(body.get("count", 4))
        password = str(body.get("password", "ANON")).strip() or "ANON"
        if not clan_id:
            return aiohttp.web.json_response({"ok": False, "error": "clan_id required"})
        if bot_state.creating:
            return aiohttp.web.json_response({"ok": False, "error": "already creating"})
        count = max(1, min(4, count))
        asyncio.create_task(create_four_accounts(region, clan_id, count, password))
        return aiohttp.web.json_response({"ok": True, "msg": f"creating {count} bots"})
    except Exception as e:
        traceback.print_exc()
        return aiohttp.web.json_response({"ok": False, "error": str(e)})

async def api_account_add(request):
    try:
        body = await request.json()
        if body.get("token"):
            token = str(body["token"]).strip()
            if not token:
                return aiohttp.web.json_response({"status": "error", "error": "empty token"})
            asyncio.create_task(account_loop_token(token))
            return aiohttp.web.json_response({"status": "ok"})
        uid = str(body.get("uid", "")).strip()
        password = str(body.get("password", "")).strip()
        if not uid or not password:
            return aiohttp.web.json_response({"status": "error", "error": "uid/password required"})
        asyncio.create_task(account_loop_guest(uid, password))
        return aiohttp.web.json_response({"status": "ok"})
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "error": str(e)})

async def api_account_delete(request):
    try:
        body = await request.json()
        uid = str(body.get("uid", "")).strip()
        auth_uid = str(body.get("auth_uid", "")).strip()
        for u in {uid, auth_uid} - {""}:
            if u in bot_state.account_workers:
                try: bot_state.account_workers[u].cancel()
                except Exception: pass
                bot_state.account_workers.pop(u, None)
            if u in bot_state.accounts: del bot_state.accounts[u]
            cache_invalidate(u)
            bot_state.toggle_pause(u, False)
        return aiohttp.web.json_response({"status": "ok"})
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "error": str(e)})

async def api_account_refresh(request):
    try:
        body = await request.json()
        uid = str(body.get("uid", "")).strip()
        asyncio.create_task(refresh_account_profile(uid))
        return aiohttp.web.json_response({"status": "ok"})
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "error": str(e)})

async def api_account_restart(request):
    try:
        body = await request.json()
        uid = str(body.get("uid", "")).strip()
        resolved = {uid}
        if uid in bot_state.game_to_auth_id: resolved.add(str(bot_state.game_to_auth_id[uid]))
        if uid in bot_state.auth_to_game_id: resolved.add(str(bot_state.auth_to_game_id[uid]))
        for u in resolved:
            if u in bot_state.account_workers:
                try: bot_state.account_workers[u].cancel()
                except Exception: pass
                bot_state.account_workers.pop(u, None)
        try:
            if os.path.exists(ACCOUNTS_FILE):
                with open(ACCOUNTS_FILE,"r",encoding="utf-8") as f:
                    accs = json.load(f)
                for a in accs:
                    if str(a.get("uid")) in resolved and a.get("password"):
                        t = asyncio.create_task(account_loop_guest(str(a["uid"]), a["password"]))
                        bot_state.account_workers[str(a["uid"])] = t
                        break
        except Exception: pass
        return aiohttp.web.json_response({"status": "ok"})
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "error": str(e)})

async def api_account_pause(request):
    try:
        body = await request.json()
        uid = str(body.get("uid", "")).strip()
        new_state = not bot_state.is_paused(uid)
        bot_state.toggle_pause(uid, new_state)
        if new_state: bot_state.close_writers_for_account(uid)
        return aiohttp.web.json_response({"status": "ok", "is_paused": new_state})
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "error": str(e)})

async def api_account_pause_all(request):
    try:
        accs = list(bot_state.accounts.keys())
        if not accs:
            return aiohttp.web.json_response({"status": "ok", "all_paused": False})
        all_paused = all(bot_state.is_paused(u) for u in accs)
        new_state = not all_paused
        for u in accs:
            bot_state.toggle_pause(u, new_state)
            if new_state: bot_state.close_writers_for_account(u)
        return aiohttp.web.json_response({"status": "ok", "all_paused": new_state})
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "error": str(e)})

async def api_logs_clear(request):
    bot_state.logs = []
    return aiohttp.web.json_response({"status": "ok"})

async def account_loop_token(token):
    print_info(f"[TOKEN] Login {token[:20]}...")
    try:
        vc = await version_config("ID")
        if not vc: return
        release_version, client_version, server_url, gop_1, app_version = vc
        url = f"https://100067.connect.garena.com/oauth/token/inspect?token={token}"
        hdrs = {"User-Agent":"GarenaMSDK/4.0.19P4(G011A ;Android 9;en;US;)"}
        r = await client.get(url, headers=hdrs)
        if r.status_code != 200: return
        data = r.json()
        if 'error' in data: return
        open_id = data.get('open_id'); platform = data.get('platform', 4)
        if not open_id: return
        major_payload, login_payload = await get_payload(
            token, open_id, platform, client_version)
        res_json = await major_login(major_payload, server_url, release_version)
        if not res_json: return
        def _gd(k):
            v = res_json.get(str(k))
            if isinstance(v, dict) and "data" in v: return v["data"]
            return v
        acc_id_raw = _gd(1); jwt_token = _gd(8)
        server_url_raw = _gd(10); server_time = _gd(21)
        region_raw = _gd(2) or "ID"
        aes_ak, iv_i = _extract_key_iv(res_json)
        if not (jwt_token and server_url_raw and aes_ak): return
        ld = await get_login_data(login_payload, jwt_token, server_url_raw, release_version)
        def _gf(f, d=None):
            v = (ld or {}).get(str(f), {}).get("data")
            return v if v is not None else d
        level = int(_gf(6,1) or 1); exp = int(_gf(7,0) or 0)
        nickname = _gf(4, f"Player_{acc_id_raw}") or f"Player_{acc_id_raw}"
        ad = {'account_id':acc_id_raw,'nickname':nickname,'region':region_raw,
            'level':level,'exp':exp,'likes':int(_gf(8,0) or 0),
            'open_id':open_id,'access_token':token,'platform':str(platform),
            'token':jwt_token,'server_time':server_time,
            'aes_ak':aes_ak,'iv_i':iv_i,
            'functional_addrs':_gf_field(ld, 14),
            'informational_addrs':_gf_field(ld, 32),
            'release_version':release_version,'client_version':client_version,
            'server_url':server_url_raw,'login_payload_data':login_payload,
            'auth_type':'token','auth_token':token}
        _register_credentials(ad)
        cache_set(f"tok_{token[:20]}", ad)
        await run_account_worker(ad, str(acc_id_raw))
    except Exception as e:
        print_error(f"[TOKEN] {e}")


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


async def start_web_server():
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

# ==================== MAIN ====================
async def _start_legacy_elite_dashboard():
    try:
        import dashboard_server as dashboard
        dashboard.bot_state = bot_state
        dashboard.handle_get_stats = api_stats
        dashboard.handle_api_logs = api_logs
        dashboard.handle_add_account = api_account_add
        dashboard.handle_delete_account = api_account_delete
        dashboard.handle_refresh_account = api_account_refresh
        dashboard.handle_restart_account = api_account_restart
        dashboard.handle_toggle_pause = api_account_pause
        dashboard.handle_toggle_pause_all = api_account_pause_all
        dashboard.handle_clear_logs = api_logs_clear
        dashboard.handle_api_clan_join = api_clanjoin
        async def live_admin_bot_accounts(request):
            return aiohttp.web.json_response({"accounts": list(bot_state.accounts.values())})
        dashboard.handle_admin_bot_accounts = live_admin_bot_accounts
        runner = await dashboard.start_web_dashboard(host=WEB_HOST, port=7000)
        print_success("[WEB] Elite Arena pages/auth/admin → http://localhost:7000")
        return runner
    except Exception as e:
        print_error(f"[WEB] Legacy dashboard failed to start: {e}")
        traceback.print_exc()
        return None

def print_logo():
    """3D 'EMON' banner (ANSI-Shadow style) with a cyan -> magenta gradient."""
    rows = [
        "███████╗███╗   ███╗ ██████╗ ███╗   ██╗",
        "██╔════╝████╗ ████║██╔═══██╗████╗  ██║",
        "█████╗  ██╔████╔██║██║   ██║██╔██╗ ██║",
        "██╔══╝  ██║╚██╔╝██║██║   ██║██║╚██╗██║",
        "███████╗██║ ╚═╝ ██║╚██████╔╝██║ ╚████║",
        "╚══════╝╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝",
    ]
    pal = [51, 45, 39, 33, 99, 135, 171, 201]          # cyan -> blue -> magenta
    width = max(len(r) for r in rows)
    print()
    for r in rows:
        out = ""
        for i, ch in enumerate(r):
            if ch == " ":
                out += " "
            elif ch == "█":                                 # front face: bright gradient
                out += f"\033[1;38;5;{pal[min(i * len(pal) // width, len(pal) - 1)]}m{ch}"
            else:                                           # 3D edge/shadow: dim
                out += f"\033[38;5;240m{ch}"
        print(out + "\033[0m")
    print("\033[1;38;5;220m" + "👑 EMON 👑".center(width) + "\033[0m")
    print("\033[38;5;245m" + f"FF 4-Bot SQUADS | Real Movement | Mode: {GAME_MODE}".center(width) + "\033[0m")
    print()

async def main():
    print_logo()
    await _start_legacy_elite_dashboard()
    try:
        await start_web_server()
    except Exception as e:
        print_error(f"[WEB] {e}"); return
    try:
        await _autoload_accounts()
    except Exception as e:
        print_error(f"[BOOT] autoload failed: {e}")
    asyncio.create_task(_no_match_watchdog())

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
    asyncio.create_task(_guild_xp_tracker())
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print_warning("Shutting down...")
        for t in list(bot_state.account_workers.values()):
            t.cancel()
        await asyncio.gather(*bot_state.account_workers.values(), return_exceptions=True)
        try: await client.aclose()
        except Exception: pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print_warning("stopped")