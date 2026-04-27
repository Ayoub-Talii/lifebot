import discord
from discord import app_commands
from discord.ext import commands, tasks
from discord.ui import View, Button, Select, Modal, TextInput
import os, random, time, logging, string, asyncio
from collections import Counter
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()
TOKEN     = os.getenv("DISCORD_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(open(1, "w", encoding="utf-8", closefd=False)),
    ]
)
log = logging.getLogger(__name__)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# ================================================================
# DATABASE
# ================================================================
_mongo  = MongoClient(MONGO_URI)
_db     = _mongo["lifebot"]
_pcol   = _db["players"]      # players collection
_gcol   = _db["gangs"]        # gangs collection
_tcol   = _db["territories"]  # territories collection
_bncol  = _db["bounties"]     # bounties collection
_ltcol  = _db["lottery"]     # lottery collection
_propcol = _db["properties"] # property income collection (persists lottery state + prop incomes)

def load_data() -> dict:
    result = {}
    for doc in _pcol.find():
        uid = str(doc.pop("_id"))
        result[uid] = doc
    return result

def save_data():
    if not players: return
    ops = [UpdateOne({"_id": uid}, {"$set": {**data, "_id": uid}}, upsert=True)
           for uid, data in players.items()]
    _pcol.bulk_write(ops, ordered=False)

def load_gangs() -> dict:
    result = {}
    for doc in _gcol.find():
        gid = str(doc.pop("_id"))
        result[gid] = doc
    return result

def save_gangs():
    if not gangs: return
    ops = [UpdateOne({"_id": gid}, {"$set": {**data, "_id": gid}}, upsert=True)
           for gid, data in gangs.items()]
    _gcol.bulk_write(ops, ordered=False)

def delete_gang(gid: str):
    _gcol.delete_one({"_id": gid})

def load_territories() -> dict:
    result = {}
    for doc in _tcol.find():
        tid = str(doc.pop("_id"))
        result[tid] = doc
    return result

def save_territory(tid: str):
    data = territories.get(tid, {})
    _tcol.replace_one({"_id": tid}, {"_id": tid, **data}, upsert=True)

def load_bounties() -> dict:
    result = {}
    for doc in _bncol.find():
        tid = str(doc.pop("_id"))
        result[tid] = doc
    return result

def save_bounties():
    if not bounties: return
    ops = [UpdateOne({"_id": tid}, {"$set": {**data, "_id": tid}}, upsert=True)
           for tid, data in bounties.items()]
    _bncol.bulk_write(ops, ordered=False)

def delete_bounty_db(target_uid: str):
    _bncol.delete_one({"_id": target_uid})

def load_lottery() -> dict:
    doc = _ltcol.find_one({"_id": "state"}) or {}
    doc.pop("_id", None)
    return doc

def save_lottery():
    _ltcol.replace_one({"_id": "state"}, {"_id": "state", **lottery_state}, upsert=True)

players     = load_data()
gangs:       dict = load_gangs()
bounties:    dict = load_bounties()   # target_uid -> {total, entries, placed_at}
active_mines:   dict = {}              # uid -> mines game state
active_chicken: dict = {}              # uid -> chicken cross game state
territories: dict = {}   # populated below by init_territories()

def _next_sunday_ts() -> int:
    import datetime
    now        = datetime.datetime.utcnow()
    days_ahead = (6 - now.weekday()) % 7 or 7  # 6 = Sunday
    next_sun   = (now + datetime.timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(next_sun.timestamp())

_lt_raw      = load_lottery()
lottery_state: dict = {
    "pot":         _lt_raw.get("pot",        0),
    "tickets":     _lt_raw.get("tickets",    {}),
    "draw_at":     _lt_raw.get("draw_at",    0) or _next_sunday_ts(),
    "last_winner": _lt_raw.get("last_winner", {}),
}

for uid in players:
    p = players[uid]
    p.setdefault("money", 1000)
    p.setdefault("bank", 0)
    p.setdefault("level", 1)
    p.setdefault("xp", 0)
    p.setdefault("items", [])
    p.setdefault("stats", {"wins": 0, "losses": 0, "total_won": 0, "total_lost": 0, "jobs_done": 0, "crimes_done": 0})
    p.setdefault("flags", {})
    p.setdefault("cooldowns", {})
    p.setdefault("equipped", {"theme": None, "skin": None})
    p.setdefault("heat", 0)
    p.setdefault("streak", 0)
    p.setdefault("last_daily", 0)
    p.setdefault("energy", 100)
    p.setdefault("last_energy_update", time.time())
    p.setdefault("prestige", 0)
    p.setdefault("jailed_until", 0)
    p.setdefault("gang_id", None)
    p.setdefault("casino_session", {"bj": 0, "slots": 0, "reset_at": 0})
    p.setdefault("casino_daily_loss", {"amount": 0, "reset_at": 0})
    p.setdefault("job_streak", {"key": None, "count": 0})
    p.setdefault("bank_deposit_time", 0)

# ================================================================
# CONFIG
# ================================================================
SHOP = {
    "scratch_card":     {"price": 200,  "emoji": "🎫", "desc": "Scratch to win! Match 2 same = 2× · Match 3 = 10× (1 use)", "type": "active"},
    "luck_charm":       {"price": 800,  "emoji": "🍀", "desc": "+15% crime success & gambling luck (1 use)",  "type": "active"},
    "card_peek":        {"price": 600,  "emoji": "👁️",  "desc": "Preview the next card in Blackjack (1 use)", "type": "active"},
    "insurance_shield": {"price": 1000, "emoji": "🛡️", "desc": "Lose only 50% of bet on bust (1 use)",        "type": "active"},
    "double_boost":     {"price": 1500, "emoji": "⚡",  "desc": "Double winnings for 1 Blackjack round",       "type": "active"},
    "xp_booster":       {"price": 500,  "emoji": "🚀",  "desc": "2x XP for your next 10 rounds",               "type": "passive"},
    "heat_shield":      {"price": 700,  "emoji": "🧊",  "desc": "Reduce heat by 3 instantly",                   "type": "active"},
    "lucky_spin":       {"price": 350,  "emoji": "🎡",  "desc": "+50% payout on next Slots spin",               "type": "active"},
    "energy_drink":     {"price": 200,  "emoji": "⚡",  "desc": "Restore 50 energy instantly",                  "type": "active"},
    "bail_bond":        {"price": 1200, "emoji": "⛓️",  "desc": "Get out of jail instantly (1 use)",            "type": "active"},
    "avatar_skin":      {"price": 400,  "emoji": "🎭",  "desc": "Crown icon next to your name (cosmetic)",      "type": "cosmetic"},
    "mask":             {"price": 600,  "emoji": "🥷",  "desc": "Required to steal — +10% success, -1 heat (1 use)",      "type": "active"},
    "gloves":           {"price": 500,  "emoji": "🧤",  "desc": "+25% escape chance if steal fails (1 use)",              "type": "active"},
    "scanner":          {"price": 400,  "emoji": "🔭",  "desc": "Required to scan a target's wallet & chance (1 use)",    "type": "active"},
    "tracker":          {"price": 350,  "emoji": "📡",  "desc": "Required to activate revenge against your thief (1 use)", "type": "active"},
    "theme_fire":     {"price": 800,  "emoji": "🔥", "desc": "Fire theme — orange-red embeds",   "type": "theme"},
    "theme_ocean":    {"price": 800,  "emoji": "🌊", "desc": "Ocean theme — deep blue embeds",   "type": "theme"},
    "theme_gold":     {"price": 1200, "emoji": "💛", "desc": "Gold theme — shiny gold embeds",   "type": "theme"},
    "theme_neon":     {"price": 1000, "emoji": "💚", "desc": "Neon theme — electric green",      "type": "theme"},
    "theme_royal":    {"price": 1500, "emoji": "👑", "desc": "Royal theme — deep purple",        "type": "theme"},
    "theme_midnight": {"price": 1000, "emoji": "🌙", "desc": "Midnight theme — dark navy",       "type": "theme"},
    "theme_crimson":  {"price": 900,  "emoji": "❤️", "desc": "Crimson theme — dark red",         "type": "theme"},
    "theme_ice":      {"price": 900,  "emoji": "🧊", "desc": "Ice theme — cool cyan",            "type": "theme"},
}

# Energy costs per action
ENERGY_COST = {"safe_job": 15, "risky_job": 25, "skill_job": 35, "crime": 30, "gamble": 10, "slots": 10, "blackjack": 15, "steal": 20}

# Gang system config
GANG_CREATE_COST      = 2000
GANG_CREATE_LEVEL_REQ = 5
GANG_HEIST_COOLDOWN   = 21600   # 6 hours (legacy, kept for old references)
GANG_ATTACK_COOLDOWN  = 3600    # 1 hour per member
GANG_WAR_DURATION     = 86400   # 24 hours

HEIST_TIERS = {
    "corner_store": {
        "name": "Corner Store", "emoji": "🏪",
        "min_members": 1, "min_gang_level": 1,
        "loot": (500, 1_500), "fail_chance": 0.15,
        "cd": 1800,       # 30 min
        "gang_xp": 50,
        "penalty": 0,
    },
    "city_bank": {
        "name": "City Bank", "emoji": "🏦",
        "min_members": 3, "min_gang_level": 2,
        "loot": (3_000, 6_000), "fail_chance": 0.30,
        "cd": 7200,       # 2 h
        "gang_xp": 150,
        "penalty": 500,
    },
    "diamond_vault": {
        "name": "Diamond Vault", "emoji": "💎",
        "min_members": 5, "min_gang_level": 4,
        "loot": (10_000, 20_000), "fail_chance": 0.45,
        "cd": 21600,      # 6 h
        "gang_xp": 400,
        "penalty": 2_000,
    },
    "federal_reserve": {
        "name": "Federal Reserve", "emoji": "🚀",
        "min_members": 8, "min_gang_level": 5,
        "loot": (40_000, 80_000), "fail_chance": 0.60,
        "cd": 86400,      # 24 h
        "gang_xp": 1_000,
        "penalty": 8_000,
    },
}

HEIST_ROLES = {
    "hacker": {
        "name": "Hacker", "emoji": "💻",
        "desc": "Reduces heist fail chance by 10%",
        "fail_reduce": 0.10, "loot_bonus": 0.0, "cd_reduce": 0.0, "guard": False,
    },
    "guard": {
        "name": "Guard", "emoji": "🔫",
        "desc": "Cuts member losses in half if heist fails",
        "fail_reduce": 0.0, "loot_bonus": 0.0, "cd_reduce": 0.0, "guard": True,
    },
    "bag_man": {
        "name": "Bag Man", "emoji": "💰",
        "desc": "Boosts personal loot share by 20%",
        "fail_reduce": 0.0, "loot_bonus": 0.20, "cd_reduce": 0.0, "guard": False,
    },
    "getaway": {
        "name": "Getaway Driver", "emoji": "🚗",
        "desc": "Reduces heist cooldown by 25%",
        "fail_reduce": 0.0, "loot_bonus": 0.0, "cd_reduce": 0.25, "guard": False,
    },
}
GANG_WAR_STAKE        = 0.15    # 15% of loser's bank
GANG_INVITE_TTL       = 3600    # invite expires in 1 hour
MAX_OFFICERS          = 3
GANG_LEVEL_TABLE = [
    #  (xp_req, max_members, perks)
    (0,    5,  []),
    (500,  8,  ["💼 +5% job pay"]),
    (1500, 10, ["💼 +5% job pay", "🏦 Gang Heist"]),
    (3500, 12, ["💼 +5% job pay", "🏦 Gang Heist", "🦹 +10% steal success"]),
    (7000, 15, ["💼 +5% job pay", "🏦 Gang Heist", "🦹 +10% steal success", "💰 +15% all income", "⚔️ Gang War"]),
]

# Territory system config
TERRITORIES = {
    # ── Common (high drop rate) ───────────────────────────────────
    "slums": {
        "name": "The Slums",      "emoji": "🏚️", "rarity": "common",
        "desc": "Back alleys where anything goes — perfect for criminals.",
        "income": (500, 1000),  "income_cd": 21600, "base_def": 60,
        "perks": ["+15% steal success", "-1 heat on crime"],
        "perk_keys": ["steal_15", "crime_heat_1"],
    },
    "port": {
        "name": "The Port",       "emoji": "⚓", "rarity": "common",
        "desc": "Smuggler's paradise — the perfect heist staging ground.",
        "income": (800, 1500),  "income_cd": 21600, "base_def": 80,
        "perks": ["+25% gang heist loot"],
        "perk_keys": ["heist_25"],
    },
    "train_yard": {
        "name": "Train Yard",     "emoji": "🚂", "rarity": "common",
        "desc": "A busy rail hub — perfect for smuggling contraband across the city.",
        "income": (400, 900),   "income_cd": 21600, "base_def": 50,
        "perks": ["+10% crime success", "-1 heat on steal"],
        "perk_keys": ["crime_10", "steal_heat_1"],
    },
    "bar_district": {
        "name": "Bar District",   "emoji": "🍺", "rarity": "common",
        "desc": "Neon-lit streets where every bartender knows the local score.",
        "income": (300, 700),   "income_cd": 21600, "base_def": 40,
        "perks": ["+10% crime success"],
        "perk_keys": ["crime_10"],
    },
    "industrial_zone": {
        "name": "Industrial Zone","emoji": "🏭", "rarity": "common",
        "desc": "Smoke-filled factories ideal for running a chop shop off the books.",
        "income": (450, 850),   "income_cd": 21600, "base_def": 55,
        "perks": ["+10% job pay"],
        "perk_keys": ["work_10"],
    },
    "the_projects": {
        "name": "The Projects",   "emoji": "🏘️", "rarity": "common",
        "desc": "Tight-knit blocks where loyalty is everything.",
        "income": (350, 750),   "income_cd": 21600, "base_def": 45,
        "perks": ["+10% steal success"],
        "perk_keys": ["steal_10"],
    },
    "highway": {
        "name": "The Highway",    "emoji": "🛣️", "rarity": "common",
        "desc": "Control the roads, control the flow of goods and cash.",
        "income": (380, 800),   "income_cd": 21600, "base_def": 45,
        "perks": ["-2 heat on crime"],
        "perk_keys": ["crime_heat_2"],
    },
    # ── Rare ─────────────────────────────────────────────────────
    "downtown": {
        "name": "Downtown",       "emoji": "🏙️", "rarity": "rare",
        "desc": "The financial heart of the city — controls the money flow.",
        "income": (1000, 2000), "income_cd": 21600, "base_def": 100,
        "perks": ["+20% job income", "+10% bank interest"],
        "perk_keys": ["work_20", "bank_10"],
    },
    "tech_zone": {
        "name": "Tech Zone",      "emoji": "💻", "rarity": "rare",
        "desc": "High-tech innovation district — hack smarter, not harder.",
        "income": (700, 1200),  "income_cd": 21600, "base_def": 70,
        "perks": ["-20% hacker fail chance", "+10% crime success"],
        "perk_keys": ["hack_20", "crime_10"],
    },
    "shopping_mall": {
        "name": "Shopping Mall",  "emoji": "🏪", "rarity": "rare",
        "desc": "A front for black market goods moving through the city's busiest retail zone.",
        "income": (800, 1600),  "income_cd": 21600, "base_def": 80,
        "perks": ["+15% all income"],
        "perk_keys": ["income_15"],
    },
    "casino_block": {
        "name": "Casino Block",   "emoji": "🎰", "rarity": "rare",
        "desc": "The city's gambling strip — whoever controls this prints money.",
        "income": (900, 1800),  "income_cd": 21600, "base_def": 90,
        "perks": ["+20% gambling winnings", "+10% job pay"],
        "perk_keys": ["gamble_20", "work_10"],
    },
    "harbor": {
        "name": "The Harbor",     "emoji": "🚢", "rarity": "rare",
        "desc": "Cargo ships and bribes — another perfect heist staging ground.",
        "income": (750, 1400),  "income_cd": 21600, "base_def": 80,
        "perks": ["+20% gang heist loot", "+5% crime success"],
        "perk_keys": ["heist_20", "crime_5"],
    },
    "chinatown": {
        "name": "Chinatown",      "emoji": "🏮", "rarity": "rare",
        "desc": "The underground market — nothing is tracked, everything is for sale.",
        "income": (700, 1500),  "income_cd": 21600, "base_def": 75,
        "perks": ["+15% steal success", "-1 heat on steal"],
        "perk_keys": ["steal_15", "steal_heat_1"],
    },
    "university_district": {
        "name": "University District","emoji": "🎓", "rarity": "rare",
        "desc": "Bright minds turned to crime — the best hackers live here.",
        "income": (650, 1300),  "income_cd": 21600, "base_def": 70,
        "perks": ["-25% hacker fail chance", "+10% crime success"],
        "perk_keys": ["hack_25", "crime_10"],
    },
    # ── Epic ─────────────────────────────────────────────────────
    "city_hall": {
        "name": "City Hall",      "emoji": "🏛️", "rarity": "epic",
        "desc": "Political power means legal cover — own this and nothing sticks.",
        "income": (1500, 2800), "income_cd": 21600, "base_def": 130,
        "perks": ["-3 heat on crime", "+15% all income"],
        "perk_keys": ["crime_heat_3", "income_15"],
    },
    "the_armory": {
        "name": "The Armory",     "emoji": "⚔️", "rarity": "epic",
        "desc": "A hidden weapons cache — power to whoever bids highest.",
        "income": (1200, 2500), "income_cd": 21600, "base_def": 140,
        "perks": ["+25% steal success", "+20% crime success"],
        "perk_keys": ["steal_25", "crime_20"],
    },
    "stock_exchange": {
        "name": "Stock Exchange",  "emoji": "📈", "rarity": "epic",
        "desc": "Launder millions through the markets — the cleanest dirty money in town.",
        "income": (1800, 3200), "income_cd": 21600, "base_def": 125,
        "perks": ["+25% bank interest", "+20% job income"],
        "perk_keys": ["bank_25", "work_20"],
    },
    # ── Legendary ────────────────────────────────────────────────
    "diamond_district": {
        "name": "Diamond District","emoji": "💎", "rarity": "legendary",
        "desc": "The crown jewel of the city — whoever holds it dominates everything.",
        "income": (3000, 5000), "income_cd": 21600, "base_def": 150,
        "perks": ["+50% all income"],
        "perk_keys": ["income_50"],
    },
    "federal_reserve": {
        "name": "Federal Reserve", "emoji": "🏦", "rarity": "legendary",
        "desc": "The nation's vault — control it and you control the city's lifeblood.",
        "income": (2500, 4500), "income_cd": 21600, "base_def": 160,
        "perks": ["+40% all income", "+15% bank interest"],
        "perk_keys": ["income_40", "bank_15"],
    },
    # ── Extra Legendary ──────────────────────────────────────────
    "shadow_empire": {
        "name": "Shadow Empire",  "emoji": "👁️", "rarity": "extra_legendary",
        "desc": "The mythical underground that controls everything from the shadows. Whoever holds this rules the city.",
        "income": (5000, 10000), "income_cd": 21600, "base_def": 220,
        "perks": ["+75% all income", "+30% steal success"],
        "perk_keys": ["income_75", "steal_30"],
    },
}
TERRITORY_RARITY_WEIGHTS = {"common": 70, "rare": 25, "epic": 10, "legendary": 4, "extra_legendary": 1}
TERRITORY_HEAT            = {"common": 1,  "rare": 2,  "epic": 3, "legendary": 3, "extra_legendary": 5}
TERRITORY_RARITY_EMOJI    = {"common": "⬜", "rare": "🟦", "epic": "🟣", "legendary": "🟡", "extra_legendary": "🔴"}
CHEST_COST                = 1500   # deducted from gang treasury
CHEST_TERRITORY_DURATION  = (43200, 86400)  # 12–24 hours (random per roll)
BATTLE_DURATION           = 1800   # 30 minutes
CONTRIBUTE_CD             = 3600   # 1 hour per member per battle
TERRITORY_ATTACK_CD       = 7200   # 2 hours between attacks on same territory
MAX_TERRITORIES           = 1      # one active territory per gang (chest system)

STEAL_COOLDOWN        = 900    # 15 minutes
TARGET_STEAL_COOLDOWN = 3600   # 1 hour per target
MAX_ENERGY   = 200
ENERGY_REGEN = 180  # seconds per 1 energy point (200 energy in ~10 hours)

THEMES = {
    "default":   {"emoji": "🎨", "name": "Default",   "color": (88,  101, 242)},
    "fire":      {"emoji": "🔥", "name": "Fire",       "color": (255, 87,  34)},
    "ocean":     {"emoji": "🌊", "name": "Ocean",      "color": (0,   150, 199)},
    "gold":      {"emoji": "💛", "name": "Gold",       "color": (255, 193, 7)},
    "neon":      {"emoji": "💚", "name": "Neon",       "color": (57,  255, 20)},
    "royal":     {"emoji": "👑", "name": "Royal",      "color": (123, 31,  162)},
    "midnight":  {"emoji": "🌙", "name": "Midnight",   "color": (25,  32,  90)},
    "crimson":   {"emoji": "❤️", "name": "Crimson",    "color": (183, 28,  28)},
    "ice":       {"emoji": "🧊", "name": "Ice",        "color": (0,   188, 212)},
}

# Bank interest: 2% daily, capped at $500
BANK_INTEREST_RATE = 0.02
BANK_INTEREST_CAP  = 500

# Anti-inflation: players above $50k pay 1% wealth tax per day (max $1000)
TAX_THRESHOLD = 50_000
TAX_RATE      = 0.01
TAX_CAP       = 1_000

# Prestige: unlock at level 30, each prestige gives +5% job pay and +3% crime success
PRESTIGE_LEVEL_REQ  = 30
PRESTIGE_JOB_BONUS  = 0.05
PRESTIGE_CRIME_BONUS = 0.03

JOBS = {
    "taxi":     {"emoji":"🚕","name":"Taxi Driver",    "cat":"safe",  "pay":(80,180),   "cd":300,  "xp":10,  "req":1,  "fail":0.00, "fine":(0,0),     "heat":0},
    "waiter":   {"emoji":"🍽️","name":"Waiter",         "cat":"safe",  "pay":(60,150),   "cd":300,  "xp":10,  "req":1,  "fail":0.00, "fine":(0,0),     "heat":0},
    "delivery": {"emoji":"📦","name":"Delivery Guy",   "cat":"safe",  "pay":(100,200),  "cd":480,  "xp":12,  "req":1,  "fail":0.00, "fine":(0,0),     "heat":0},
    "hack":     {"emoji":"💻","name":"Hacker",         "cat":"risky", "pay":(300,600),  "cd":1200, "xp":25,  "req":1,  "fail":0.35, "fine":(100,200), "heat":1},
    "pick":     {"emoji":"👜","name":"Pickpocket",     "cat":"risky", "pay":(150,350),  "cd":900,  "xp":20,  "req":1,  "fail":0.40, "fine":(80,150),  "heat":1},
    "con":      {"emoji":"🎭","name":"Con Artist",     "cat":"risky", "pay":(200,500),  "cd":1500, "xp":22,  "req":1,  "fail":0.30, "fine":(100,200), "heat":2},
    "gambler":  {"emoji":"🎰","name":"Pro Gambler",    "cat":"skill", "pay":(400,800),  "cd":1800, "xp":35,  "req":5,  "fail":0.25, "fine":(150,250), "heat":0},
    "heist":    {"emoji":"🏦","name":"Bank Heist",     "cat":"skill", "pay":(800,1500), "cd":3600, "xp":60,  "req":10, "fail":0.45, "fine":(300,500), "heat":3},
    "diamond":  {"emoji":"💎","name":"Diamond Heist",  "cat":"skill", "pay":(1500,3000),"cd":7200, "xp":100, "req":20, "fail":0.50, "fine":(500,800), "heat":4},
}
INTERACTIVE_JOBS = {"hack", "heist", "diamond"}

JOB_PROMOTIONS = {
    "taxi":     {"emoji":"🚁","name":"Helicopter Pilot",   "cat":"safe",  "pay":(350, 700),   "cd":600,  "xp":30,  "req":1,  "fail":0.00, "fine":(0,0),      "heat":0, "req_count":20},
    "waiter":   {"emoji":"🍾","name":"Head Chef",          "cat":"safe",  "pay":(280, 580),   "cd":600,  "xp":28,  "req":1,  "fail":0.00, "fine":(0,0),      "heat":0, "req_count":20},
    "delivery": {"emoji":"🚀","name":"Express Courier",    "cat":"safe",  "pay":(420, 800),   "cd":720,  "xp":32,  "req":1,  "fail":0.00, "fine":(0,0),      "heat":0, "req_count":20},
    "hack":     {"emoji":"🖥️","name":"Elite Hacker",       "cat":"risky", "pay":(700, 1400),  "cd":1500, "xp":55,  "req":1,  "fail":0.20, "fine":(150,350),  "heat":1, "req_count":15},
    "pick":     {"emoji":"🎩","name":"Professional Thief", "cat":"risky", "pay":(400, 850),   "cd":1200, "xp":45,  "req":1,  "fail":0.25, "fine":(100,250),  "heat":1, "req_count":15},
    "con":      {"emoji":"🎪","name":"Master Manipulator", "cat":"risky", "pay":(550, 1200),  "cd":1800, "xp":50,  "req":1,  "fail":0.15, "fine":(150,350),  "heat":2, "req_count":15},
    "gambler":  {"emoji":"🎲","name":"Casino Shark",       "cat":"skill", "pay":(900, 1800),  "cd":2400, "xp":80,  "req":5,  "fail":0.15, "fine":(250,450),  "heat":0, "req_count":10},
    "heist":    {"emoji":"🏛️","name":"Federal Vault",      "cat":"skill", "pay":(2000, 4000), "cd":5400, "xp":130, "req":10, "fail":0.30, "fine":(500,900),  "heat":3, "req_count":10},
    "diamond":  {"emoji":"🗿","name":"Museum Heist",       "cat":"skill", "pay":(4000, 8000), "cd":10800,"xp":220, "req":20, "fail":0.35, "fine":(800,1400), "heat":4, "req_count":10},
}

def _job_count(p: dict, key: str) -> int:
    return p.get("job_counts", {}).get(key, 0)

def _is_promoted(p: dict, key: str) -> bool:
    return p.get("job_promotions", {}).get(key, False)

def _can_promote(p: dict, key: str) -> bool:
    if _is_promoted(p, key) or key not in JOB_PROMOTIONS: return False
    return _job_count(p, key) >= JOB_PROMOTIONS[key]["req_count"]

def _effective_job(p: dict, key: str) -> dict:
    if _is_promoted(p, key) and key in JOB_PROMOTIONS:
        return JOB_PROMOTIONS[key]
    return JOBS[key]

# ── Job Modes ────────────────────────────────────────────────────
JOB_MODES = {
    "safe":   {"label":"🟢 Safe",    "pay_mult":0.80, "fail_add":0.00,  "heat_mod":-1, "desc":"-20% pay · 0% fail · -1 heat"},
    "normal": {"label":"🟡 Normal",  "pay_mult":1.00, "fail_add":0.00,  "heat_mod": 0, "desc":"Default values"},
    "risk":   {"label":"🔴 Risk",    "pay_mult":1.40, "fail_add":0.20,  "heat_mod":+1, "desc":"+40% pay · +20% fail · +1 heat"},
    "allin":  {"label":"💀 All-In",  "pay_mult":2.00, "fail_add":0.40,  "heat_mod":+2, "desc":"+100% pay · +40% fail · +2 heat"},
}

# ── Per-job random events (20% trigger chance on success) ────────
JOB_EVENTS = {
    "taxi":     [
        {"msg":"🚖 VIP passenger — massive tip!",        "pay_pct": 0.40, "heat": 0,  "rare": False},
        {"msg":"🎵 Passenger left a playlist tip!",       "pay_pct": 0.20, "heat": 0,  "rare": False},
        {"msg":"😤 Drive-off — passenger skipped the bill!", "pay_pct":-0.20,"heat": 0, "rare": False},
    ],
    "waiter":   [
        {"msg":"🎉 Private party — doubled tips!",        "pay_pct": 0.50, "heat": 0,  "rare": False},
        {"msg":"⭐ Influencer reviewed your service!",    "pay_pct": 0.25, "heat": 0,  "rare": False},
        {"msg":"🤮 Food complaint — docked from your pay.","pay_pct":-0.20,"heat": 0,  "rare": False},
    ],
    "delivery": [
        {"msg":"📦 Express bonus — delivered in record time!", "pay_pct": 0.30, "heat": 0, "rare": False},
        {"msg":"🍕 Customer gave a cash tip at the door!",     "pay_pct": 0.20, "heat": 0, "rare": False},
        {"msg":"💥 Package damaged — you pay for it.",         "pay_pct":-0.25,"heat": 0, "rare": False},
    ],
    "hack":     [
        {"msg":"🏦 Found an unprotected secondary account!",  "pay_pct": 0.45, "heat": 0,  "rare": False},
        {"msg":"🔥 System logs wiped — heat reduced!",        "pay_pct": 0.00, "heat":-1,  "rare": False},
        {"msg":"🚨 Honeypot triggered — extra heat!",         "pay_pct": 0.00, "heat":+2,  "rare": False},
    ],
    "pick":     [
        {"msg":"💎 Found a loaded wallet — jackpot!",         "pay_pct": 0.50, "heat": 0,  "rare": False},
        {"msg":"👀 Nobody noticed — clean getaway.",          "pay_pct": 0.15, "heat":-1,  "rare": False},
        {"msg":"📸 Caught on CCTV — extra heat!",             "pay_pct": 0.00, "heat":+2,  "rare": False},
    ],
    "con":      [
        {"msg":"🎰 Mark doubled down — you cleaned them out!", "pay_pct": 0.60, "heat": 0, "rare": False},
        {"msg":"🤝 Left on good terms — no heat.",             "pay_pct": 0.10, "heat":-1, "rare": False},
        {"msg":"😡 Mark figured it out — furious!",            "pay_pct": 0.00, "heat":+2, "rare": False},
    ],
    "gambler":  [
        {"msg":"🃏 Read the table perfectly — bonus hand!",   "pay_pct": 0.40, "heat": 0, "rare": False},
        {"msg":"🍀 Lucky streak — house just couldn't beat you!", "pay_pct": 0.25, "heat": 0, "rare": False},
        {"msg":"🎲 House edge hit hard — smaller cut.",        "pay_pct":-0.20,"heat": 0, "rare": False},
    ],
    "heist":    [
        {"msg":"🏆 Found a secret vault inside!",             "pay_pct": 0.50, "heat": 0,  "rare": True},
        {"msg":"🚁 Clean escape — no witnesses.",              "pay_pct": 0.20, "heat":-1,  "rare": False},
        {"msg":"🚔 Dye pack exploded — some cash lost.",       "pay_pct":-0.25,"heat": 0,  "rare": False},
    ],
    "diamond":  [
        {"msg":"💎 Discovered a hidden gem cache!",           "pay_pct": 0.60, "heat": 0,  "rare": True},
        {"msg":"🌑 Blackout escape — no trail left.",          "pay_pct": 0.15, "heat":-2,  "rare": False},
        {"msg":"🔦 Guard patrol doubled — too hot.",           "pay_pct": 0.00, "heat":+2,  "rare": False},
    ],
}

def calculate_reward(base_pay: tuple, level: int, fatigue: float,
                     prestige_bonus: float, gang_mult: float, training_mult: float = 1.0) -> int:
    """Dynamic reward scaling: base × level scaling × all multipliers."""
    raw      = random.randint(*base_pay)
    level_sc = min(1.0 + level * 0.03, 2.5)   # cap at +150% from level alone
    return int(raw * level_sc * fatigue * prestige_bonus * gang_mult * training_mult)

def apply_mode(pay: int, fail_chance: float, heat_base: int,
               mode_key: str) -> tuple[int, float, int]:
    """Apply mode modifiers. Returns (final_pay, final_fail, final_heat_add)."""
    m      = JOB_MODES[mode_key]
    new_pay  = int(pay * m["pay_mult"])
    new_fail = max(0.0, min(0.95, fail_chance + m["fail_add"]))
    new_heat = max(0, heat_base + m["heat_mod"])
    return new_pay, new_fail, new_heat

def trigger_event(job_key: str, base_pay: int) -> tuple[str, int, int]:
    """20% chance to trigger a job event. Returns (msg, pay_delta, heat_delta)."""
    if random.random() > 0.20:
        return "", 0, 0
    ev       = random.choice(JOB_EVENTS.get(job_key, []))
    pay_delta = int(base_pay * ev["pay_pct"])
    return ev["msg"], pay_delta, ev["heat"]

XP_PER_LEVEL   = 150
STARTING_MONEY = 1000
WORK_COOLDOWN  = 300   # 5 minutes (legacy, kept for crime etc.)
CRIME_COOLDOWN = 1800  # 30 minutes

CRIME_TYPES = {
    "petty":   {"name": "🏪 Petty Theft",   "reward": (100,  300),  "catch": 0.15, "heat": 1, "energy": 15, "desc": "Easy target, low risk"},
    "pick":    {"name": "👜 Pickpocket",     "reward": (200,  500),  "catch": 0.25, "heat": 1, "energy": 20, "desc": "Quick hands, quick cash"},
    "hack":    {"name": "💻 Cyber Hack",     "reward": (400,  900),  "catch": 0.35, "heat": 2, "energy": 25, "desc": "Breach a system remotely"},
    "bank":    {"name": "🏦 Bank Job",       "reward": (800,  2000), "catch": 0.55, "heat": 3, "energy": 30, "desc": "High stakes armed robbery"},
    "jewelry": {"name": "💎 Jewelry Heist",  "reward": (1500, 4000), "catch": 0.70, "heat": 4, "energy": 40, "desc": "The big score — very risky"},
}

CRIME_RANKS = [
    {"name": "Street Rat",  "emoji": "🐀", "min": 0,   "bonus": 0.00},
    {"name": "Thug",        "emoji": "🔪", "min": 10,  "bonus": 0.05},
    {"name": "Gangster",    "emoji": "🔫", "min": 25,  "bonus": 0.10},
    {"name": "Kingpin",     "emoji": "👑", "min": 50,  "bonus": 0.20},
    {"name": "Crime Lord",  "emoji": "💀", "min": 100, "bonus": 0.30},
]

CRIME_LOOT_POOL = ["mask", "gloves", "scanner", "tracker", "luck_charm"]
JACKPOT_SEED   = 5000
jackpot_pool   = JACKPOT_SEED

# ── Lottery ───────────────────────────────────────────────────────
LOTTERY_TICKET_PRICE = 100
LOTTERY_WINNER_CUT   = 0.80   # 80% to winner, 20% to their gang bank
LOTTERY_SEED         = 2_000  # minimum jackpot added each week

# ── Properties ────────────────────────────────────────────────────
PROPERTIES = {
    "apartment":  {"name": "Apartment",  "emoji": "🏠", "cost": 2_000,  "income": (150,  300),  "cd": 21600},
    "storefront": {"name": "Storefront", "emoji": "🏪", "cost": 8_000,  "income": (600,  1_000), "cd": 21600},
    "warehouse":  {"name": "Warehouse",  "emoji": "🏭", "cost": 25_000, "income": (2_000, 3_500),"cd": 21600},
    "penthouse":  {"name": "Penthouse",  "emoji": "🏙️", "cost": 80_000, "income": (6_000, 10_000),"cd": 21600},
}
MAX_PROPERTIES    = 5   # max slots per player
PROPERTY_RAID_CD  = 14400  # 4h between raiding the same target

# ── Training ──────────────────────────────────────────────────────
# ── Career Paths ─────────────────────────────────────────────────
CAREER_PATHS = {
    "corporate": {
        "name": "Corporate", "emoji": "💼",
        "color": discord.Color.blue(),
        "desc": "Master of the legitimate hustle — more pay, better banking, lighter fatigue.",
        "perks": [
            "+20% job pay on all jobs",
            "Job fatigue penalty halved (−5% per repeat instead of −10%)",
            "+3% bank interest rate",
        ],
    },
    "hustler": {
        "name": "Hustler", "emoji": "🤑",
        "color": discord.Color.green(),
        "desc": "Fast, efficient, never stopping — cooldowns shrink and streaks hit harder.",
        "perks": [
            "Job cooldowns −30%",
            "Crime cooldown −25%",
            "Job streak bonus doubled (−5% fatigue per repeat instead of −10%... wait, no)",
        ],
    },
    "shadow": {
        "name": "Shadow", "emoji": "🕶️",
        "color": discord.Color.dark_gray(),
        "desc": "Ghost in the city — less heat, better steals, shorter jail time.",
        "perks": [
            "All heat gain −40%",
            "+8% steal success chance",
            "Jail duration −50%",
        ],
    },
}
CAREER_UNLOCK_LEVEL = 10

TRAINING_STATS = {
    "steal": {"name": "Steal Mastery",   "emoji": "🥷", "desc": "+3% steal success per level",     "bonus": 0.03},
    "crime": {"name": "Crime Expertise", "emoji": "🔫", "desc": "-3% crime catch chance per level", "bonus": 0.03},
    "jobs":  {"name": "Work Ethic",      "emoji": "💼", "desc": "+5% job pay per level",             "bonus": 0.05},
    "xp":    {"name": "Fast Learner",    "emoji": "⭐", "desc": "+10% XP gain per level",            "bonus": 0.10},
}
TRAINING_COSTS = [2_000, 5_000, 12_000, 30_000, 75_000]  # cost to reach level 1,2,3,4,5
TRAINING_MAX   = 5

# ── Achievements ──────────────────────────────────────────────────
ACHIEVEMENTS = {
    # 💰 Economy
    "first_paycheck": {"name": "First Paycheck",  "emoji": "💼", "cat": "💰 Economy",  "desc": "Complete your first job"},
    "grinder":        {"name": "Grinder",          "emoji": "⚙️",  "cat": "💰 Economy",  "desc": "Complete 50 jobs"},
    "workaholic":     {"name": "Workaholic",       "emoji": "🏭", "cat": "💰 Economy",  "desc": "Complete 100 jobs"},
    "thousandaire":   {"name": "Thousandaire",     "emoji": "💵", "cat": "💰 Economy",  "desc": "Reach $10,000 net worth"},
    "hundred_k":      {"name": "High Earner",      "emoji": "💰", "cat": "💰 Economy",  "desc": "Reach $100,000 net worth"},
    "millionaire":    {"name": "Millionaire",      "emoji": "🤑", "cat": "💰 Economy",  "desc": "Reach $1,000,000 net worth"},
    "diamond_hands":  {"name": "Diamond Hands",   "emoji": "💎", "cat": "💰 Economy",  "desc": "Save $100,000 in the bank"},
    # 🔫 Crime
    "first_crime":    {"name": "Going Bad",        "emoji": "🔫", "cat": "🔫 Crime",    "desc": "Commit your first crime"},
    "criminal":       {"name": "Career Criminal",  "emoji": "🕵️", "cat": "🔫 Crime",    "desc": "Commit 25 crimes"},
    "crime_lord":     {"name": "Crime Lord",       "emoji": "👑", "cat": "🔫 Crime",    "desc": "Commit 100 crimes"},
    "most_wanted":    {"name": "Most Wanted",      "emoji": "🌡️", "cat": "🔫 Crime",    "desc": "Reach Heat level 8+"},
    # ⚔️ PvP
    "pickpocket":     {"name": "Pickpocket",       "emoji": "🥷", "cat": "⚔️ PvP",      "desc": "Win your first steal or duel"},
    "street_king":    {"name": "Street King",      "emoji": "🗡️", "cat": "⚔️ PvP",      "desc": "Win 25 steals or duels"},
    "bounty_hunter":  {"name": "Bounty Hunter",    "emoji": "🎯", "cat": "⚔️ PvP",      "desc": "Claim your first bounty"},
    # 🎰 Casino
    "lucky":          {"name": "Lucky",            "emoji": "🍀", "cat": "🎰 Casino",   "desc": "Win 10 gambling games"},
    "high_roller":    {"name": "High Roller",      "emoji": "🎰", "cat": "🎰 Casino",   "desc": "Win $10,000 in casino total"},
    "whale":          {"name": "Whale",            "emoji": "🐋", "cat": "🎰 Casino",   "desc": "Win $100,000 in casino total"},
    "scratch_winner": {"name": "Lucky Ticket",     "emoji": "🎫", "cat": "🎰 Casino",   "desc": "Win big on a scratch card"},
    # ⭐ Progression
    "level_10":       {"name": "Rising Star",      "emoji": "⭐", "cat": "⭐ Progress", "desc": "Reach Level 10"},
    "level_25":       {"name": "Veteran",          "emoji": "🌟", "cat": "⭐ Progress", "desc": "Reach Level 25"},
    "prestige_1":     {"name": "Prestige I",       "emoji": "🔱", "cat": "⭐ Progress", "desc": "Prestige for the first time"},
    "daily_streak":   {"name": "Dedicated",        "emoji": "📅", "cat": "⭐ Progress", "desc": "Maintain a 7-day daily streak"},
    # 🏴 Gang
    "gang_member":    {"name": "Gang Member",      "emoji": "🏴", "cat": "🏴 Gang",     "desc": "Join or create a gang"},
    "heist_squad":    {"name": "Heist Squad",      "emoji": "🎯", "cat": "🏴 Gang",     "desc": "Complete a gang heist"},
}

def _ach_check(p: dict, aid: str) -> bool:
    s = p.get("stats", {})
    worth = p.get("money", 0) + p.get("bank", 0)
    checks = {
        "first_paycheck": s.get("jobs_done", 0) >= 1,
        "grinder":        s.get("jobs_done", 0) >= 50,
        "workaholic":     s.get("jobs_done", 0) >= 100,
        "thousandaire":   worth >= 10_000,
        "hundred_k":      worth >= 100_000,
        "millionaire":    worth >= 1_000_000,
        "diamond_hands":  p.get("bank", 0) >= 100_000,
        "first_crime":    s.get("crimes_done", 0) >= 1,
        "criminal":       s.get("crimes_done", 0) >= 25,
        "crime_lord":     s.get("crimes_done", 0) >= 100,
        "most_wanted":    p.get("heat", 0) >= 8,
        "pickpocket":     s.get("wins", 0) >= 1,
        "street_king":    s.get("wins", 0) >= 25,
        "bounty_hunter":  s.get("bounties_claimed", 0) >= 1,
        "lucky":          s.get("wins", 0) >= 10,
        "high_roller":    s.get("total_won", 0) >= 10_000,
        "whale":          s.get("total_won", 0) >= 100_000,
        "scratch_winner": s.get("scratch_wins", 0) >= 1,
        "level_10":       p.get("level", 1) >= 10,
        "level_25":       p.get("level", 1) >= 25,
        "prestige_1":     p.get("prestige", 0) >= 1,
        "daily_streak":   p.get("streak", 0) >= 7,
        "gang_member":    bool(p.get("gang_id")),
        "heist_squad":    s.get("heists_done", 0) >= 1,
    }
    return checks.get(aid, False)

def check_achievements(p: dict) -> list:
    """Check all achievements; return list of newly unlocked IDs."""
    unlocked = set(p.setdefault("achievements", []))
    newly    = []
    for aid in ACHIEVEMENTS:
        if aid not in unlocked and _ach_check(p, aid):
            p["achievements"].append(aid)
            newly.append(aid)
    return newly

def _ach_notify(new_ids: list) -> str:
    """Return a string to embed into a result embed footer/field."""
    if not new_ids: return ""
    lines = [f"{ACHIEVEMENTS[a]['emoji']} **{ACHIEVEMENTS[a]['name']}** unlocked!" for a in new_ids]
    return "\n".join(lines)

# ── Weekly Challenges ─────────────────────────────────────────────
WEEKLY_CHALLENGES = [
    {"id": "wc_jobs",   "name": "Hard Worker", "emoji": "💼", "desc": "Complete {goal} jobs",      "type": "jobs",      "goal": 10},
    {"id": "wc_crimes", "name": "Outlaw",       "emoji": "🔫", "desc": "Commit {goal} crimes",      "type": "crimes",    "goal": 5},
    {"id": "wc_steal",  "name": "Pickpocket",   "emoji": "🥷", "desc": "Steal ${goal:,} total",     "type": "steal_amt", "goal": 500},
    {"id": "wc_duels",  "name": "Duelist",      "emoji": "⚔️", "desc": "Win {goal} duels",          "type": "duel_wins", "goal": 2},
    {"id": "wc_heists", "name": "Heist Crew",   "emoji": "🎯", "desc": "Complete {goal} heists",    "type": "heists",    "goal": 3},
]
WEEKLY_XP_REWARD         = 500
WEEKLY_CASH_REWARD_RANGE = (1_500, 6_000)

def _next_monday_ts() -> int:
    import datetime
    now      = datetime.datetime.utcnow()
    days_ahead = (7 - now.weekday()) % 7 or 7
    next_mon = (now + datetime.timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(next_mon.timestamp())

def _ensure_weekly(p: dict):
    now = time.time()
    w   = p.setdefault("weekly", {"reset_at": 0, "progress": {}, "claimed": False})
    if now >= w.get("reset_at", 0):
        w["reset_at"] = _next_monday_ts()
        w["progress"] = {c["id"]: 0 for c in WEEKLY_CHALLENGES}
        w["claimed"]  = False
    for c in WEEKLY_CHALLENGES:
        w["progress"].setdefault(c["id"], 0)

def _weekly_inc(p: dict, ctype: str, amount: int = 1):
    _ensure_weekly(p)
    if p["weekly"]["claimed"]:
        return
    for c in WEEKLY_CHALLENGES:
        if c["type"] == ctype:
            cid = c["id"]
            p["weekly"]["progress"][cid] = min(c["goal"], p["weekly"]["progress"].get(cid, 0) + amount)

def _weekly_all_done(p: dict) -> bool:
    _ensure_weekly(p)
    return all(p["weekly"]["progress"].get(c["id"], 0) >= c["goal"] for c in WEEKLY_CHALLENGES)

def _weekly_embed(p: dict) -> discord.Embed:
    _ensure_weekly(p)
    w        = p["weekly"]
    done_ct  = sum(1 for c in WEEKLY_CHALLENGES if w["progress"].get(c["id"], 0) >= c["goal"])
    claimed  = w["claimed"]
    reset_ts = w["reset_at"]
    color    = discord.Color.gold() if done_ct == len(WEEKLY_CHALLENGES) else discord.Color.blurple()
    embed    = discord.Embed(
        title="📋 Weekly Challenges",
        description=(
            f"**{done_ct}/{len(WEEKLY_CHALLENGES)}** complete  •  Resets <t:{reset_ts}:R>\n"
            + ("✅ **Reward claimed!**" if claimed else
               "🎁 Complete all 5 to claim **cash + XP bonus**!")
        ),
        color=color
    )
    for c in WEEKLY_CHALLENGES:
        cid      = c["id"]
        prog     = w["progress"].get(cid, 0)
        goal     = c["goal"]
        bar_len  = 10
        filled   = int(bar_len * min(prog, goal) / goal)
        bar      = "█" * filled + "░" * (bar_len - filled)
        if c["type"] == "steal_amt":
            prog_txt = f"${prog:,}/${goal:,}"
        else:
            prog_txt = f"{prog}/{goal}"
        status   = "✅" if prog >= goal else "🔲"
        embed.add_field(
            name=f"{status} {c['emoji']} {c['name']}",
            value=f"{c['desc'].format(goal=goal)}\n`{bar}` {prog_txt}",
            inline=True
        )
    embed.set_footer(text="Challenges refresh every Monday at midnight UTC")
    return embed

# ── Live Roulette ─────────────────────────────────────────────────
ROULETTE_MIN_BET    = 100
ROULETTE_MAX_BET    = 50_000
ROULETTE_MAX_BETS   = 3          # bets per player per round
ROULETTE_BET_WINDOW = 20         # seconds to accept bets
ROULETTE_ROUND_GAP  = 30         # seconds between rounds

ROULETTE_RED_NUMS = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}

ROULETTE_PAYOUTS = {             # house-edge applied (true odds −1 step)
    "red":   1.9,
    "black": 1.9,
    "green": 14.0,
    "even":  1.9,
    "odd":   1.9,
    "low":   1.9,
    "high":  1.9,
    # straight number bets use key "number" dynamically (stored as "0"–"36")
}
ROULETTE_NUMBER_PAYOUT = 32.0    # straight-up number bet (true: 36x, house: 32x)
ROULETTE_KEYWORD_BETS  = set(ROULETTE_PAYOUTS.keys())

roulette_state = {
    "active":        False,
    "round_id":      0,
    "bets":          {},    # uid -> [{"type": str, "amount": int}, ...]
    "pending_bets":  {},    # uid -> [{"type": str, "amount": int}, ...] — queued for next round
    "phase":         "idle",
    "_start_time":   0,
    "next_round_at": 0,     # unix timestamp — when the next round begins
    "last_result":   None,  # uid -> result dict, set after each round
}
_roulette_channel_id = int(os.getenv("ROULETTE_CHANNEL_ID", "0")) or None
roulette_webhooks: dict = {}   # uid -> discord.Interaction (for auto-editing the panel)

# ── Server Events ─────────────────────────────────────────────────
SERVER_EVENTS = {
    "money_drop": {
        "name":  "💰 Money Drop",
        "color": discord.Color.gold(),
        "desc":  "A bag of cash fell from the sky! First player to click **Grab It!** wins.",
        "duration": 0,   # one-shot (first click wins)
    },
    "heat_wave": {
        "name":  "🔥 Heat Wave",
        "color": discord.Color.red(),
        "desc":  "A city-wide crackdown is underway! All players receive **+2 Heat** immediately.",
        "duration": 3600,
    },
    "xp_boost": {
        "name":  "🎉 XP Boost",
        "color": discord.Color.green(),
        "desc":  "Double XP active for **30 minutes**! All job XP is doubled.",
        "duration": 1800,
    },
    "immunity": {
        "name":  "🛡️ Immunity Window",
        "color": discord.Color.blue(),
        "desc":  "City police patrol active! **All steals are blocked** for 20 minutes.",
        "duration": 1200,
    },
    "cash_rain": {
        "name":  "🌧️ Cash Rain",
        "color": discord.Color.teal() if hasattr(discord.Color, 'teal') else discord.Color.blurple(),
        "desc":  "It's raining money! Every active player receives **$150–$400** for free.",
        "duration": 0,   # one-shot (applied instantly to all)
    },
    "crime_spree": {
        "name":  "🔓 Crime Spree",
        "color": discord.Color.dark_red(),
        "desc":  "Police are overwhelmed! Crime **catch chance** is halved for **30 minutes**.",
        "duration": 1800,
    },
}

# active_server_event = {"type": str, "ends_at": float, "msg_id": int, "channel_id": int}
active_server_event: dict = {}
_event_channel_id: int = int(os.getenv("EVENT_CHANNEL_ID", "0")) or None
_event_config_file = os.path.join(os.path.dirname(__file__), "event_config.txt")

SLOTS_SYMBOLS = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎", "7️⃣"]
SLOTS_WEIGHTS = [30, 25, 20, 15, 6, 3, 1]
SLOTS_PAYOUTS = {
    "🍒🍒🍒": 2, "🍋🍋🍋": 3, "🍊🍊🍊": 3,
    "🍇🍇🍇": 4, "⭐⭐⭐": 5, "💎💎💎": 10, "7️⃣7️⃣7️⃣": 18,
}

# Scratch Card
SCRATCH_SYMBOLS = ["🍒", "🍋", "💎", "7️⃣", "⭐", "🎰"]
SCRATCH_WEIGHTS = [35,   25,   15,   12,    8,    5  ]
SCRATCH_PRICE   = 200   # matches shop price

# Mines
MINES_MIN_BET   = 50
MINES_MINE_OPTS = [2, 3, 5, 8, 14]   # selectable mine counts
MINES_TOTAL     = 20                   # 4 rows × 5 tiles; row 4 = cash-out

CHICKEN_MIN_BET      = 50
CHICKEN_LANES        = 8
CHICKEN_MULTS        = [1.20, 1.50, 1.95, 2.55, 3.40, 4.80, 7.20, 11.50]
CHICKEN_CRASH_CHANCE = {"easy": 0.15, "medium": 0.28, "hard": 0.45}

# match 2 same → 2× payout ($400), match 3 same → 10× payout ($2000)
# Casino session limits (per player per 4 hours)
CASINO_BJ_LIMIT    = 25   # max blackjack hands
CASINO_SLOTS_LIMIT = 40   # max slots spins
CASINO_DAILY_LOSS  = 75_000  # max net loss per 24h across all casino games

rooms        = {}
menu_messages = {}  # uid -> discord.Message  (active !play panels)
user_context  = {}  # uid -> {"guild_id": int, "channel_id": int}

# ================================================================
# CARD LOGIC
# ================================================================
def make_deck():
    suits = ["♠", "♥", "♦", "♣"]
    ranks = list(range(2, 11)) + ["J", "Q", "K", "A"]
    deck  = [(r, s) for s in suits for r in ranks]
    random.shuffle(deck)
    return deck

def card_str(card) -> str:
    return f"{card[0]}{card[1]}"

def hand_value(hand) -> int:
    total, aces = 0, 0
    for rank, _ in hand:
        if rank in ("J", "Q", "K"):  total += 10
        elif rank == "A":             aces += 1; total += 11
        else:                         total += rank
    while total > 21 and aces:
        total -= 10; aces -= 1
    return total

def hand_str(hand, hide_second=False) -> str:
    if hide_second and len(hand) >= 2:
        return f"{card_str(hand[0])} 🂠"
    return " ".join(card_str(c) for c in hand)

def gen_room_code() -> str:
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if code not in rooms:
            return code

# ================================================================
# HELPERS
# ================================================================
def get_player(uid: str, name: str = None) -> dict:
    if uid not in players:
        players[uid] = {
            "money": STARTING_MONEY, "bank": 0, "level": 1, "xp": 0,
            "items": [], "stats": {"wins": 0, "losses": 0, "total_won": 0, "total_lost": 0, "jobs_done": 0, "crimes_done": 0},
            "flags": {}, "cooldowns": {}, "equipped": {"theme": None, "skin": None},
            "heat": 0, "streak": 0, "last_daily": 0,
            "energy": MAX_ENERGY, "last_energy_update": time.time(),
            "prestige": 0, "jailed_until": 0,
            "name": name or f"Player {uid[:6]}"
        }
    elif name:
        players[uid]["name"] = name
    p = players[uid]
    p.setdefault("heat", 0); p.setdefault("streak", 0); p.setdefault("last_daily", 0)
    p.setdefault("name", f"Player {uid[:6]}")
    p.setdefault("energy", MAX_ENERGY); p.setdefault("last_energy_update", time.time())
    p.setdefault("prestige", 0); p.setdefault("jailed_until", 0)
    p.setdefault("stats", {}).setdefault("jobs_done", 0)
    p["stats"].setdefault("crimes_done", 0)
    p.setdefault("gang_id", None)
    p.setdefault("casino_session",    {"bj": 0, "slots": 0, "reset_at": 0})
    p.setdefault("casino_daily_loss", {"amount": 0, "reset_at": 0})
    p.setdefault("job_streak",        {"key": None, "count": 0})
    p.setdefault("job_counts",        {})
    p.setdefault("job_promotions",    {})
    p.setdefault("achievements",      [])
    p.setdefault("bank_deposit_time", 0)
    p["stats"].setdefault("bounties_claimed", 0)
    p["stats"].setdefault("scratch_wins",     0)
    p["stats"].setdefault("heists_done",      0)
    p.setdefault("weekly",      {"reset_at": 0, "progress": {}, "claimed": False})
    p.setdefault("properties",  [])
    p["stats"].setdefault("properties_raided", 0)
    p.setdefault("career",   None)
    p.setdefault("training", {"steal": 0, "crime": 0, "jobs": 0, "xp": 0})
    for key in TRAINING_STATS:
        p["training"].setdefault(key, 0)
    return p

def add_xp(p: dict, amount: int) -> bool:
    xp_train = p.get("training", {}).get("xp", 0)
    if xp_train > 0:
        amount = int(amount * (1.0 + xp_train * TRAINING_STATS["xp"]["bonus"]))
    if p["flags"].get("xp_booster", 0) > 0:
        amount *= 2; p["flags"]["xp_booster"] -= 1
    if event_active("xp_boost"):
        amount *= 2
    p["xp"] += amount
    leveled = False
    while p["xp"] >= XP_PER_LEVEL:
        p["xp"] -= XP_PER_LEVEL; p["level"] += 1; leveled = True
    return leveled

def xp_bar(p: dict) -> str:
    filled = int((p["xp"] / XP_PER_LEVEL) * 10)
    return "█" * filled + "░" * (10 - filled)

def cd_remaining(p: dict, key: str, t: float) -> int:
    return max(0, int(t - (time.time() - p["cooldowns"].get(key, 0))))

def fmt_cd(secs: int) -> str:
    if secs <= 0: return "Ready ✅"
    m, s = divmod(secs, 60); h, m = divmod(m, 60)
    if h:   return f"{h}h {m}m"
    return f"{m}m {s}s" if m else f"{s}s"

def player_name(uid: str, guild: discord.Guild) -> str:
    if guild:
        m = guild.get_member(int(uid))
        if m:
            get_player(uid, m.display_name)
            return m.display_name
    return players.get(uid, {}).get("name", f"Player {uid[:6]}")

def _get_guild(interaction: discord.Interaction, uid: str) -> discord.Guild:
    if interaction.guild: return interaction.guild
    gid = user_context.get(uid, {}).get("guild_id")
    return bot.get_guild(gid) if gid else None

def _get_server_channel(uid: str):
    cid = user_context.get(uid, {}).get("channel_id")
    return bot.get_channel(cid) if cid else None

def player_icon(uid: str) -> str:
    p = get_player(uid)
    return "👑" if p["equipped"].get("skin") == "avatar_skin" else "🎴"

def heat_bar(heat: int) -> str:
    h = min(heat, 10)
    return "🔥" * h + "⬜" * (10 - h)

def heat_label(heat: int) -> str:
    if heat <= 2:  return "🟢 Low"
    if heat <= 5:  return "🟡 Medium"
    if heat <= 8:  return "🟠 High"
    return "🔴 Critical"

_HEAT_CATCH = [0.15, 0.15, 0.15, 0.28, 0.28, 0.45, 0.45, 0.60, 0.60, 0.72, 0.72]

def crime_catch_chance(heat: int, level: int = 1, prestige: int = 0) -> float:
    base           = _HEAT_CATCH[min(heat, 10)]
    level_bonus    = min(level * 0.004, 0.10)
    prestige_bonus = prestige * PRESTIGE_CRIME_BONUS
    return max(0.05, min(0.72, base - level_bonus - prestige_bonus))

def _crime_rank(p: dict) -> dict:
    done = p.get("stats", {}).get("crimes_done", 0)
    rank = CRIME_RANKS[0]
    for r in CRIME_RANKS:
        if done >= r["min"]: rank = r
    return rank

def _crime_streak_bonus(p: dict) -> float:
    streak = p.get("crime_streak", 0)
    return min(streak * 0.10, 0.50)  # +10% per success, max +50%

def _crime_menu_embed(p: dict) -> discord.Embed:
    rank   = _crime_rank(p)
    streak = p.get("crime_streak", 0)
    done   = p.get("stats", {}).get("crimes_done", 0)
    heat   = p.get("heat", 0)
    next_rank = next((r for r in CRIME_RANKS if r["min"] > done), None)
    streak_bonus = _crime_streak_bonus(p)

    embed = discord.Embed(
        title="🔫 Crime",
        description=(
            f"{rank['emoji']} **{rank['name']}** — {done} crimes committed\n"
            f"🌡️ Heat: `{heat_bar(heat)}` {heat_label(heat)}\n"
            f"🔥 Streak: **{streak}** (+{int(streak_bonus*100)}% haul bonus)"
            + (f" — next rank at **{next_rank['min']}** crimes" if next_rank else " — **MAX RANK**")
        ),
        color=discord.Color.dark_red()
    )
    t_perks = _territory_perks_for_player(p.get("uid_ref", ""))
    for key, ct in CRIME_TYPES.items():
        base_catch = ct["catch"]
        # apply rank, territory, luck adjustments for display
        adj_catch = max(0.05, base_catch - rank["bonus"] - (0.10 if "crime_10" in t_perks else 0))
        rmin, rmax = ct["reward"]
        bonus_rmax = int(rmax * (1 + streak_bonus + rank["bonus"]))
        embed.add_field(
            name=ct["name"],
            value=f"💰 ${rmin:,}–${bonus_rmax:,}  •  🎲 Catch: **{int(adj_catch*100)}%**  •  🌡️ +{ct['heat']} heat\n*{ct['desc']}*",
            inline=False
        )
    footer = "💵 Wallet: ${:,}  •  ⚡ Energy: {}/{}".format(p["money"], p.get("energy", 100), 100)
    if event_active("crime_spree"):
        footer += f"  •  🔓 Crime Spree active ({fmt_cd(event_time_left())} left)"
    embed.set_footer(text=footer)
    return embed

def get_energy(p: dict) -> int:
    now   = time.time()
    delta = int((now - p.get("last_energy_update", now)) / ENERGY_REGEN)
    if delta > 0:
        p["energy"] = min(MAX_ENERGY, p.get("energy", MAX_ENERGY) + delta)
        p["last_energy_update"] = now
    return p["energy"]

def use_energy(p: dict, action: str) -> bool:
    cost = ENERGY_COST.get(action, 0)
    get_energy(p)  # refresh first
    if p["energy"] < cost: return False
    p["energy"] -= cost; return True

def energy_bar(p: dict) -> str:
    e = get_energy(p); filled = int(e / 10)
    return "⚡" * filled + "▪️" * (10 - filled)

def is_jailed(p: dict) -> int:
    return max(0, int(p.get("jailed_until", 0) - time.time()))

# ── Bounty helpers ────────────────────────────────────────────────
BOUNTY_MIN      = 500
BOUNTY_TTL      = 86400   # 24 hours
BOUNTY_REFUND   = 0.50    # 50% back on manual removal

def _prune_bounty(target_uid: str):
    """Remove expired bounty and refund all contributors 50%."""
    b = bounties.get(target_uid)
    if not b: return
    if time.time() > b.get("placed_at", 0) + BOUNTY_TTL:
        for entry in b.get("entries", []):
            p = players.get(entry["by"])
            if p:
                p["money"] += int(entry["amount"] * BOUNTY_REFUND)
        bounties.pop(target_uid, None)
        delete_bounty_db(target_uid)
        save_data()

def _bounty_total(target_uid: str) -> int:
    _prune_bounty(target_uid)
    return bounties.get(target_uid, {}).get("total", 0)

def _bounty_embed_list() -> discord.Embed:
    # Prune all expired first
    for uid in list(bounties.keys()):
        _prune_bounty(uid)
    if not bounties:
        return discord.Embed(title="🎯 Active Bounties", description="No active bounties right now.", color=discord.Color.dark_gray())
    embed = discord.Embed(title="🎯 Active Bounty Board", color=discord.Color.red())
    sorted_b = sorted(bounties.items(), key=lambda x: x[1].get("total", 0), reverse=True)
    for i, (tid, b) in enumerate(sorted_b[:10], 1):
        p      = players.get(tid, {})
        name   = p.get("name", f"Player {tid[:6]}")
        total  = b.get("total", 0)
        n      = len(b.get("entries", []))
        left   = max(0, int(b.get("placed_at", 0) + BOUNTY_TTL - time.time()))
        embed.add_field(
            name=f"#{i}  {name}",
            value=f"💰 **${total:,}**  •  {n} contributor{'s' if n != 1 else ''}  •  ⏳ {fmt_cd(left)}",
            inline=False
        )
    embed.set_footer(text="Steal from a target with a bounty to claim it automatically!")
    return embed


# ── Server Event helpers ───────────────────────────────────────────
def _load_event_channel() -> int:
    global _event_channel_id
    try:
        if os.path.exists(_event_config_file):
            with open(_event_config_file) as f:
                _event_channel_id = int(f.read().strip())
    except Exception:
        pass
    return _event_channel_id

def _save_event_channel(cid: int):
    global _event_channel_id
    _event_channel_id = cid
    try:
        with open(_event_config_file, "w") as f:
            f.write(str(cid))
    except Exception:
        pass

def event_active(etype: str) -> bool:
    """Return True if an event of the given type is currently running."""
    ev = active_server_event
    if not ev or ev.get("type") != etype: return False
    dur = SERVER_EVENTS[etype]["duration"]
    if dur == 0: return False   # one-shot events don't persist
    return time.time() < ev.get("ends_at", 0)

def event_time_left() -> int:
    """Seconds remaining for the current active event (0 if none / expired)."""
    ev = active_server_event
    if not ev: return 0
    return max(0, int(ev.get("ends_at", 0) - time.time()))

def _event_embed(etype: str, money_amount: int = 0) -> discord.Embed:
    ev_cfg = SERVER_EVENTS[etype]
    embed  = discord.Embed(title=f"🌍 SERVER EVENT: {ev_cfg['name']}", color=ev_cfg["color"])
    embed.description = ev_cfg["desc"]
    if etype == "money_drop":
        embed.add_field(name="💵 Prize", value=f"**${money_amount:,}**", inline=True)
        embed.set_footer(text="⚡ First click wins — one per player!")
    elif ev_cfg["duration"] > 0:
        ends = int(time.time() + ev_cfg["duration"])
        embed.add_field(name="⏱️ Duration", value=f"Ends <t:{ends}:R>", inline=True)
        embed.set_footer(text="This event affects all players automatically.")
    else:
        embed.set_footer(text="Applied instantly to all active players.")
    return embed

def jail_duration(heat: int) -> int:
    """New formula: only jailable at heat>=8, shorter time."""
    if heat < 8: return 0
    return (heat - 7) * 75   # heat8→75s, heat9→150s, heat10→225s

def _casino_session(p: dict) -> dict:
    """Return session counters, resetting if 4-hour window expired."""
    now = time.time()
    cs  = p.setdefault("casino_session", {"bj": 0, "slots": 0, "reset_at": 0})
    if now > cs.get("reset_at", 0):
        cs["bj"] = 0; cs["slots"] = 0; cs["reset_at"] = now + 14400
    return cs

def _casino_daily_loss(p: dict) -> dict:
    """Return daily loss tracker, resetting after 24h."""
    now = time.time()
    dl  = p.setdefault("casino_daily_loss", {"amount": 0, "reset_at": 0})
    if now > dl.get("reset_at", 0):
        dl["amount"] = 0; dl["reset_at"] = now + 86400
    return dl

def _record_casino_loss(p: dict, amount: int):
    dl = _casino_daily_loss(p)
    dl["amount"] = dl.get("amount", 0) + amount

def _casino_loss_blocked(p: dict) -> bool:
    dl = _casino_daily_loss(p)
    return dl.get("amount", 0) >= CASINO_DAILY_LOSS

def job_fatigue_mult(p: dict, job_key: str) -> float:
    """Return pay multiplier based on consecutive same-job streak. Corporate halves penalty."""
    return _career_fatigue_mult(p, job_key)

def update_job_streak(p: dict, job_key: str):
    js = p.setdefault("job_streak", {"key": None, "count": 0})
    if js.get("key") == job_key:
        js["count"] = min(js.get("count", 0) + 1, 3)
    else:
        js["key"]   = job_key
        js["count"] = 1

def bank_interest_eligible(p: dict) -> int:
    """Return bank balance eligible for interest (deposited >=24h ago)."""
    age = time.time() - p.get("bank_deposit_time", 0)
    if age >= 86400:
        return p.get("bank", 0)
    return 0

def prestige_pay_bonus(p: dict) -> float:
    return 1.0 + p.get("prestige", 0) * PRESTIGE_JOB_BONUS

def _career(p: dict) -> str | None:
    return p.get("career")

def _career_job_pay_mult(p: dict) -> float:
    return 1.20 if _career(p) == "corporate" else 1.0

def _career_job_cd_mult(p: dict) -> float:
    return 0.70 if _career(p) == "hustler" else 1.0

def _career_crime_cd_mult(p: dict) -> float:
    return 0.75 if _career(p) == "hustler" else 1.0

def _career_heat_mult(p: dict) -> float:
    return 0.60 if _career(p) == "shadow" else 1.0

def _career_steal_bonus(p: dict) -> float:
    return 8.0 if _career(p) == "shadow" else 0.0

def _career_jail_mult(p: dict) -> float:
    return 0.50 if _career(p) == "shadow" else 1.0

def _career_bank_interest_bonus(p: dict) -> float:
    return 0.03 if _career(p) == "corporate" else 0.0

def _career_fatigue_mult(p: dict, job_key: str) -> float:
    """Corporate halves fatigue penalty."""
    js = p.setdefault("job_streak", {"key": None, "count": 0})
    if js.get("key") == job_key:
        count    = js.get("count", 0)
        penalty  = 0.05 if _career(p) == "corporate" else 0.10
        return max(0.70, 1.0 - count * penalty)
    return 1.0

def steal_chance(atk: dict, tgt: dict, mask: bool = False, gloves: bool = False, revenge: bool = False) -> float:
    train_bonus = atk.get("training", {}).get("steal", 0) * TRAINING_STATS["steal"]["bonus"] * 100
    val = (50
           + atk.get("level", 1) * 2
           - tgt.get("level", 1) * 1.5
           - atk.get("heat", 0) * 5
           + (10 if mask   else 0)
           + (8  if gloves else 0)
           + (20 if revenge else 0)
           - (20 if "insurance_shield" in tgt.get("items", []) else 0)
           + train_bonus)
    return max(5, min(95, val)) / 100

# ── Gang helpers ─────────────────────────────────────────────────
def _gang_level(g: dict) -> int:
    xp  = g.get("xp", 0); lvl = 1
    for i, (req, _, _) in enumerate(GANG_LEVEL_TABLE):
        if xp >= req: lvl = i + 1
    return lvl

def _gang_max_members(g: dict) -> int:
    return GANG_LEVEL_TABLE[_gang_level(g) - 1][1]

def _gang_perks(g: dict) -> list:
    return GANG_LEVEL_TABLE[_gang_level(g) - 1][2]

def _gang_xp_next(g: dict) -> int:
    lvl = _gang_level(g)
    return GANG_LEVEL_TABLE[lvl][0] if lvl < len(GANG_LEVEL_TABLE) else 0

def _gang_xp_bar(g: dict) -> str:
    lvl = _gang_level(g)
    if lvl >= len(GANG_LEVEL_TABLE): return "█" * 10
    cur  = GANG_LEVEL_TABLE[lvl - 1][0]; nxt = GANG_LEVEL_TABLE[lvl][0]
    filled = max(0, min(10, int(((g.get("xp", 0) - cur) / max(1, nxt - cur)) * 10)))
    return "█" * filled + "░" * (10 - filled)

def _find_player_gang(uid: str):
    gid = players.get(uid, {}).get("gang_id")
    if gid and gid in gangs: return gid, gangs[gid]
    return None, None

def _gang_color_obj(g: dict) -> discord.Color:
    return {"red": discord.Color.red(), "blue": discord.Color.blue(), "green": discord.Color.green(),
            "gold": discord.Color.gold(), "purple": discord.Color.purple(), "orange": discord.Color.orange()
            }.get(g.get("color", "red"), discord.Color.red())

def _territory_perks_for_player(uid: str) -> set:
    gid, _ = _find_player_gang(uid)
    if not gid: return set()
    result = set()
    for tid, td in territories.items():
        if td.get("owner_gid") == gid:
            result.update(TERRITORIES.get(tid, {}).get("perk_keys", []))
    return result

def _territory_perks_for_gang(gid: str) -> set:
    result = set()
    for tid, td in territories.items():
        if td.get("owner_gid") == gid:
            result.update(TERRITORIES.get(tid, {}).get("perk_keys", []))
    return result

def _gang_territory_count(gid: str) -> int:
    return sum(1 for td in territories.values() if td.get("owner_gid") == gid)

_INCOME_PERK_VALUES = {
    "work_10": 0.10, "work_20": 0.20, "work_25": 0.25,
    "income_15": 0.15, "income_25": 0.25, "income_30": 0.30,
    "income_40": 0.40, "income_50": 0.50, "income_75": 0.75,
}
_STEAL_PERK_VALUES = {
    "steal_10": 0.10, "steal_15": 0.15, "steal_20": 0.20,
    "steal_25": 0.25, "steal_30": 0.30,
}

def _gang_income_multiplier(uid: str) -> float:
    _, g = _find_player_gang(uid)
    if not g: return 1.0
    perks = _gang_perks(g)
    mult  = 1.0
    if any("+15%" in p for p in perks): mult = 1.15
    elif any("+5%" in p for p in perks): mult = 1.05
    t_perks = _territory_perks_for_player(uid)
    for key, val in _INCOME_PERK_VALUES.items():
        if key in t_perks: mult += val
    return mult

def _gang_steal_bonus(uid: str) -> float:
    _, g = _find_player_gang(uid)
    if not g: return 0.0
    bonus = 0.10 if any("+10% steal" in p for p in _gang_perks(g)) else 0.0
    t_perks = _territory_perks_for_player(uid)
    for key, val in _STEAL_PERK_VALUES.items():
        if key in t_perks: bonus += val
    return bonus

def add_gang_xp(g: dict, amount: int):
    g["xp"] = g.get("xp", 0) + amount

def _make_gang_embed(g: dict, gid: str, guild=None) -> discord.Embed:
    lvl     = _gang_level(g); perks = _gang_perks(g)
    next_xp = _gang_xp_next(g)
    xp_text = (f"`{_gang_xp_bar(g)}` **{g.get('xp',0):,}** / {next_xp:,} XP"
               if next_xp else f"`{'█'*10}` **MAX LEVEL**")
    members  = g.get("members", []); officers = g.get("officers", []); leader = g.get("leader", "")
    priv_icon = "🔓 Public" if g.get("privacy", "public") == "public" else "🔒 Private"
    reqs      = len(g.get("join_requests", []))
    req_str   = f"  •  📋 {reqs} request{'s' if reqs != 1 else ''}" if reqs else ""
    embed = discord.Embed(
        title=f"{g.get('emoji','🏴')} **{g['name']}**  `{g.get('tag','')}`",
        description=f"Level **{lvl}** Gang  •  {priv_icon}{req_str}  •  Founded <t:{int(g.get('created_at', time.time()))}:R>",
        color=_gang_color_obj(g)
    )
    embed.add_field(name="💰 Treasury", value=f"**${g.get('bank', 0):,}**",           inline=True)
    embed.add_field(name="👥 Members",  value=f"**{len(members)}/{_gang_max_members(g)}**", inline=True)
    embed.add_field(name="⭐ Gang XP",  value=xp_text,                                 inline=False)
    roster = []
    for uid in members:
        name_ = player_name(uid, guild) if guild else players.get(uid, {}).get("name", f"Player {uid[:4]}")
        p_    = players.get(uid, {})
        role_ = "👑" if uid == leader else ("⭐" if uid in officers else "👥")
        roster.append(f"{role_} **{name_}** — Lv.{p_.get('level', 1)} | ${p_.get('money',0):,}")
    embed.add_field(name="🏅 Roster",       value="\n".join(roster) or "*Empty*", inline=False)
    if perks:
        embed.add_field(name="⚡ Active Perks", value="\n".join(perks),          inline=False)
    if g.get("at_war_with"):
        eid = g["at_war_with"]; eg = gangs.get(eid, {}); wend = g.get("war_ends_at", 0)
        embed.add_field(name="⚔️ ACTIVE WAR",
            value=f"vs **{eg.get('emoji','🏴')} {eg.get('name','Unknown')}**\n"
                  f"Ends <t:{int(wend)}:R>  •  **{g.get('war_wins',0)}W — {g.get('war_losses',0)}L**",
            inline=False)
    embed.set_footer(text=f"Gang ID: {gid}  •  /gang commands to manage")
    return embed

async def _resolve_war(gid: str):
    g = gangs.get(gid)
    if not g or not g.get("at_war_with"): return None, 0
    egid = g["at_war_with"]; eg = gangs.get(egid, {})
    winner_gid, loser_gid = (gid, egid) if g.get("war_wins",0) >= eg.get("war_wins",0) else (egid, gid)
    wg = gangs[winner_gid]; lg = gangs[loser_gid]
    stake = int(lg.get("bank", 0) * GANG_WAR_STAKE)
    lg["bank"] = max(0, lg.get("bank", 0) - stake); wg["bank"] = wg.get("bank", 0) + stake
    add_gang_xp(wg, 500)
    for g_ in [wg, lg]:
        g_["at_war_with"] = None; g_["war_ends_at"] = 0; g_["war_wins"] = 0; g_["war_losses"] = 0
    save_gangs()
    return winner_gid, stake

# ── Territory helpers ─────────────────────────────────────────────
def _territory_color(gid) -> discord.Color:
    g = gangs.get(gid, {}) if gid else None
    return _gang_color_obj(g) if g else discord.Color.dark_gray()

def _territory_embed(tid: str, td: dict, guild=None) -> discord.Embed:
    tinfo     = TERRITORIES.get(tid, {})
    rarity    = tinfo.get("rarity", "common")
    r_emoji   = TERRITORY_RARITY_EMOJI.get(rarity, "⬜")
    owner_gid = td.get("owner_gid")
    og        = gangs.get(owner_gid, {}) if owner_gid else None
    owner_str = f"{og.get('emoji','')} **{og['name']}**" if og else "*Unclaimed* 🚩"
    embed = discord.Embed(
        title=f"{tinfo.get('emoji','')} {tinfo.get('name', tid)}  {r_emoji} {rarity.capitalize()}",
        description=tinfo.get("desc", ""),
        color=_territory_color(owner_gid)
    )
    embed.add_field(name="👑 Owner",    value=owner_str, inline=True)
    embed.add_field(name="💰 Income",   value=f"${tinfo['income'][0]:,}–${tinfo['income'][1]:,} / 6h", inline=True)
    embed.add_field(name="🌡️ Heat",     value=f"+{TERRITORY_HEAT.get(rarity, 1)} to gang members", inline=True)
    embed.add_field(name="🛡️ Defense",  value=f"**{tinfo.get('base_def', 60)} pts** base", inline=True)
    embed.add_field(name="⚡ Perks",    value="\n".join(f"• {p}" for p in tinfo.get("perks", [])) or "None", inline=False)
    expires = td.get("expires_at", 0)
    if owner_gid and expires:
        embed.add_field(name="⏳ Expires", value=f"<t:{int(expires)}:R>", inline=True)
    b = td.get("battle")
    if b:
        ag  = gangs.get(b["attacker_gid"], {})
        embed.add_field(name="⚔️ BATTLE IN PROGRESS",
            value=f"**{ag.get('emoji','')} {ag.get('name','?')}** is attacking!\n"
                  f"⚔️ Atk: **{b['atk_pts']}** pts  •  🛡️ Def: **{b['def_pts']}** pts\n"
                  f"Ends <t:{int(b['ends_at'])}:R>",
            inline=False)
    footer_parts = []
    if owner_gid and td.get("last_income"): footer_parts.append(f"Last income: <t:{int(td['last_income'])}:R>")
    if footer_parts: embed.set_footer(text="  •  ".join(footer_parts))
    return embed

def _all_territories_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🗺️ Territory Map",
        description="Open a **🎁 Chest** to claim a territory. One per gang, 12–24h duration. Other gangs can attack!",
        color=discord.Color.dark_gold()
    )
    for tid, tinfo in TERRITORIES.items():
        td      = territories.get(tid, {})
        owner   = td.get("owner_gid")
        og      = gangs.get(owner, {}) if owner else None
        o_str   = f"{og.get('emoji','')} {og['name']}" if og else "*Unclaimed*"
        battle  = " ⚔️" if td.get("battle") else ""
        expires = td.get("expires_at", 0)
        exp_str = f"\n⏳ <t:{int(expires)}:R>" if owner and expires else ""
        r_emoji = TERRITORY_RARITY_EMOJI.get(tinfo.get("rarity", "common"), "⬜")
        embed.add_field(
            name=f"{tinfo['emoji']} {tinfo['name']}  {r_emoji}{battle}",
            value=f"**{o_str}**{exp_str}\n💰 ${tinfo['income'][0]:,}–${tinfo['income'][1]:,}/6h\n" +
                  "\n".join(f"• {p}" for p in tinfo["perks"]),
            inline=True
        )
    return embed

async def _resolve_battle(tid: str) -> tuple:
    td = territories.get(tid)
    if not td or not td.get("battle"): return None, 0
    b       = td["battle"]
    atk_gid = b["attacker_gid"]; def_gid = td.get("owner_gid")
    atk_g   = gangs.get(atk_gid, {}); def_g = gangs.get(def_gid, {}) if def_gid else None
    attacker_wins = b["atk_pts"] > b["def_pts"]
    if attacker_wins:
        territories[tid]["owner_gid"] = atk_gid
        add_gang_xp(atk_g, 300)
        if def_g: add_gang_xp(def_g, 50)
        winner_gid = atk_gid
    else:
        if def_g: add_gang_xp(def_g, 150)
        add_gang_xp(atk_g, 30)
        winner_gid = def_gid
    territories[tid]["battle"] = None; territories[tid]["last_attacked"] = time.time()
    save_territory(tid); save_gangs()
    return winner_gid, attacker_wins

def init_territories():
    loaded = load_territories()
    for tid in TERRITORIES:
        if tid not in loaded:
            loaded[tid] = {"owner_gid": None, "battle": None, "last_income": 0, "last_attacked": 0, "expires_at": 0}
            save_territory(tid)
        else:
            loaded[tid].setdefault("expires_at", 0)
    territories.update(loaded)

init_territories()

def get_theme_color(p: dict) -> discord.Color:
    key = p.get("equipped", {}).get("theme") or "default"
    key = key.replace("theme_", "")
    t = THEMES.get(key, THEMES["default"])
    return discord.Color.from_rgb(*t["color"])

def owned_themes(p: dict) -> list:
    return ["default"] + [i.replace("theme_", "") for i in p.get("items", []) if i.startswith("theme_") and i in SHOP]

def spin_slots() -> list:
    return random.choices(SLOTS_SYMBOLS, weights=SLOTS_WEIGHTS, k=3)

def _slots_result(bet: int, uid: str) -> discord.Embed | None:
    global jackpot_pool
    p  = get_player(uid)
    cs = _casino_session(p)
    if cs["slots"] >= CASINO_SLOTS_LIMIT:
        return None   # caller checks for None and sends limit message
    if _casino_loss_blocked(p):
        return None
    cs["slots"] += 1
    jackpot_pool += max(1, bet // 20)
    p["money"] -= bet

    reels = spin_slots()
    key   = "".join(reels)
    mult  = SLOTS_PAYOUTS.get(key, 0)

    if "lucky_spin" in p["items"] and mult > 0:
        p["items"].remove("lucky_spin"); mult = int(mult * 1.5)

    if mult == 0 and reels.count("🍒") >= 2:
        winnings = int(bet * 1.5); mult_str = "1.5x 🍒🍒"
    elif mult > 0:
        winnings = bet * mult; mult_str = f"{mult}x"
    else:
        winnings = 0; mult_str = "No match"

    jackpot_hit = random.random() < 0.005
    jp_won = 0
    if jackpot_hit:
        jp_won = jackpot_pool; winnings += jp_won; jackpot_pool = JACKPOT_SEED

    if winnings > 0:
        p["money"] += winnings
        p["stats"]["wins"]      += 1
        p["stats"]["total_won"] += winnings - bet
        add_xp(p, 15)
        color = discord.Color.gold() if jackpot_hit else discord.Color.green()
    else:
        p["stats"]["losses"]     += 1
        p["stats"]["total_lost"] += bet
        _record_casino_loss(p, bet)
        add_xp(p, 5)
        color = discord.Color.red()

    save_data()
    reel_str = " | ".join(reels)
    embed = discord.Embed(title="🎰 JACKPOT!! 🎰" if jackpot_hit else "🎰 Slots", color=color)
    embed.add_field(name="Reels",  value=f"╔══════════╗\n║ {reel_str} ║\n╚══════════╝", inline=False)
    if jackpot_hit:
        embed.add_field(name="🏆 JACKPOT!", value=f"+**${jp_won:,}**!", inline=False)
    if winnings > 0:
        embed.add_field(name="Result", value=f"✅ **{mult_str}** — +**${winnings:,}**", inline=True)
    else:
        embed.add_field(name="Result", value=f"❌ {mult_str} — -**${bet:,}**", inline=True)
    embed.add_field(name="Wallet", value=f"**${p['money']:,}**", inline=True)
    embed.set_footer(text=f"🎰 Jackpot pool: ${jackpot_pool:,}  •  7️⃣7️⃣7️⃣ pays 25x!")
    return embed

# ================================================================
# EMBEDS
# ================================================================
def _rank_title(level: int) -> str:
    if level >= 30: return "💎 Diamond"
    if level >= 20: return "🥇 Gold"
    if level >= 10: return "🥈 Silver"
    if level >= 5:  return "🥉 Bronze"
    return "🃏 Rookie"

def _rank_color(level: int) -> discord.Color:
    if level >= 30: return discord.Color.from_rgb(0, 220, 255)
    if level >= 20: return discord.Color.gold()
    if level >= 10: return discord.Color.from_rgb(192, 192, 192)
    if level >= 5:  return discord.Color.from_rgb(205, 127, 50)
    return discord.Color.dark_green()

def private_profile_embed(user: discord.Member) -> discord.Embed:
    uid  = str(user.id)
    p    = get_player(uid)
    s    = p["stats"]
    total   = s["wins"] + s["losses"]
    wr      = f"{s['wins']/total*100:.1f}%" if total else "—"
    net     = s["total_won"] - s["total_lost"]
    net_str = f"+${net:,}" if net >= 0 else f"-${abs(net):,}"
    worth   = p["money"] + p["bank"]

    embed = discord.Embed(
        title=f"{'👑' if p['equipped'].get('skin') else '🎴'}  {user.display_name}",
        description=f"{_rank_title(p['level'])}  •  Level **{p['level']}**  •  Net worth **${worth:,}**",
        color=get_theme_color(p)
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name="━━━  💵 Economy  ━━━",
        value=f"```\nWallet  ${p['money']:>10,}\nBank    ${p['bank']:>10,}\nTotal   ${worth:>10,}\n```",
        inline=False)
    xp_boost = p["flags"].get("xp_booster", 0)
    prestige = p.get("prestige", 0)
    prestige_str = f"  •  {'⭐' * min(prestige,5)} Prestige **{prestige}**" if prestige else ""
    embed.add_field(name="━━━  📊 Progress  ━━━",
        value=(f"XP  `{xp_bar(p)}`  `{p['xp']}/{XP_PER_LEVEL}`{prestige_str}\n"
               + (f"🚀 XP Boost: **{xp_boost} rounds left**\n" if xp_boost else "")
               + (f"🔥 Streak: **{p.get('streak',0)} days**" if p.get("streak", 0) > 1 else "")),
        inline=False)
    eng = get_energy(p)
    embed.add_field(name="━━━  ⚡ Energy  ━━━",
        value=f"`{energy_bar(p)}`  **{eng}/{MAX_ENERGY}**  *(+1 every 3 min)*", inline=False)
    jail_rem = is_jailed(p)
    if jail_rem: embed.add_field(name="⛓️ JAILED", value=f"Released in `{fmt_cd(jail_rem)}`", inline=False)
    embed.add_field(name="━━━  🃏 Stats  ━━━",
        value=f"```\nWins      {s['wins']:>6}\nLosses    {s['losses']:>6}\nWin Rate  {wr:>6}\nNet P&L   {net_str:>8}\nJobs Done {s.get('jobs_done',0):>6}\nCrimes    {s.get('crimes_done',0):>6}\n```",
        inline=False)
    embed.add_field(name="━━━  🌡️ Heat  ━━━",
        value=f"`{heat_bar(p['heat'])}` {heat_label(p['heat'])}  ({p['heat']}/10)",
        inline=False)
    items_str = "  ".join(f"{SHOP[i]['emoji']} {i}" for i in p["items"] if i in SHOP) if p["items"] else "*Empty*"
    embed.add_field(name="━━━  🎒 Inventory  ━━━", value=items_str, inline=False)
    embed.add_field(name="🎨 Theme", value=f"🎨 {p['equipped']['theme']}" if p["equipped"].get("theme") else "🎨 Default", inline=True)
    embed.add_field(name="🎭 Skin",  value=f"🎭 {p['equipped']['skin']}"  if p["equipped"].get("skin")  else "🎭 None",    inline=True)
    gid, g = _find_player_gang(uid)
    if gid and g:
        owned_tids = [tid for tid, td in territories.items() if td.get("owner_gid") == gid]
        t_perks    = _territory_perks_for_player(uid)
        perk_map   = {pk: desc for tid in owned_tids for pk, desc in zip(
                          TERRITORIES.get(tid, {}).get("perk_keys", []),
                          TERRITORIES.get(tid, {}).get("perks", []))}
        if owned_tids:
            zones_str = "\n".join(
                f"{TERRITORIES[t]['emoji']} **{TERRITORIES[t]['name']}**  •  "
                + "  ".join(f"`{pk}`" for pk in TERRITORIES[t].get("perk_keys", []))
                for t in owned_tids
            )
            perks_str = "\n".join(f"• {desc}" for desc in perk_map.values()) or "*None*"
            embed.add_field(
                name="━━━  🗺️ Territory Perks  ━━━",
                value=f"{zones_str}\n\n⚡ **Active bonuses:**\n{perks_str}",
                inline=False
            )
    embed.set_footer(text="🔒 Only visible to you  •  Use /play to start")
    return embed

def public_summary_line(uid: str, guild: discord.Guild) -> str:
    p    = get_player(uid)
    name = player_name(uid, guild)
    return f"{player_icon(uid)} **{name}** | ⭐ Lv.{p['level']}"

def room_embed(code: str, guild: discord.Guild) -> discord.Embed:
    room = rooms[code]
    state_label = {"waiting": "⏳ Waiting", "betting": "💰 Betting", "playing": "🃏 In Progress", "finished": "✅ Done"}.get(room["state"], room["state"])
    embed = discord.Embed(
        title=f"🎰 Blackjack Room  `{code}`",
        description=f"**Host:** {player_name(room['host'], guild)}   **Status:** {state_label}",
        color=discord.Color.dark_green()
    )
    seats = "\n".join(f"🪑 {i+1}: {public_summary_line(uid, guild)}" for i, uid in enumerate(room["players"])) or "*Empty*"
    embed.add_field(name=f"👥 Players ({len(room['players'])}/6)", value=seats, inline=False)

    if room["state"] in ("playing", "finished") and room.get("dealer_hand"):
        hide  = room["state"] == "playing"
        d_str = f"`{hand_str(room['dealer_hand'], hide_second=hide)}`"
        d_val = f" = **{hand_value(room['dealer_hand'])}**" if not hide else ""
        embed.add_field(name="🏠 Dealer", value=d_str + d_val, inline=False)

    if room["state"] in ("playing", "finished", "betting") and room.get("hands"):
        SI = {"standing": "✋", "bust": "💥", "blackjack": "🌟", "waiting": "⏳", "won": "🏆", "lost": "❌", "push": "🤝"}
        lines = []
        for uid in room["players"]:
            hand = room["hands"].get(uid, [])
            if not hand: continue
            bet    = room["bets"].get(uid, 0)
            status = room.get("player_status", {}).get(uid, "waiting")
            is_cur = (room.get("current_idx") is not None and len(room["players"]) > room["current_idx"]
                      and room["players"][room["current_idx"]] == uid)
            lines.append(f"{'➡️ ' if is_cur else ''}{SI.get(status,'🎯')} **{player_name(uid,guild)}**: `{hand_str(hand)}` = **{hand_value(hand)}** | ${bet:,}")
        if lines:
            embed.add_field(name="🃏 Hands", value="\n".join(lines), inline=False)

    embed.set_footer(text=f"Join: !join {code}  |  Max 6 players")
    return embed

def leaderboard_embed(guild: discord.Guild, category: str = "rich") -> discord.Embed:
    medals = ["🥇", "🥈", "🥉"] + ["🔹"] * 7
    if category == "rich":
        top   = sorted(players.items(), key=lambda x: x[1].get("money",0)+x[1].get("bank",0), reverse=True)[:10]
        lines = [f"{medals[i]} **{player_name(uid,guild)}** — **${d.get('money',0)+d.get('bank',0):,}**  *(Lv.{d.get('level',1)})*"
                 for i, (uid,d) in enumerate(top)]
        title = "💰 Leaderboard — Richest Players"
    elif category == "wanted":
        top   = sorted(players.items(), key=lambda x: x[1].get("heat",0), reverse=True)[:10]
        lines = [f"{medals[i]} **{player_name(uid,guild)}** — {heat_bar(d.get('heat',0))} **{d.get('heat',0)}/10**  *({heat_label(d.get('heat',0))})*"
                 for i, (uid,d) in enumerate(top) if d.get("heat",0) > 0]
        title = "🌡️ Leaderboard — Most Wanted"
    elif category == "gambler":
        top   = sorted(players.items(), key=lambda x: x[1].get("stats",{}).get("total_won",0), reverse=True)[:10]
        lines = [f"{medals[i]} **{player_name(uid,guild)}** — Won **${d.get('stats',{}).get('total_won',0):,}**  *({d.get('stats',{}).get('wins',0)}W / {d.get('stats',{}).get('losses',0)}L)*"
                 for i, (uid,d) in enumerate(top)]
        title = "🃏 Leaderboard — Best Gamblers"
    elif category == "worker":
        top   = sorted(players.items(), key=lambda x: x[1].get("stats",{}).get("jobs_done",0), reverse=True)[:10]
        lines = [f"{medals[i]} **{player_name(uid,guild)}** — **{d.get('stats',{}).get('jobs_done',0)}** jobs  *(Crimes: {d.get('stats',{}).get('crimes_done',0)})*"
                 for i, (uid,d) in enumerate(top)]
        title = "💼 Leaderboard — Most Active Workers"
    else:
        lines = []; title = "🏆 Leaderboard"
    return discord.Embed(title=title, description="\n".join(lines) or "No data yet.", color=discord.Color.gold())

class LeaderboardView(View):
    def __init__(self, guild, uid):
        super().__init__(timeout=60)
        self.guild = guild; self.uid = uid
        select = discord.ui.Select(placeholder="Choose leaderboard...", options=[
            discord.SelectOption(label="💰 Richest",       value="rich",    emoji="💰"),
            discord.SelectOption(label="🌡️ Most Wanted",   value="wanted",  emoji="🌡️"),
            discord.SelectOption(label="🃏 Best Gamblers",  value="gambler", emoji="🃏"),
            discord.SelectOption(label="💼 Most Active",   value="worker",  emoji="💼"),
        ])
        async def cb(inter):
            await inter.response.edit_message(embed=leaderboard_embed(self.guild, select.values[0]), view=self)
        select.callback = cb; self.add_item(select)

def _main_menu_embed(user: discord.Member, p: dict) -> discord.Embed:
    worth = p["money"] + p["bank"]
    total = p["stats"]["wins"] + p["stats"]["losses"]
    wr    = f"{p['stats']['wins']/total*100:.1f}%" if total else "—"
    embed = discord.Embed(
        title=f"🎰  Welcome back, {user.display_name}",
        description=(f"{_rank_title(p['level'])}  •  Level **{p['level']}**  •  Net worth **${worth:,}**\n"
                     f"```\nWallet  ${p['money']:>10,}\nBank    ${p['bank']:>10,}\n```"),
        color=get_theme_color(p)
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    gid_m, g_m = _find_player_gang(str(user.id))
    gang_line = (f"🏴 **Gang** — {g_m.get('emoji','')} {g_m['name']} (Lv.{_gang_level(g_m)})"
                 if g_m else "🏴 **Gang** — Not in a gang")
    embed.add_field(name="━━━  🎮 Main Menu  ━━━",
        value=("🃏 **Casino** — Blackjack, Slots, Coin Flip, Multiplayer\n"
               "💼 **Economy** — Work, Crime, Bank, Shop\n"
               f"{gang_line}\n"
               "🦹 **Heist** — Steal, Scan, Revenge, Duel\n"
               "👤 **Profile** — Full stats & inventory\n"
               "🏆 **Leaderboard** — Top 10 richest players"),
        inline=False)
    embed.add_field(name="━━━  📊 Quick Stats  ━━━",
        value=f"```\nGames    {total:>6}\nWin Rate {wr:>8}\nHeat     {heat_label(p.get('heat',0)):>10}\n```",
        inline=False)
    t_key = (p.get("equipped", {}).get("theme") or "default").replace("theme_", "")
    t = THEMES.get(t_key, THEMES["default"])
    career = p.get("career")
    career_txt = (f"{CAREER_PATHS[career]['emoji']} {CAREER_PATHS[career]['name']}" if career
                  else ("🔒 Career (Lv10)" if p.get("level",1) < CAREER_UNLOCK_LEVEL else "🎯 Career — pick one! /career"))
    embed.set_footer(text=f"XP  {xp_bar(p)}  {p['xp']}/{XP_PER_LEVEL}  •  {career_txt}  •  /help")
    return embed

def _shop_embed(p: dict) -> discord.Embed:
    embed = discord.Embed(title="🛒 Item Shop", description=f"Balance: **${p['money']:,}**\n*Only visible to you*", color=discord.Color.gold())
    for name, info in SHOP.items():
        embed.add_field(name=f"{info['emoji']} {name}  —  ${info['price']:,}", value=f"{info['desc']}  *[{info['type']}]*", inline=False)
    return embed

# ================================================================
# GAME LOGIC
# ================================================================
def deal_initial(room: dict):
    deck = room["deck"]
    for uid in room["players"]:
        room["hands"][uid] = [deck.pop(), deck.pop()]
    room["dealer_hand"] = [deck.pop(), deck.pop()]

def resolve_round(room: dict) -> dict:
    while hand_value(room["dealer_hand"]) < 17:
        room["dealer_hand"].append(room["deck"].pop())
    dealer_val  = hand_value(room["dealer_hand"])
    dealer_bust = dealer_val > 21
    results     = {}

    for uid in room["players"]:
        p      = get_player(uid)
        hand   = room["hands"].get(uid, [])
        bet    = room["bets"].get(uid, 0)
        val    = hand_value(hand)
        status = room.get("player_status", {}).get(uid, "")
        used   = room.get("used_items", {}).get(uid, [])

        if len(hand) == 2 and val == 21:
            win = int(bet * 1.5); p["money"] += bet + win
            p["stats"]["wins"] += 1; p["stats"]["total_won"] += win; add_xp(p, 30)
            results[uid] = ("blackjack", win); room["player_status"][uid] = "blackjack"; continue

        if status == "bust":
            if "insurance_shield" in used:
                p["money"] += bet // 2; loss = bet - (bet // 2)
            else:
                loss = bet
            p["stats"]["losses"] += 1; p["stats"]["total_lost"] += loss; add_xp(p, 5)
            results[uid] = ("bust", -loss); room["player_status"][uid] = "lost"; continue

        margin = val - dealer_val
        if not dealer_bust and margin < 0 and abs(margin) <= 2 and "luck_charm" in used:
            p["money"] += bet; results[uid] = ("push", 0); room["player_status"][uid] = "push"; add_xp(p, 10); continue

        if dealer_bust or val > dealer_val:
            multiplier = 2 if "double_boost" in used else 1
            win = bet * multiplier; p["money"] += bet + win
            p["stats"]["wins"] += 1; p["stats"]["total_won"] += win; add_xp(p, 20)
            results[uid] = ("win", win); room["player_status"][uid] = "won"
        elif val == dealer_val:
            p["money"] += bet; results[uid] = ("push", 0); room["player_status"][uid] = "push"; add_xp(p, 10)
        else:
            p["stats"]["losses"] += 1; p["stats"]["total_lost"] += bet
            results[uid] = ("lose", -bet); room["player_status"][uid] = "lost"; add_xp(p, 5)

    save_data()
    return results

# ================================================================
# MODALS
# ================================================================
class BetModal(Modal, title="💰 Place Your Bet"):
    amount = TextInput(label="Bet Amount", placeholder="Min $10", required=True)
    def __init__(self, user_id: str, room_code: str):
        super().__init__(); self.user_id = user_id; self.room_code = room_code

    async def on_submit(self, interaction: discord.Interaction):
        p = get_player(self.user_id); raw = self.amount.value.strip(); room = rooms.get(self.room_code)
        if not raw.isdigit():
            return await interaction.response.send_message("❌ Enter a valid number.", ephemeral=True)
        amt = int(raw)
        if amt < 10:    return await interaction.response.send_message("❌ Min bet is **$10**.", ephemeral=True)
        if amt > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
        if not room or room["state"] != "betting": return await interaction.response.send_message("❌ Betting closed.", ephemeral=True)
        if self.user_id in room.get("bets", {}): return await interaction.response.send_message("✅ Bet already placed!", ephemeral=True)
        p["money"] -= amt; room["bets"][self.user_id] = amt; save_data()
        await interaction.response.send_message(f"✅ Bet placed: **${amt:,}**", ephemeral=True)
        if all(uid in room["bets"] for uid in room["players"]):
            await _start_playing(interaction, self.room_code)
        else:
            remaining = [uid for uid in room["players"] if uid not in room["bets"]]
            await interaction.channel.send(f"⏳ Waiting: {' '.join(f'<@{u}>' for u in remaining)}", delete_after=10)
            await _update_room_msg(interaction, self.room_code)

class DepositModal(Modal, title="🏦 Deposit to Bank"):
    amount = TextInput(label="Amount", placeholder="Number or 'all'", required=True)
    def __init__(self, user_id): super().__init__(); self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        p = get_player(self.user_id); raw = self.amount.value.strip().lower()
        amt = p["money"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if amt <= 0 or amt > p["money"]: return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        p["money"] -= amt; p["bank"] += amt
        p["bank_deposit_time"] = time.time()   # track for 24h interest lock
        save_data()
        await interaction.response.send_message(
            f"✅ Deposited **${amt:,}** to bank.\n"
            f"💡 Interest applies after **24 hours** in the bank.", ephemeral=True)

WITHDRAW_FEE_RATE = 0.05  # 5% withdrawal fee

class WithdrawModal(Modal, title="💸 Withdraw from Bank"):
    amount = TextInput(label="Amount", placeholder="Number or 'all'", required=True)
    def __init__(self, user_id): super().__init__(); self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        p   = get_player(self.user_id); raw = self.amount.value.strip().lower()
        amt = p["bank"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if amt <= 0 or amt > p["bank"]:
            return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        fee      = max(1, int(amt * WITHDRAW_FEE_RATE))
        received = amt - fee
        p["bank"] -= amt; p["money"] += received; save_data()
        await interaction.response.send_message(
            f"✅ Withdrew **${amt:,}** — **${fee:,}** bank fee (5%) = **+${received:,}** to wallet.",
            ephemeral=True)

class SoloBetModal(Modal, title="🃏 Solo Blackjack — Place Bet"):
    amount = TextInput(label="Bet Amount", placeholder="Min $10, or type 'all'", required=True)
    def __init__(self, user_id, channel_id): super().__init__(); self.user_id = user_id; self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction):
        p = get_player(self.user_id, interaction.user.display_name); raw = self.amount.value.strip().lower()
        bet = p["money"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if bet <= 0:        return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        if bet < 10:        return await interaction.response.send_message("❌ Minimum bet is **$10**.", ephemeral=True)
        if bet > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
        if _casino_loss_blocked(p):
            return await interaction.response.send_message("🚫 Daily casino loss limit ($75,000) reached. Resets in 24h.", ephemeral=True)
        cs = _casino_session(p)
        if cs["bj"] >= CASINO_BJ_LIMIT:
            rem = int(cs["reset_at"] - time.time())
            return await interaction.response.send_message(f"🚫 Blackjack limit ({CASINO_BJ_LIMIT} hands/4h). Resets in `{fmt_cd(rem)}`.", ephemeral=True)
        cs["bj"] += 1
        p["money"] -= bet; save_data()
        deck = make_deck(); ph = [deck.pop(), deck.pop()]; dh = [deck.pop(), deck.pop()]
        view = SoloBlackjackView(self.user_id, bet, ph, dh, deck)
        await interaction.response.send_message(embed=view.build_embed(interaction.user), view=view)

class GambleModal(Modal, title="🎲 Coin Flip"):
    amount = TextInput(label="Bet Amount", placeholder="Min $10, or type 'all'", required=True)
    def __init__(self, user_id): super().__init__(); self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        p = get_player(self.user_id, interaction.user.display_name); raw = self.amount.value.strip().lower()
        bet = p["money"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if bet <= 0:        return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        if bet < 10:        return await interaction.response.send_message("❌ Minimum bet is **$10**.", ephemeral=True)
        if bet > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
        win = random.random() < 0.5
        if win:
            p["money"] += bet; p["stats"]["wins"] += 1; p["stats"]["total_won"] += bet; add_xp(p, 10)
            result_str = f"🟢 **YOU WIN!**  +**${bet:,}**"; color = discord.Color.green()
        else:
            p["money"] -= bet; p["stats"]["losses"] += 1; p["stats"]["total_lost"] += bet; add_xp(p, 5)
            result_str = f"🔴 **YOU LOSE!**  -**${bet:,}**"; color = discord.Color.red()
        save_data()
        embed = discord.Embed(title="🎲 Coin Flip", description="🪙 Heads — Win!" if win else "🪙 Tails — Lose!", color=color)
        embed.add_field(name="Result", value=result_str, inline=False)
        embed.add_field(name="Wallet", value=f"**${p['money']:,}**", inline=True)
        embed.set_footer(text="50/50 chance • Use !play to play again")
        await interaction.response.send_message(embed=embed, ephemeral=True)

class SlotsModal(Modal, title="🎰 Slots — Place Bet"):
    amount = TextInput(label="Bet Amount", placeholder="Min $10, or type 'all'", required=True)
    def __init__(self, user_id): super().__init__(); self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        p = get_player(self.user_id, interaction.user.display_name); raw = self.amount.value.strip().lower()
        bet = p["money"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if bet <= 0:        return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        if bet < 10:        return await interaction.response.send_message("❌ Minimum bet is **$10**.", ephemeral=True)
        if bet > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
        p2 = get_player(self.user_id)
        if _casino_loss_blocked(p2):
            return await interaction.response.send_message("🚫 Daily casino loss limit ($75,000) reached. Resets in 24h.", ephemeral=True)
        cs = _casino_session(p2)
        if cs["slots"] >= CASINO_SLOTS_LIMIT:
            rem = int(cs["reset_at"] - time.time())
            return await interaction.response.send_message(f"🚫 Slots limit ({CASINO_SLOTS_LIMIT} spins/4h). Resets in `{fmt_cd(rem)}`.", ephemeral=True)
        embed = _slots_result(bet, self.user_id)
        if embed is None:
            return await interaction.response.send_message("🚫 Casino limit reached.", ephemeral=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

# ================================================================
# ROOM HELPERS
# ================================================================
async def _update_room_msg(interaction, code):
    try:
        embed = room_embed(code, interaction.guild)
        if interaction.message:
            await interaction.message.edit(embed=embed)
        else:
            mid = rooms.get(code, {}).get("message_id")
            if mid:
                msg = await interaction.channel.fetch_message(mid)
                await msg.edit(embed=embed)
    except Exception as e:
        log.error(f"_update_room_msg: {e}")

async def _start_playing(interaction, code):
    room = rooms[code]
    room.update(state="playing", deck=make_deck(), hands={},
                player_status={uid: "waiting" for uid in room["players"]},
                used_items={}, current_idx=0)
    deal_initial(room)
    first = room["players"][0]
    view  = GamePlayView(code, first)
    try:
        if interaction.message:
            await interaction.message.edit(embed=room_embed(code, interaction.guild), view=view)
        else:
            mid = rooms[code].get("message_id")
            if mid:
                msg = await interaction.channel.fetch_message(mid)
                await msg.edit(embed=room_embed(code, interaction.guild), view=view)
        await interaction.channel.send(f"🃏 Game started! <@{first}> — your turn!", delete_after=10)
    except Exception as e:
        log.error(f"_start_playing: {e}")

# ================================================================
# VIEWS — LOBBY
# ================================================================
class RoomLobbyView(View):
    def __init__(self, code):
        super().__init__(timeout=None); self.code = code

    @discord.ui.button(label="✅ Join Room", style=discord.ButtonStyle.green, row=0)
    async def join_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        if not room:                          return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        if room["state"] != "waiting":        return await interaction.response.send_message("❌ Game started.", ephemeral=True)
        if uid in room["players"]:            return await interaction.response.send_message("✅ Already in!", ephemeral=True)
        if len(room["players"]) >= 6:         return await interaction.response.send_message("❌ Room full.", ephemeral=True)
        get_player(uid, interaction.user.display_name); room["players"].append(uid); save_data()
        await interaction.response.send_message(f"✅ Joined `{self.code}`!", ephemeral=True)
        await interaction.message.edit(embed=room_embed(self.code, interaction.guild), view=self)

    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.red, row=0)
    async def leave_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        if not room or uid not in room["players"]: return await interaction.response.send_message("❌ Not in room.", ephemeral=True)
        room["players"].remove(uid)
        if not room["players"]:
            del rooms[self.code]
            return await interaction.response.edit_message(embed=discord.Embed(title="🚪 Room Closed", color=discord.Color.red()), view=None)
        if room["host"] == uid: room["host"] = room["players"][0]
        await interaction.response.send_message("🚪 Left.", ephemeral=True)
        await interaction.message.edit(embed=room_embed(self.code, interaction.guild), view=self)

    @discord.ui.button(label="▶️ Start Game", style=discord.ButtonStyle.blurple, row=0)
    async def start_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        if not room:                   return await interaction.response.send_message("❌ Room not found.", ephemeral=True)
        if room["host"] != uid:        return await interaction.response.send_message("❌ Only host can start.", ephemeral=True)
        if room["state"] != "waiting": return await interaction.response.send_message("❌ Already started.", ephemeral=True)
        room["state"] = "betting"; room["bets"] = {}
        await interaction.response.edit_message(embed=room_embed(self.code, interaction.guild), view=BettingView(self.code))
        await interaction.channel.send("💰 **Betting phase! Place your bets.**", delete_after=15)

# ================================================================
# VIEWS — BETTING
# ================================================================
class BettingView(View):
    def __init__(self, code):
        super().__init__(timeout=None); self.code = code

    @discord.ui.button(label="💰 Place Bet", style=discord.ButtonStyle.green)
    async def bet_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        if not room:                        return await interaction.response.send_message("❌ Room not found.", ephemeral=True)
        if uid not in room["players"]:      return await interaction.response.send_message("❌ Not in game.", ephemeral=True)
        if uid in room.get("bets", {}):     return await interaction.response.send_message(f"✅ Bet placed: **${room['bets'][uid]:,}**", ephemeral=True)
        await interaction.response.send_modal(BetModal(uid, self.code))

# ================================================================
# VIEWS — GAMEPLAY
# ================================================================
class GamePlayView(View):
    def __init__(self, code, current_uid):
        super().__init__(timeout=None); self.code = code; self.current_uid = current_uid

    async def _advance(self, interaction):
        room = rooms[self.code]; idx = room.get("current_idx", 0)
        while True:
            idx += 1
            if idx >= len(room["players"]):
                results = resolve_round(room); room["state"] = "finished"
                lines = []
                for uid, (outcome, amount) in results.items():
                    name = player_name(uid, interaction.guild)
                    if outcome in ("win", "blackjack"):
                        lines.append(f"🏆 **{name}**: +${amount:,}" + (" 🌟 BLACKJACK!" if outcome == "blackjack" else ""))
                    elif outcome == "push":
                        lines.append(f"🤝 **{name}**: Push")
                    else:
                        lines.append(f"❌ **{name}**: -${abs(amount):,}")
                res_embed = discord.Embed(title="🎴 Round Results", description="\n".join(lines) or "—", color=discord.Color.gold())
                await interaction.message.edit(embed=room_embed(self.code, interaction.guild), view=PostGameView(self.code))
                await interaction.channel.send(embed=res_embed, delete_after=30)
                return
            next_uid = room["players"][idx]
            if room["player_status"].get(next_uid) not in ("bust", "standing", "blackjack"):
                break
        room["current_idx"] = idx; next_uid = room["players"][idx]
        await interaction.message.edit(embed=room_embed(self.code, interaction.guild), view=GamePlayView(self.code, next_uid))
        await interaction.channel.send(f"🎯 <@{next_uid}> — your turn!", delete_after=10)

    @discord.ui.button(label="👆 Hit", style=discord.ButtonStyle.green, row=0)
    async def hit_btn(self, interaction, button):
        uid = str(interaction.user.id)
        if uid != self.current_uid: return await interaction.response.send_message("⏳ Not your turn.", ephemeral=True)
        room = rooms.get(self.code); card = room["deck"].pop(); room["hands"][uid].append(card)
        val  = hand_value(room["hands"][uid])
        await interaction.response.send_message(f"🃏 Drew **`{card_str(card)}`** = **{val}**", ephemeral=True)
        if val > 21:
            room["player_status"][uid] = "bust"
            await interaction.channel.send(f"💥 <@{uid}> **BUST!** ({val})", delete_after=8)
            await self._advance(interaction)
        elif val == 21:
            room["player_status"][uid] = "standing"
            await interaction.channel.send(f"🎯 <@{uid}> hits **21**!", delete_after=8)
            await self._advance(interaction)
        else:
            await interaction.message.edit(embed=room_embed(self.code, interaction.guild))

    @discord.ui.button(label="✋ Stand", style=discord.ButtonStyle.red, row=0)
    async def stand_btn(self, interaction, button):
        uid = str(interaction.user.id)
        if uid != self.current_uid: return await interaction.response.send_message("⏳ Not your turn.", ephemeral=True)
        room = rooms.get(self.code); room["player_status"][uid] = "standing"
        await interaction.response.send_message(f"✋ Stood at **{hand_value(room['hands'][uid])}**.", ephemeral=True)
        await self._advance(interaction)

    @discord.ui.button(label="⚡ Double Down", style=discord.ButtonStyle.blurple, row=0)
    async def double_btn(self, interaction, button):
        uid = str(interaction.user.id)
        if uid != self.current_uid: return await interaction.response.send_message("⏳ Not your turn.", ephemeral=True)
        room = rooms.get(self.code); p = get_player(uid); bet = room["bets"].get(uid, 0)
        if len(room["hands"][uid]) != 2: return await interaction.response.send_message("❌ Only on first 2 cards.", ephemeral=True)
        if p["money"] < bet:             return await interaction.response.send_message(f"❌ Need **${bet:,}**.", ephemeral=True)
        p["money"] -= bet; room["bets"][uid] = bet * 2
        card = room["deck"].pop(); room["hands"][uid].append(card); val = hand_value(room["hands"][uid]); save_data()
        await interaction.response.send_message(f"⚡ Doubled! Drew **`{card_str(card)}`** = **{val}** | Bet: **${room['bets'][uid]:,}**", ephemeral=True)
        room["player_status"][uid] = "bust" if val > 21 else "standing"
        if val > 21: await interaction.channel.send(f"💥 <@{uid}> **BUST after double!**", delete_after=8)
        await self._advance(interaction)

    @discord.ui.button(label="👁️ Card Peek", style=discord.ButtonStyle.gray, row=0)
    async def peek_btn(self, interaction, button):
        uid = str(interaction.user.id); p = get_player(uid); room = rooms.get(self.code)
        if "card_peek" not in p["items"]: return await interaction.response.send_message("❌ Need **👁️ card_peek**!", ephemeral=True)
        if not room["deck"]:              return await interaction.response.send_message("❌ Deck empty.", ephemeral=True)
        next_card = room["deck"][-1]; p["items"].remove("card_peek")
        room.setdefault("used_items", {}).setdefault(uid, []).append("card_peek"); save_data()
        await interaction.response.send_message(f"👁️ Next card: **`{card_str(next_card)}`**", ephemeral=True)

    @discord.ui.button(label="🎒 Use Item", style=discord.ButtonStyle.gray, row=1)
    async def item_btn(self, interaction, button):
        uid = str(interaction.user.id); p = get_player(uid)
        usable = [i for i in p["items"] if SHOP.get(i, {}).get("type") == "active" and i != "card_peek"]
        if not usable: return await interaction.response.send_message("❌ No active items.", ephemeral=True)
        opts = [discord.SelectOption(label=i, value=i, emoji=SHOP[i]["emoji"], description=SHOP[i]["desc"]) for i in usable[:25]]
        select = Select(placeholder="Choose item", options=opts); view = View()
        async def cb(inter):
            item = select.values[0]; room = rooms.get(self.code)
            msgs = {"luck_charm": "🍀 Luck Charm active!", "insurance_shield": "🛡️ Shield active!", "double_boost": "⚡ Boost active!"}
            room.setdefault("used_items", {}).setdefault(uid, []).append(item); p["items"].remove(item); save_data()
            await inter.response.send_message(msgs.get(item, f"Used **{item}**."), ephemeral=True)
        select.callback = cb; view.add_item(select)
        await interaction.response.send_message("Choose item:", view=view, ephemeral=True)

    @discord.ui.button(label="👤 My Hand", style=discord.ButtonStyle.gray, row=1)
    async def hand_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        hand = room["hands"].get(uid, []); bet = room["bets"].get(uid, 0); used = room.get("used_items", {}).get(uid, [])
        msg = f"🃏 `{hand_str(hand)}` = **{hand_value(hand)}**  💰 Bet: **${bet:,}**"
        if used: msg += f"\n⚡ Active: {', '.join(used)}"
        await interaction.response.send_message(msg, ephemeral=True)

    @discord.ui.button(label="💬 Emote", style=discord.ButtonStyle.gray, row=1)
    async def emote_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        if uid not in room.get("players", []): return await interaction.response.send_message("❌ Not in game.", ephemeral=True)
        opts = [discord.SelectOption(label=e, value=e) for e in ["👍","👎","😂","😱","🔥","💀","🤑","🫡"]]
        select = Select(placeholder="Send emote", options=opts); view = View()
        async def cb(inter):
            await inter.channel.send(f"{select.values[0]} **{player_name(uid, inter.guild)}**", delete_after=8)
            await inter.response.send_message("Sent!", ephemeral=True)
        select.callback = cb; view.add_item(select)
        await interaction.response.send_message("React:", view=view, ephemeral=True)

# ================================================================
# VIEWS — POST GAME
# ================================================================
class PostGameView(View):
    def __init__(self, code):
        super().__init__(timeout=120); self.code = code

    @discord.ui.button(label="🔄 Play Again", style=discord.ButtonStyle.green, row=0)
    async def again_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        if not room:           return await interaction.response.send_message("❌ Room not found.", ephemeral=True)
        if room["host"] != uid: return await interaction.response.send_message("❌ Only host can restart.", ephemeral=True)
        room.update(state="betting", bets={}, hands={}, player_status={}, used_items={}, current_idx=0)
        await interaction.response.edit_message(embed=room_embed(self.code, interaction.guild), view=BettingView(self.code))
        await interaction.channel.send("💰 **New round! Place your bets.**", delete_after=15)

    @discord.ui.button(label="🚪 End Room", style=discord.ButtonStyle.red, row=0)
    async def end_btn(self, interaction, button):
        uid = str(interaction.user.id); room = rooms.get(self.code)
        if not room:           return await interaction.response.send_message("❌ Room not found.", ephemeral=True)
        if room["host"] != uid: return await interaction.response.send_message("❌ Only host can end.", ephemeral=True)
        del rooms[self.code]
        await interaction.response.edit_message(embed=discord.Embed(title="🚪 Room Closed", description="Thanks for playing!", color=discord.Color.red()), view=None)

    @discord.ui.button(label="👤 My Profile", style=discord.ButtonStyle.gray, row=0)
    async def profile_btn(self, interaction, button):
        get_player(str(interaction.user.id), interaction.user.display_name)
        await interaction.response.send_message(embed=private_profile_embed(interaction.user), ephemeral=True)

    @discord.ui.button(label="🏆 Leaderboard", style=discord.ButtonStyle.blurple, row=0)
    async def lb_btn(self, interaction, button):
        await interaction.response.send_message(embed=leaderboard_embed(interaction.guild), ephemeral=True)

# ================================================================
# VIEWS — SHOP & BANK
# ================================================================
CARD_FEE_RATE = 0.08  # 8% card surcharge

def _apply_shop_purchase(p: dict, item: str) -> str | None:
    """Apply item effect to player. Returns error string or None on success."""
    if item == "xp_booster":
        p["flags"]["xp_booster"] = p["flags"].get("xp_booster", 0) + 10
    elif item == "heat_shield":
        p["heat"] = max(0, p.get("heat", 0) - 3)
    elif item == "energy_drink":
        get_energy(p)
        p["energy"] = min(MAX_ENERGY, p["energy"] + 50)
        p["last_energy_update"] = time.time()
    elif item == "bail_bond":
        if is_jailed(p) <= 0:
            return "❌ You're not in jail!"
        p["jailed_until"] = 0
    elif SHOP[item]["type"] == "theme":
        if item in p["items"]:
            return f"❌ You already own **{SHOP[item]['emoji']} {item}**!"
        p["items"].append(item)
        theme_key = item.replace("theme_", "")
        p["equipped"]["theme"] = theme_key
    else:
        p["items"].append(item)
    return None

class ShopPaymentView(View):
    def __init__(self, user_id: str, item: str):
        super().__init__(timeout=30)
        self.user_id = user_id
        self.item    = item

    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.button(label="💵 Cash (Free)", style=discord.ButtonStyle.green, row=0)
    async def cash_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/shop`.", ephemeral=True)
        p = get_player(self.user_id); price = SHOP[self.item]["price"]
        if p["money"] < price:
            return await interaction.response.send_message(f"❌ Need **${price:,}** in wallet.", ephemeral=True)
        p["money"] -= price
        err = _apply_shop_purchase(p, self.item)
        if err:
            p["money"] += price
            return await interaction.response.send_message(err, ephemeral=True)
        save_data()
        await interaction.response.edit_message(
            content=f"✅ Bought **{SHOP[self.item]['emoji']} {self.item}** for **${price:,}** (cash).",
            embed=None, view=None)

    @discord.ui.button(label="💳 Card — Bank (+8% fee)", style=discord.ButtonStyle.blurple, row=0)
    async def card_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/shop`.", ephemeral=True)
        p = get_player(self.user_id); base = SHOP[self.item]["price"]
        fee   = max(1, int(base * CARD_FEE_RATE))
        total = base + fee
        if p["bank"] < total:
            return await interaction.response.send_message(
                f"❌ Need **${total:,}** in bank (${base:,} + ${fee:,} fee). Current bank: **${p['bank']:,}**.", ephemeral=True)
        p["bank"] -= total
        err = _apply_shop_purchase(p, self.item)
        if err:
            p["bank"] += total
            return await interaction.response.send_message(err, ephemeral=True)
        save_data()
        await interaction.response.edit_message(
            content=f"✅ Bought **{SHOP[self.item]['emoji']} {self.item}** — **${base:,}** + **${fee:,}** card fee debited from bank.",
            embed=None, view=None)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.gray, row=0)
    async def cancel_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/shop`.", ephemeral=True)
        await interaction.response.edit_message(content="🛒 Purchase cancelled.", embed=None, view=None)

class ShopView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id
        opts = [discord.SelectOption(label=n, value=n, emoji=i["emoji"], description=f"${i['price']:,} — {i['desc']}") for n, i in SHOP.items()]
        select = Select(placeholder="🛒 Select item to buy", options=opts)
        async def cb(interaction):
            if str(interaction.user.id) != self.user_id:
                return await interaction.response.send_message("❌ Open your own `/shop`.", ephemeral=True)
            item  = select.values[0]; info = SHOP[item]; base = info["price"]
            fee   = max(1, int(base * CARD_FEE_RATE))
            embed = discord.Embed(
                title=f"{info['emoji']} {item}",
                description=info["desc"],
                color=discord.Color.blurple()
            )
            embed.add_field(name="💵 Cash (wallet)", value=f"**${base:,}** — no fee",              inline=True)
            embed.add_field(name="💳 Card (bank)",  value=f"**${base+fee:,}** — +${fee:,} fee", inline=True)
            embed.set_footer(text="Choose your payment method")
            await interaction.response.send_message(embed=embed, view=ShopPaymentView(self.user_id, item), ephemeral=True)
        select.callback = cb; self.add_item(select)

class BankView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id

    @discord.ui.button(label="⬆️ Deposit (Free)", style=discord.ButtonStyle.green)
    async def dep_btn(self, interaction, button):
        await interaction.response.send_modal(DepositModal(self.user_id))

    @discord.ui.button(label="⬇️ Withdraw (5% fee)", style=discord.ButtonStyle.red)
    async def wth_btn(self, interaction, button):
        await interaction.response.send_modal(WithdrawModal(self.user_id))

# ================================================================
# VIEWS — MULTIPLAYER PICK
# ================================================================
class MultiplayerPickView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=60)
        self.user_id = user_id

    def _ok(self, interaction):
        return str(interaction.user.id) == self.user_id

    @discord.ui.button(label="🃏 Blackjack Room", style=discord.ButtonStyle.green, row=0)
    async def bj_room_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        uid = self.user_id
        for c, r in list(rooms.items()):
            if r["host"] == uid and r["state"] == "waiting": del rooms[c]
        code = gen_room_code()
        rooms[code] = {"host": uid, "players": [uid], "state": "waiting", "deck": [], "hands": {}, "bets": {},
                       "dealer_hand": [], "player_status": {}, "used_items": {}, "current_idx": 0,
                       "channel_id": interaction.channel_id, "game_mode": "blackjack"}
        save_data()
        server_ch = _get_server_channel(uid) or interaction.channel
        guild     = server_ch.guild if hasattr(server_ch, "guild") else interaction.guild
        embed     = room_embed(code, guild)
        msg       = await server_ch.send(embed=embed, view=RoomLobbyView(code))
        rooms[code]["message_id"] = msg.id
        await interaction.response.send_message(f"✅ Blackjack room `{code}` created! Friends: `!join {code}`", ephemeral=True)

    @discord.ui.button(label="✊ Rock Paper Scissors Room", style=discord.ButtonStyle.blurple, row=0)
    async def rps_room_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        uid = self.user_id
        for c, r in list(rooms.items()):
            if r["host"] == uid and r["state"] == "waiting": del rooms[c]
        code = gen_room_code()
        rooms[code] = {"host": uid, "players": [uid], "state": "waiting", "game_mode": "rps",
                       "channel_id": interaction.channel_id}
        save_data()
        embed = discord.Embed(
            title=f"✊ RPS Room  `{code}`",
            description=f"**Host:** <@{uid}>  •  Waiting for a challenger...",
            color=discord.Color.blurple()
        )
        embed.add_field(name="👥 Players (1/2)", value=f"🪑 1: <@{uid}>", inline=False)
        embed.set_footer(text=f"Join: !join {code}  •  Max 2 players")
        server_ch = _get_server_channel(uid) or interaction.channel
        msg = await server_ch.send(embed=embed, view=RpsRoomLobbyView(code))
        rooms[code]["message_id"] = msg.id
        await interaction.response.send_message(f"✅ RPS room `{code}` created! Friends: `!join {code}`", ephemeral=True)

    @discord.ui.button(label="🎡 Roulette Room", style=discord.ButtonStyle.red, row=0)
    async def rou_room_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        uid = self.user_id
        for c, r in list(rooms.items()):
            if r.get("host") == uid and r.get("state") == "waiting" and r.get("game_mode") == "roulette_pvt":
                del rooms[c]
        code = gen_room_code()
        rooms[code] = {
            "host": uid, "players": [uid], "state": "waiting",
            "game_mode": "roulette_pvt", "bets": {}, "payouts": {},
            "result": None, "channel_id": interaction.channel_id, "message_id": None,
        }
        server_ch = _get_server_channel(uid) or interaction.channel
        msg = await server_ch.send(embed=_pvt_rou_embed(code), view=RouletteRoomLobbyView(code))
        rooms[code]["message_id"] = msg.id
        await interaction.response.send_message(
            f"🎡 Private Roulette room created!\nShare code **`{code}`** — friends use `/join {code}`",
            ephemeral=True
        )

    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        await _back_to_main(interaction, self.user_id)


class RpsRoomLobbyView(View):
    def __init__(self, code: str):
        super().__init__(timeout=None)
        self.code = code

    @discord.ui.button(label="✅ Join Room", style=discord.ButtonStyle.green, row=0)
    async def join_btn(self, interaction: discord.Interaction, button: Button):
        uid  = str(interaction.user.id)
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        if room["state"] != "waiting": return await interaction.response.send_message("❌ Game started.", ephemeral=True)
        if uid == room["host"]: return await interaction.response.send_message("✅ You're already the host.", ephemeral=True)
        if room.get("challenger"): return await interaction.response.send_message("❌ Room is full (2/2).", ephemeral=True)
        get_player(uid, interaction.user.display_name)
        room["challenger"] = uid; save_data()
        embed = discord.Embed(
            title=f"✊ RPS Room  `{self.code}`",
            description=f"**Host:** <@{room['host']}>  •  Ready to start!",
            color=discord.Color.blurple()
        )
        embed.add_field(name="👥 Players (2/2)", value=f"🪑 1: <@{room['host']}>\n🪑 2: <@{uid}>", inline=False)
        embed.set_footer(text="Host — start when ready!")
        await interaction.response.edit_message(embed=embed, view=self)
        await interaction.followup.send(f"✅ <@{uid}> joined! Host can now start the game.", delete_after=10)

    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.red, row=0)
    async def leave_btn(self, interaction: discord.Interaction, button: Button):
        uid  = str(interaction.user.id)
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        if uid == room["host"]:
            del rooms[self.code]
            return await interaction.response.edit_message(embed=discord.Embed(title="🚪 Room Closed", color=discord.Color.red()), view=None)
        if uid == room.get("challenger"):
            room.pop("challenger", None)
            embed = discord.Embed(title=f"✊ RPS Room  `{self.code}`", description=f"**Host:** <@{room['host']}>  •  Waiting for challenger...", color=discord.Color.blurple())
            embed.add_field(name="👥 Players (1/2)", value=f"🪑 1: <@{room['host']}>", inline=False)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("❌ You're not in this room.", ephemeral=True)

    @discord.ui.button(label="▶️ Start Game", style=discord.ButtonStyle.blurple, row=0)
    async def start_btn(self, interaction: discord.Interaction, button: Button):
        uid  = str(interaction.user.id)
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        if uid != room["host"]: return await interaction.response.send_message("❌ Only the host can start.", ephemeral=True)
        if not room.get("challenger"): return await interaction.response.send_message("❌ Need 2 players to start.", ephemeral=True)
        await interaction.response.send_modal(RpsRoomBetModal(self.code))


class RpsRoomBetModal(Modal, title="✊ RPS Room — Set Bet"):
    amount = TextInput(label="Bet Amount", placeholder="Min $10, or type 'all'", required=True)

    def __init__(self, code: str):
        super().__init__()
        self.code = code

    async def on_submit(self, interaction: discord.Interaction):
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        uid  = str(interaction.user.id)
        p    = get_player(uid, interaction.user.display_name)
        raw  = self.amount.value.strip().lower()
        bet  = p["money"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if bet <= 0:        return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        if bet < 10:        return await interaction.response.send_message("❌ Min bet is **$10**.", ephemeral=True)
        if bet > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)

        p["money"] -= bet; save_data()
        cid = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        rps_challenges[cid] = {
            "challenger": uid, "challenged": room["challenger"],
            "bet": bet, "challenger_choice": None, "challenged_choice": None,
            "accepted": True, "resolved": False,
        }

        embed = discord.Embed(
            title="⚔️ RPS Battle — In Progress",
            description=(f"<@{uid}> **VS** <@{room['challenger']}>\n"
                         f"💰 Pot: **${bet*2:,}** — winner takes all!\n"
                         f"🤫 Both players — pick your move secretly!"),
            color=discord.Color.orange()
        )
        room_ch = bot.get_channel(room.get("channel_id")) or interaction.channel
        try:
            mid = room.get("message_id")
            if mid:
                msg = await room_ch.fetch_message(mid)
                await msg.edit(embed=embed, view=None)
        except Exception: pass

        rps_challenges[cid]["msg_id"]     = room.get("message_id")
        rps_challenges[cid]["channel_id"] = room.get("channel_id") or interaction.channel_id
        del rooms[self.code]

        await interaction.response.send_message("🤫 Your move (secret!):", view=RpsPickView(cid, "challenger"), ephemeral=True)
        await room_ch.send(f"<@{room['challenger']}> — pick your move!", delete_after=5)
        ch_view = RpsPickView(cid, "challenged")
        guild   = room_ch.guild if hasattr(room_ch, "guild") else None
        challenged_member = guild.get_member(int(room["challenger"])) if guild else None
        if challenged_member:
            try:
                await challenged_member.send("🤫 Your RPS move (secret!):", view=ch_view)
            except Exception:
                await room_ch.send(f"<@{room['challenger']}> click here to pick:", view=ch_view, delete_after=60)
        else:
            await room_ch.send(f"<@{room['challenger']}> click here to pick:", view=ch_view, delete_after=60)


# ================================================================
# VIEWS — CASINO MENU
# ================================================================
class CasinoMenuView(View):
    def __init__(self, user_id):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, interaction):
        return str(interaction.user.id) == self.user_id

    @discord.ui.button(label="🃏 Blackjack", style=discord.ButtonStyle.green, row=0)
    async def bj_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        get_player(self.user_id, interaction.user.display_name)
        await interaction.response.send_modal(SoloBetModal(self.user_id, interaction.channel_id))

    @discord.ui.button(label="🎰 Slots", style=discord.ButtonStyle.blurple, row=0)
    async def slots_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        get_player(self.user_id, interaction.user.display_name)
        await interaction.response.send_modal(SlotsModal(self.user_id))

    @discord.ui.button(label="🎲 Coin Flip", style=discord.ButtonStyle.red, row=0)
    async def flip_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        get_player(self.user_id, interaction.user.display_name)
        await interaction.response.send_modal(GambleModal(self.user_id))

    @discord.ui.button(label="🎡 Roulette", style=discord.ButtonStyle.red, row=1)
    async def roulette_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        roulette_webhooks[self.user_id] = interaction
        rs = roulette_state
        if rs["active"] and rs["phase"] == "betting":
            pot    = sum(b["amount"] for ub in rs["bets"].values() for b in ub)
            pcount = len(rs["bets"])
            elapsed = time.time() - rs.get("_start_time", time.time())
            left    = max(0, int(ROULETTE_BET_WINDOW - elapsed))
            embed = discord.Embed(
                title=f"🎡 Live Roulette — Round #{rs['round_id']}",
                description=(
                    f"⏳ **{left}s** left to bet!\n\n"
                    f"**Bet types:** `red · black · green · even · odd · low · high · 0–36`\n"
                    f"**Payouts:** Red/Black/Even/Odd/Low/High = **1.9×**  •  Green = **14×**  •  Number = **32×**"
                ),
                color=discord.Color.gold()
            )
            embed.add_field(name="👥 Players", value=str(pcount), inline=True)
            embed.add_field(name="💰 Pot",     value=f"**${pot:,}**", inline=True)
            embed.set_footer(text="Min $100 · Max $50,000 · Max 3 bets per round")
            await interaction.response.edit_message(embed=embed, view=RouletteFromCasinoView(self.user_id))
        elif rs["active"] and rs["phase"] == "locked":
            spin_nums = random.sample(range(0, 37), 9)
            def _spin_fmt(n):
                c = _rou_color(n)
                return f"{_rou_emoji(c)}`{n:02d}`"
            spinning_row = "  ".join(_spin_fmt(n) for n in spin_nums)
            embed = discord.Embed(
                title="🔒 Spinning...",
                description=(
                    f"**The wheel is spinning!**\n\n"
                    f"{spinning_row}\n\n"
                    f"*Keep refreshing to watch the numbers fly...*"
                ),
                color=discord.Color.orange()
            )
            embed.set_footer(text="🎡 Result will appear here when the wheel stops")
            await interaction.response.edit_message(embed=embed, view=RouletteFromCasinoView(self.user_id))
        else:
            nxt = roulette_state.get("next_round_at", 0)
            uid = self.user_id
            pre = roulette_state["pending_bets"].get(uid, [])
            nxt_str  = f"<t:{nxt}:R>" if nxt > time.time() else "**very soon**"
            countdown = f"⏳ Next round starts {nxt_str}"
            pre_info = ""
            if pre:
                pre_total = sum(b["amount"] for b in pre)
                pre_info  = f"\n\n✅ **{len(pre)} pre-bet(s) queued** (${pre_total:,}) — entering next round automatically."

            # Show last result in panel
            last_result_info = ""
            lr = (roulette_state.get("last_result") or {}).get(uid)
            if lr:
                rn = lr["round"]; we = lr["emoji"]; wn = lr["number"]; wc = lr["color"].upper()
                if lr["won"]:
                    last_result_info = f"\n\n🏆 **Round #{rn} result:** {we} {wn} {wc} — You won **+${lr['profit']:,}** (returned ${lr['returned']:,})"
                else:
                    last_result_info = f"\n\n💸 **Round #{rn} result:** {we} {wn} {wc} — You lost **-${lr['lost']:,}**"

            embed = discord.Embed(
                title="🎡 Live Roulette",
                description=(
                    f"{countdown}\n\n"
                    "**Place bets anytime** — they auto-enter when the round starts!\n\n"
                    "**Bet types:** `red · black · green · even · odd · low · high · 0–36`\n"
                    "**Payouts:** Red/Black/Even/Odd/Low/High = **1.9×**  •  Green = **14×**  •  Number = **32×**"
                    f"{pre_info}{last_result_info}"
                ),
                color=discord.Color.dark_blue()
            )
            embed.set_footer(text="Shared round — all players bet together  •  Min $100  •  Max 3 bets/round")
            await interaction.response.edit_message(embed=embed, view=RouletteFromCasinoView(self.user_id))

    @discord.ui.button(label="💣 Mines", style=discord.ButtonStyle.danger, row=1)
    async def mines_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        uid = self.user_id
        if uid in active_mines:
            return await interaction.response.send_message(
                embed=_mines_embed(active_mines[uid]),
                view=MinesGameView(uid), ephemeral=True
            )
        await interaction.response.send_modal(MinesBetModal(uid))

    @discord.ui.button(label="👥 Multiplayer", style=discord.ButtonStyle.gray, row=1)
    async def room_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        get_player(self.user_id, interaction.user.display_name)
        embed = discord.Embed(
            title="👥 Multiplayer — Choose Game",
            description="Pick the game mode for your private room:",
            color=discord.Color.dark_green()
        )
        embed.add_field(name="🃏 Blackjack", value="Classic multiplayer Blackjack vs the dealer", inline=False)
        embed.add_field(name="✊ Rock Paper Scissors", value="PvP — challenge a friend in the room", inline=False)
        embed.add_field(name="🎡 Roulette", value="Private roulette round — host spins for all players", inline=False)
        await interaction.response.edit_message(embed=embed, view=MultiplayerPickView(self.user_id))

    @discord.ui.button(label="🎟️ Lottery", style=discord.ButtonStyle.green, row=2)
    async def lottery_casino_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        await interaction.response.send_message(embed=_lottery_embed(), view=LotteryView(self.user_id), ephemeral=True)

    @discord.ui.button(label="🎫 Scratch Card ($200)", style=discord.ButtonStyle.blurple, row=2)
    async def scratch_casino_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; p = get_player(uid, interaction.user.display_name)
        # Use inventory card first, otherwise charge $200 directly
        if "scratch_card" in p.get("items", []):
            p["items"].remove("scratch_card")
        elif p["money"] >= SCRATCH_PRICE:
            p["money"] -= SCRATCH_PRICE
            save_data()
        else:
            return await interaction.response.send_message(f"❌ Need **${SCRATCH_PRICE:,}** to play. You have **${p['money']:,}**.", ephemeral=True)
        symbols = random.choices(SCRATCH_SYMBOLS, weights=SCRATCH_WEIGHTS, k=3)
        view = ScratchCardView(uid, symbols)
        await interaction.response.send_message(embed=view._build_embed(0), view=view, ephemeral=True)

    @discord.ui.button(label="🐔 Chicken Cross", style=discord.ButtonStyle.danger, row=2)
    async def chicken_casino_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        uid = self.user_id
        if uid in active_chicken:
            return await interaction.response.send_message(
                embed=_chicken_embed(active_chicken[uid]),
                view=ChickenCrossView(uid), ephemeral=True
            )
        await interaction.response.send_modal(ChickenBetModal(uid))

    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.gray, row=3)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        await _back_to_main(interaction, self.user_id)

class RouletteBetModal(Modal, title="🎡 Place Roulette Bet"):
    bet_type = TextInput(label="Bet Type", placeholder="red · black · green · even · odd · low · high · 0–36", max_length=10)
    bet_amt  = TextInput(label="Amount",   placeholder="e.g. 500", max_length=10)

    def __init__(self, user_id: str):
        super().__init__(); self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.user_id:
            return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)

        t = self.bet_type.value.lower().strip()
        is_num_bet = _rou_is_number_bet(t)
        if t not in ROULETTE_KEYWORD_BETS and not is_num_bet:
            return await interaction.response.send_message(
                "❌ Invalid type. Use: `red black green even odd low high` or a number `0–36`.", ephemeral=True)

        try:
            amt = int(self.bet_amt.value.strip())
        except ValueError:
            return await interaction.response.send_message("❌ Enter a valid number for the amount.", ephemeral=True)

        err = await _rou_place_bet(interaction, t, amt)
        if err:
            return await interaction.response.send_message(err, ephemeral=True)

        rs  = roulette_state
        uid = self.user_id
        is_live    = rs["active"] and rs["phase"] == "betting"
        bucket     = rs["bets"] if is_live else rs["pending_bets"]
        player_bets = bucket.get(uid, [])
        nxt        = rs["next_round_at"]
        nxt_str    = f"<t:{nxt}:R>"
        status     = "this round ✅" if is_live else f"next round ({nxt_str}) ⏳"

        if is_num_bet:
            payout_str = f"{ROULETTE_NUMBER_PAYOUT}×"
            icon  = _rou_emoji(_rou_color(int(t)))
            label = f"#{t}"
        else:
            payout_str = f"{ROULETTE_PAYOUTS[t]}×"
            icon  = _rou_emoji(t) if t in ("red", "black", "green") else "🎯"
            label = t.upper()

        total_in = sum(b["amount"] for b in player_bets)
        p = get_player(uid)
        await interaction.response.send_message(
            f"{icon} **${amt:,}** on **{label}** (payout: {payout_str}) — queued for **{status}**\n"
            f"Bets queued: **{len(player_bets)}/{ROULETTE_MAX_BETS}** — staked: **${total_in:,}**\n"
            f"💵 Wallet: **${p['money']:,}**",
            ephemeral=True
        )


# ================================================================
# PRIVATE ROULETTE ROOM
# ================================================================
def _pvt_rou_embed(code: str) -> discord.Embed:
    room  = rooms[code]
    state = room["state"]
    bets  = room.get("bets", {})

    if state == "waiting":
        color = discord.Color.blue()
        title = f"🎡 Private Roulette  `{code}`"
        desc  = f"**Host:** <@{room['host']}>\nWaiting for players — host starts when ready!"
    elif state == "betting":
        left   = max(0, int(room.get("bet_end_at", time.time()) - time.time()))
        color  = discord.Color.gold()
        title  = f"🎡 Private Roulette  `{code}` — Betting Phase"
        desc   = f"⏳ **{left}s** left to bet!\nUse the **Place Bet** button!"
    elif state == "spinning":
        color = discord.Color.orange()
        title = f"🎡 Private Roulette  `{code}` — Spinning..."
        desc  = "🔒 Bets closed — wheel is spinning!"
    else:  # finished
        res = room.get("result", {})
        e   = _rou_emoji(res.get("color", "green"))
        n   = res.get("number", 0)
        c   = res.get("color", "green").upper()
        color = discord.Color.green()
        title = f"🎡 Private Roulette  `{code}` — {e} {n} {c}"
        desc  = f"**Result: {e} {n} — {c}**"

    embed = discord.Embed(title=title, description=desc, color=color)

    # Player list
    lines = []
    for uid in room["players"]:
        name      = players.get(uid, {}).get("name", f"Player {uid[:6]}")
        player_bets = bets.get(uid, [])
        if state == "waiting":
            tag = "👑 Host" if uid == room["host"] else "✅ Ready"
        elif state in ("betting", "spinning"):
            if player_bets:
                total = sum(b["amount"] for b in player_bets)
                tag = f"✅ ${total:,} staked"
            else:
                tag = "⏳ no bet yet"
        else:
            pay = room.get("payouts", {}).get(uid)
            if pay:
                tag = f"🏆 +${pay['profit']:,}" if pay["won"] else f"💸 -${pay['lost']:,}"
            else:
                tag = "—"
        lines.append(f"• **{name}** — {tag}")

    embed.add_field(name=f"👥 Players ({len(room['players'])}/8)", value="\n".join(lines), inline=False)
    embed.set_footer(text=f"Join: /join {code}  •  Min $100  •  Max 3 bets/player  •  1.9× / 14× / 32×")
    return embed


async def _run_pvt_roulette(code: str):
    room = rooms.get(code)
    if not room: return

    room["state"]      = "betting"
    room["bets"]       = {uid: [] for uid in room["players"]}
    room["bet_end_at"] = time.time() + ROULETTE_BET_WINDOW

    channel = bot.get_channel(room["channel_id"])
    try:
        msg = await channel.fetch_message(room["message_id"])
    except Exception:
        return

    # Betting window — update every 2s
    elapsed = 0
    while elapsed < ROULETTE_BET_WINDOW:
        try:
            await msg.edit(embed=_pvt_rou_embed(code), view=RouletteRoomBettingView(code))
        except Exception: pass
        await asyncio.sleep(2)
        elapsed += 2

    # Lock bets
    room["state"] = "spinning"
    try:
        await msg.edit(embed=_pvt_rou_embed(code), view=None)
    except Exception: pass
    await asyncio.sleep(1)

    # Spinning animation — 4 frames × 1.5s
    for _ in range(4):
        nums = random.sample(range(0, 37), 9)
        row  = "  ".join(f"{_rou_emoji(_rou_color(n))}`{n:02d}`" for n in nums)
        lines = []
        for uid in room["players"]:
            name  = players.get(uid, {}).get("name", f"Player {uid[:6]}")
            pbets = room["bets"].get(uid, [])
            total = sum(b["amount"] for b in pbets)
            tag   = f"✅ ${total:,} staked" if pbets else "⏳ no bet"
            lines.append(f"• **{name}** — {tag}")
        spin_embed = discord.Embed(
            title=f"🎡 Private Roulette `{code}` — Spinning...",
            description=f"**The wheel is rolling!**\n\n{row}",
            color=discord.Color.orange()
        )
        spin_embed.add_field(name=f"👥 Players ({len(room['players'])}/8)", value="\n".join(lines) or "—", inline=False)
        try:
            await msg.edit(embed=spin_embed, view=None)
        except Exception: pass
        await asyncio.sleep(1.5)

    # Winning number
    winning_num   = random.randint(0, 36)
    winning_color = _rou_color(winning_num)
    winning_emoji = _rou_emoji(winning_color)
    room["result"] = {"number": winning_num, "color": winning_color}

    # Payouts
    payouts = {}
    for uid, uid_bets in room["bets"].items():
        p = get_player(uid)
        total_in   = sum(b["amount"] for b in uid_bets)
        total_back = 0
        for b in uid_bets:
            if _rou_bet_wins(b["type"], winning_num, winning_color):
                mult = ROULETTE_NUMBER_PAYOUT if _rou_is_number_bet(b["type"]) else ROULETTE_PAYOUTS[b["type"]]
                total_back += int(b["amount"] * mult)
        won    = total_back > 0
        profit = total_back - total_in if won else 0
        if won: p["money"] += total_back
        payouts[uid] = {"won": won, "profit": profit, "lost": total_in, "returned": total_back}
    room["payouts"] = payouts
    room["state"]   = "finished"
    save_data()

    try:
        await msg.edit(embed=_pvt_rou_embed(code), view=RouletteRoomFinishedView(code))
    except Exception: pass


class RouletteRoomBetModal(Modal, title="🎡 Place Your Bet"):
    bet_type = TextInput(label="Bet Type", placeholder="red · black · green · even · odd · low · high · 0–36", max_length=10)
    bet_amt  = TextInput(label="Amount",   placeholder="e.g. 500", max_length=10)

    def __init__(self, code: str, user_id: str):
        super().__init__(); self.code = code; self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        uid  = str(interaction.user.id)
        room = rooms.get(self.code)
        if not room or room["state"] != "betting":
            return await interaction.response.send_message("❌ Betting is closed.", ephemeral=True)

        t = self.bet_type.value.lower().strip()
        is_num = _rou_is_number_bet(t)
        if t not in ROULETTE_KEYWORD_BETS and not is_num:
            return await interaction.response.send_message("❌ Invalid type: `red black green even odd low high` or `0–36`.", ephemeral=True)

        try: amt = int(self.bet_amt.value.strip())
        except ValueError: return await interaction.response.send_message("❌ Enter a valid number.", ephemeral=True)

        p = get_player(uid, interaction.user.display_name)
        if amt < ROULETTE_MIN_BET:  return await interaction.response.send_message(f"❌ Min **${ROULETTE_MIN_BET:,}**.", ephemeral=True)
        if amt > ROULETTE_MAX_BET:  return await interaction.response.send_message(f"❌ Max **${ROULETTE_MAX_BET:,}**.", ephemeral=True)
        if amt > p["money"]:        return await interaction.response.send_message(f"❌ Not enough cash (${p['money']:,}).", ephemeral=True)

        player_bets = room["bets"].get(uid, [])
        if len(player_bets) >= ROULETTE_MAX_BETS:
            return await interaction.response.send_message(f"❌ Max {ROULETTE_MAX_BETS} bets per round.", ephemeral=True)

        p["money"] -= amt
        player_bets.append({"type": t, "amount": amt})
        room["bets"][uid] = player_bets
        save_data()

        mult  = ROULETTE_NUMBER_PAYOUT if is_num else ROULETTE_PAYOUTS[t]
        icon  = _rou_emoji(_rou_color(int(t))) if is_num else (_rou_emoji(t) if t in ("red","black","green") else "🎯")
        label = f"#{t}" if is_num else t.upper()
        total = sum(b["amount"] for b in player_bets)

        # Update room embed
        try:
            channel = bot.get_channel(room["channel_id"])
            msg     = await channel.fetch_message(room["message_id"])
            await msg.edit(embed=_pvt_rou_embed(self.code))
        except Exception: pass

        await interaction.response.send_message(
            f"{icon} **${amt:,}** on **{label}** ({mult}×) — staked total: **${total:,}**\n💵 Wallet: **${p['money']:,}**",
            ephemeral=True
        )


class RouletteRoomLobbyView(View):
    def __init__(self, code: str):
        super().__init__(timeout=300); self.code = code

    @discord.ui.button(label="✅ Join Room", style=discord.ButtonStyle.green, row=0)
    async def join_btn(self, interaction: discord.Interaction, button: Button):
        uid  = str(interaction.user.id)
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        if room["state"] != "waiting": return await interaction.response.send_message("❌ Game already started.", ephemeral=True)
        if uid in room["players"]: return await interaction.response.send_message("✅ You're already in this room.", ephemeral=True)
        if len(room["players"]) >= 8: return await interaction.response.send_message("❌ Room is full (8/8).", ephemeral=True)
        get_player(uid, interaction.user.display_name)
        room["players"].append(uid); save_data()
        try: await interaction.message.edit(embed=_pvt_rou_embed(self.code))
        except Exception: pass
        await interaction.response.send_message(f"✅ Joined roulette room `{self.code}`!", ephemeral=True)

    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.red, row=0)
    async def leave_btn(self, interaction: discord.Interaction, button: Button):
        uid  = str(interaction.user.id)
        room = rooms.get(self.code)
        if not room or uid not in room["players"]:
            return await interaction.response.send_message("❌ Not in this room.", ephemeral=True)
        if uid == room["host"]:
            rooms.pop(self.code, None)
            try: await interaction.message.delete()
            except Exception: pass
            return await interaction.response.send_message("🚪 Room closed — you were the host.", ephemeral=True)
        room["players"].remove(uid)
        try: await interaction.message.edit(embed=_pvt_rou_embed(self.code))
        except Exception: pass
        await interaction.response.send_message("🚪 Left the room.", ephemeral=True)

    @discord.ui.button(label="▶️ Start Game", style=discord.ButtonStyle.blurple, row=0)
    async def start_btn(self, interaction: discord.Interaction, button: Button):
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        if str(interaction.user.id) != room["host"]:
            return await interaction.response.send_message("❌ Only the host can start.", ephemeral=True)
        self.stop()
        await interaction.response.send_message("🎡 Round starting!", ephemeral=True)
        asyncio.create_task(_run_pvt_roulette(self.code))


class RouletteRoomBettingView(View):
    def __init__(self, code: str):
        super().__init__(timeout=ROULETTE_BET_WINDOW + 5); self.code = code

    @discord.ui.button(label="🎲 Place Bet", style=discord.ButtonStyle.green, row=0)
    async def bet_btn(self, interaction: discord.Interaction, button: Button):
        uid = str(interaction.user.id)
        if uid not in rooms.get(self.code, {}).get("players", []):
            return await interaction.response.send_message("❌ You're not in this room.", ephemeral=True)
        await interaction.response.send_modal(RouletteRoomBetModal(self.code, uid))


class RouletteRoomFinishedView(View):
    def __init__(self, code: str):
        super().__init__(timeout=120); self.code = code

    @discord.ui.button(label="🔄 New Round", style=discord.ButtonStyle.green, row=0)
    async def new_btn(self, interaction: discord.Interaction, button: Button):
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room gone.", ephemeral=True)
        if str(interaction.user.id) != room["host"]:
            return await interaction.response.send_message("❌ Only the host can start a new round.", ephemeral=True)
        room["state"]   = "waiting"
        room["bets"]    = {}
        room["payouts"] = {}
        room["result"]  = None
        self.stop()
        try:
            await interaction.message.edit(embed=_pvt_rou_embed(self.code), view=RouletteRoomLobbyView(self.code))
        except Exception: pass
        await interaction.response.send_message("✅ Lobby reset — start when ready!", ephemeral=True)

    @discord.ui.button(label="🚪 Close Room", style=discord.ButtonStyle.red, row=0)
    async def close_btn(self, interaction: discord.Interaction, button: Button):
        room = rooms.get(self.code)
        if not room: return await interaction.response.send_message("❌ Room already gone.", ephemeral=True)
        if str(interaction.user.id) != room["host"]:
            return await interaction.response.send_message("❌ Only the host can close.", ephemeral=True)
        rooms.pop(self.code, None)
        try: await interaction.message.delete()
        except Exception: pass
        await interaction.response.send_message("🚪 Room closed.", ephemeral=True)


class RouletteFromCasinoView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=60); self.user_id = user_id

    def _ok(self, interaction): return str(interaction.user.id) == self.user_id

    @discord.ui.button(label="🎲 Place Bet", style=discord.ButtonStyle.green, row=0)
    async def bet_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        roulette_webhooks[self.user_id] = interaction
        await interaction.response.send_modal(RouletteBetModal(self.user_id))

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.blurple, row=0)
    async def refresh_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        roulette_webhooks[self.user_id] = interaction
        rs  = roulette_state
        uid = self.user_id
        if rs["active"] and rs["phase"] == "betting":
            pot     = sum(b["amount"] for ub in rs["bets"].values() for b in ub)
            pcount  = len(rs["bets"])
            elapsed = time.time() - rs.get("_start_time", time.time())
            left    = max(0, int(ROULETTE_BET_WINDOW - elapsed))
            embed   = discord.Embed(
                title=f"🎡 Live Roulette — Round #{rs['round_id']}",
                description=(
                    f"⏳ **{left}s** left to bet!\n\n"
                    f"**Bet types:** `red · black · green · even · odd · low · high · 0–36`\n"
                    f"**Payouts:** Red/Black/Even/Odd/Low/High = **1.9×**  •  Green = **14×**  •  Number = **32×**"
                ),
                color=discord.Color.gold()
            )
            embed.add_field(name="👥 Players", value=str(pcount), inline=True)
            embed.add_field(name="💰 Pot",     value=f"**${pot:,}**", inline=True)
            embed.set_footer(text="Min $100 · Max $50,000 · Max 3 bets per round")
        elif rs["active"] and rs["phase"] == "locked":
            spin_nums = random.sample(range(0, 37), 9)
            def _spin_fmt(n):
                c = _rou_color(n)
                return f"{_rou_emoji(c)}`{n:02d}`"
            spinning_row = "  ".join(_spin_fmt(n) for n in spin_nums)
            embed = discord.Embed(
                title="🔒 Spinning...",
                description=(
                    f"**The wheel is spinning!**\n\n"
                    f"{spinning_row}\n\n"
                    f"*Keep refreshing to watch the numbers fly...*"
                ),
                color=discord.Color.orange()
            )
            embed.set_footer(text="🎡 Result will appear here when the wheel stops")
        else:
            nxt     = rs.get("next_round_at", 0)
            nxt_str = f"<t:{nxt}:R>" if nxt > time.time() else "**very soon**"
            pre     = rs["pending_bets"].get(uid, [])
            pre_info = ""
            if pre:
                pre_total = sum(b["amount"] for b in pre)
                pre_info  = f"\n\n✅ **{len(pre)} pre-bet(s) queued** (${pre_total:,}) — entering next round automatically."
            last_result_info = ""
            lr = (rs.get("last_result") or {}).get(uid)
            if lr:
                rn = lr["round"]; we = lr["emoji"]; wn = lr["number"]; wc = lr["color"].upper()
                if lr["won"]:
                    last_result_info = f"\n\n🏆 **Round #{rn} result:** {we} {wn} {wc} — You won **+${lr['profit']:,}**!"
                else:
                    last_result_info = f"\n\n💸 **Round #{rn} result:** {we} {wn} {wc} — You lost **-${lr['lost']:,}**"
            embed = discord.Embed(
                title="🎡 Live Roulette",
                description=(
                    f"⏳ Next round {nxt_str}\n\n"
                    "**Place bets anytime** — they auto-enter when the round starts!\n\n"
                    "**Bet types:** `red · black · green · even · odd · low · high · 0–36`\n"
                    "**Payouts:** Red/Black/Even/Odd/Low/High = **1.9×**  •  Green = **14×**  •  Number = **32×**"
                    f"{pre_info}{last_result_info}"
                ),
                color=discord.Color.dark_blue()
            )
            embed.set_footer(text="Shared round — all players bet together  •  Min $100  •  Max 3 bets/round")
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="❌ Cancel Bets", style=discord.ButtonStyle.red, row=0)
    async def cancel_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        uid = self.user_id
        rs  = roulette_state
        p   = get_player(uid)

        # Refund pending pre-bets (can't cancel live bets once round is spinning)
        pending = rs["pending_bets"].pop(uid, [])
        refund  = sum(b["amount"] for b in pending)

        if refund > 0:
            p["money"] += refund
            save_data()
            msg = f"✅ Cancelled **{len(pending)} queued bet(s)** — **${refund:,}** refunded to your wallet.\n💵 Wallet: **${p['money']:,}**"
        elif rs["active"] and rs["phase"] == "betting" and uid in rs["bets"]:
            msg = "⚠️ Round is already live — can't cancel active bets. Your bets will be resolved this round."
        else:
            msg = "ℹ️ No pending bets to cancel."

        await interaction.response.send_message(msg, ephemeral=True)

    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id)
        embed = discord.Embed(title="🎰 Casino", description="Pick your game!", color=discord.Color.dark_gold())
        embed.add_field(name="🃏 Blackjack",      value="Beat the dealer — Blackjack pays 3:2",    inline=False)
        embed.add_field(name="🎰 Slots",           value="Spin to win — jackpot at 0.5% chance",   inline=False)
        embed.add_field(name="🎡 Roulette",        value="Live shared rounds — all bets resolved together", inline=False)
        embed.add_field(name="🎲 Coin Flip",       value="50/50 — double or nothing",              inline=False)
        embed.add_field(name="👥 Multiplayer",     value="Private table with friends",              inline=False)
        await interaction.response.edit_message(embed=embed, view=CasinoMenuView(self.user_id))


# ================================================================
# JOB SYSTEM HELPERS & VIEWS
# ================================================================
def _job_menu_embed(p: dict) -> discord.Embed:
    promotable = [k for k in JOBS if _can_promote(p, k)]
    promoted   = [k for k in JOBS if _is_promoted(p, k)]

    def _job_label(key):
        j = _effective_job(p, key)
        star = "⭐ " if _can_promote(p, key) else ("✨ " if _is_promoted(p, key) else "")
        return f"{star}{j['emoji']} {j['name']}"

    safe_jobs  = "  •  ".join(_job_label(k) for k, j in JOBS.items() if j["cat"] == "safe")
    risky_jobs = "  •  ".join(_job_label(k) for k, j in JOBS.items() if j["cat"] == "risky")
    skill_jobs = "  •  ".join(f"{_job_label(k)} (Lv{j['req']})" for k, j in JOBS.items() if j["cat"] == "skill")

    embed = discord.Embed(title="💼 Jobs", description="Choose your career path!", color=discord.Color.blurple())
    embed.add_field(name="🟢 Safe Jobs",  value=f"No fail chance — guaranteed pay\n{safe_jobs}",  inline=False)
    embed.add_field(name="🔴 Risky Jobs", value=f"High reward — caught = fine + heat\n{risky_jobs}", inline=False)
    embed.add_field(name="🟣 Skill Jobs", value=f"Level-gated + push-your-luck\n{skill_jobs}",    inline=False)
    if promotable:
        names = "  •  ".join(f"{JOB_PROMOTIONS[k]['emoji']} {JOB_PROMOTIONS[k]['name']}" for k in promotable)
        embed.add_field(name="⭐ Promotions Available!", value=f"Go to the job category and hit **Promote**:\n{names}", inline=False)
    footer_parts = [f"Level {p['level']}"]
    if promoted: footer_parts.append(f"{len(promoted)} job(s) promoted ✨")
    if event_active("xp_boost"): footer_parts.append(f"🎉 2× XP Boost! ({fmt_cd(event_time_left())} left)")
    embed.set_footer(text="  •  ".join(footer_parts))
    return embed

def _cat_rem(p: dict, cat: str) -> int:
    return max(0, int(p["cooldowns"].get(f"job_cat_{cat}_until", 0) - time.time()))

async def _do_job(interaction: discord.Interaction, job_key: str):
    uid = str(interaction.user.id)
    p   = get_player(uid, interaction.user.display_name)
    job = _effective_job(p, job_key)

    jail_rem = is_jailed(p)
    if jail_rem > 0:
        return await interaction.response.send_message(f"⛓️ You're in **JAIL**! Released in `{fmt_cd(jail_rem)}`.", ephemeral=True)

    if p["level"] < job["req"]:
        return await interaction.response.send_message(
            f"🔒 **{job['name']}** requires **Level {job['req']}**! You are Level {p['level']}.", ephemeral=True)

    energy_action = f"{job['cat']}_job"
    if not use_energy(p, energy_action):
        eng = get_energy(p)
        return await interaction.response.send_message(
            f"⚡ Not enough energy! **{eng}/{MAX_ENERGY}** (need **{ENERGY_COST[energy_action]}**). Regens 1 per 3 min.", ephemeral=True)

    cat_rem = _cat_rem(p, job["cat"])
    if cat_rem > 0:
        return await interaction.response.send_message(
            f"⏳ **{job['cat'].title()} Jobs** locked — you already worked this cycle!\nUnlocks in `{fmt_cd(cat_rem)}`.", ephemeral=True)

    cd_key = f"job_{job_key}"
    rem    = cd_remaining(p, cd_key, job["cd"])
    if rem > 0:
        return await interaction.response.send_message(f"⏳ **{job['name']}** cooldown: `{fmt_cd(rem)}`", ephemeral=True)

    # All checks passed — show mode picker
    fatigue      = job_fatigue_mult(p, job_key)
    streak_count = p.get("job_streak", {}).get("count", 1)
    fatigue_note = ""
    if streak_count >= 2:
        penalty_pct  = min((streak_count - 1) * 10, 30)
        fatigue_note = f"\n⚠️ Job Fatigue: **-{penalty_pct}%** pay ({streak_count}× same job)"

    fail_chance = job["fail"]
    if job_key == "hack" and "hack_20" in _territory_perks_for_player(uid):
        fail_chance = max(0.05, fail_chance - 0.20)

    mode_lines = "\n".join(
        f"{m['label']} — {m['desc']}" for m in JOB_MODES.values()
    )
    embed = discord.Embed(
        title=f"{job['emoji']} {job['name']} — Choose Mode",
        description=f"Base pay: **${job['pay'][0]:,}–${job['pay'][1]:,}**  •  Base fail: **{int(fail_chance*100)}%**{fatigue_note}\n\n{mode_lines}",
        color=discord.Color.blurple()
    )
    embed.set_footer(text="Pick a mode — this cannot be undone.")
    await interaction.response.edit_message(
        embed=embed,
        view=JobModeView(uid, job_key, fail_chance)
    )


async def _execute_job_with_mode(
    interaction: discord.Interaction,
    job_key: str,
    mode_key: str,
    base_fail: float
):
    uid = str(interaction.user.id)
    p   = get_player(uid)
    job = _effective_job(p, job_key)

    job_train_mult = (1.0 + p.get("training", {}).get("jobs", 0) * TRAINING_STATS["jobs"]["bonus"]) * _career_job_pay_mult(p)
    base_pay = calculate_reward(
        job["pay"], p["level"],
        job_fatigue_mult(p, job_key),
        prestige_pay_bonus(p),
        _gang_income_multiplier(uid),
        job_train_mult
    )

    final_pay, final_fail, heat_add = apply_mode(base_pay, base_fail, job["heat"], mode_key)

    # Commit cooldowns now
    p["cooldowns"][f"job_{job_key}"]                    = time.time()
    p["cooldowns"][f"job_cat_{job['cat']}_until"]       = time.time() + job["cd"] * _career_job_cd_mult(p)

    mode_label = JOB_MODES[mode_key]["label"]

    # ── CAUGHT ──────────────────────────────────────────────────────
    if random.random() < final_fail:
        fine = random.randint(*job["fine"]) if job["fine"][1] > 0 else 0
        fine = min(fine, p["money"])
        p["money"] -= fine
        p["heat"]   = min(10, p.get("heat", 0) + max(0, int(heat_add * _career_heat_mult(p))))
        add_xp(p, job["xp"] // 3)
        save_data()

        embed = discord.Embed(
            title=f"🚨 {job['emoji']} {job['name']} — CAUGHT!",
            color=discord.Color.red()
        )
        embed.add_field(name="🎮 Mode",    value=mode_label,                                                        inline=True)
        if fine:
            embed.add_field(name="💸 Fine",value=f"-**${fine:,}**",                                                 inline=True)
        embed.add_field(name="🌡️ Heat",   value=f"`{heat_bar(p['heat'])}` {heat_label(p['heat'])}  (+{heat_add})", inline=False)
        embed.add_field(name="💵 Wallet",  value=f"**${p['money']:,}**",                                            inline=True)
        embed.set_footer(text=f"Cooldown: {fmt_cd(job['cd'])}")
        return await interaction.response.edit_message(embed=embed, view=_JobBackView(uid))

    # ── INTERACTIVE push-your-luck jobs (heist, bank, diamond) ──────
    if job_key in INTERACTIVE_JOBS:
        update_job_streak(p, job_key)
        push_pay  = int(final_pay * 1.4)
        push_fail = int((base_fail + 0.20) * 100)
        embed = discord.Embed(
            title=f"⚡ {job['emoji']} {job['name']} — In Position!",
            description=f"Mode: {mode_label} — You can earn **${final_pay:,}** safely.\nWant to push for **${push_pay:,}** ({push_fail}% fail)?",
            color=discord.Color.yellow()
        )
        embed.add_field(name="✅ Take the Money", value=f"**${final_pay:,}**",                              inline=True)
        embed.add_field(name="🎲 Push Your Luck", value=f"**${push_pay:,}** — {push_fail}% fail chance!", inline=True)
        return await interaction.response.edit_message(
            embed=embed,
            view=JobDecisionView(uid, job_key, final_pay)
        )

    # ── SUCCESS ──────────────────────────────────────────────────────
    ev_msg, ev_pay, ev_heat = trigger_event(job_key, final_pay)
    total = max(0, final_pay + ev_pay)

    p["money"] += total
    p["heat"]   = min(10, p.get("heat", 0) + max(0, int((heat_add + ev_heat) * _career_heat_mult(p))))
    leveled     = add_xp(p, job["xp"])
    p["stats"]["jobs_done"] = p["stats"].get("jobs_done", 0) + 1
    p.setdefault("job_counts", {}); p["job_counts"][job_key] = p["job_counts"].get(job_key, 0) + 1
    update_job_streak(p, job_key)
    _weekly_inc(p, "jobs")
    new_ach = check_achievements(p)
    save_data()

    streak_count = p.get("job_streak", {}).get("count", 1)

    embed = discord.Embed(title=f"✅ {job['emoji']} {job['name']} — Done!", color=discord.Color.green())
    embed.add_field(name="🎮 Mode",        value=mode_label,                  inline=True)
    embed.add_field(name="📦 Base Reward", value=f"**${base_pay:,}**",        inline=True)
    embed.add_field(name="💰 Final Pay",   value=f"**+${total:,}**",           inline=True)
    embed.add_field(name="⭐ XP",          value=f"+**{job['xp']}**",          inline=True)
    if heat_add != 0 or ev_heat != 0:
        net_heat = heat_add + ev_heat
        embed.add_field(name="🌡️ Heat",   value=f"`{heat_bar(p['heat'])}` ({'+' if net_heat >= 0 else ''}{net_heat})", inline=True)
    if ev_msg:
        embed.add_field(name="🎲 Event",   value=ev_msg,                       inline=False)
    embed.add_field(name="💵 Wallet",      value=f"**${p['money']:,}**",        inline=True)
    if leveled:
        embed.add_field(name="🆙 Level Up!", value=f"Now **Level {p['level']}**!", inline=False)
    if new_ach:
        embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
    newly_promotable = _can_promote(p, job_key)
    if newly_promotable:
        pj = JOB_PROMOTIONS[job_key]
        embed.add_field(name="⭐ Promotion Unlocked!", value=f"You can now promote to **{pj['emoji']} {pj['name']}**!\nGo to Jobs → {job['cat'].title()} Jobs and hit **Promote**.", inline=False)
    count = _job_count(p, job_key)
    if not _is_promoted(p, job_key) and job_key in JOB_PROMOTIONS:
        req = JOB_PROMOTIONS[job_key]["req_count"]
        progress = f"  •  {job['emoji']} {count}/{req} runs to promotion"
    else:
        progress = ""
    if streak_count >= 2:
        penalty_pct = min((streak_count - 1) * 10, 30)
        embed.set_footer(text=f"⚠️ Fatigue: -{penalty_pct}% (same job {streak_count}×)  •  CD: {fmt_cd(job['cd'])}{progress}")
    else:
        embed.set_footer(text=f"Cooldown: {fmt_cd(job['cd'])}{progress}")
    await interaction.response.edit_message(embed=embed, view=_JobBackView(uid))


class JobModeView(View):
    def __init__(self, user_id: str, job_key: str, base_fail: float):
        super().__init__(timeout=60)
        self.user_id   = user_id
        self.job_key   = job_key
        self.base_fail = base_fail

    async def _pick(self, interaction: discord.Interaction, mode_key: str):
        if str(interaction.user.id) != self.user_id:
            return await interaction.response.send_message("❌ Not your job!", ephemeral=True)
        self.stop()
        await _execute_job_with_mode(interaction, self.job_key, mode_key, self.base_fail)

    @discord.ui.button(label="🟢 Safe",    style=discord.ButtonStyle.green,  row=0)
    async def safe_btn(self, interaction: discord.Interaction, button: Button):
        await self._pick(interaction, "safe")

    @discord.ui.button(label="🟡 Normal",  style=discord.ButtonStyle.secondary, row=0)
    async def normal_btn(self, interaction: discord.Interaction, button: Button):
        await self._pick(interaction, "normal")

    @discord.ui.button(label="🔴 Risk",    style=discord.ButtonStyle.red,    row=0)
    async def risk_btn(self, interaction: discord.Interaction, button: Button):
        await self._pick(interaction, "risk")

    @discord.ui.button(label="💀 All-In",  style=discord.ButtonStyle.danger, row=0)
    async def allin_btn(self, interaction: discord.Interaction, button: Button):
        await self._pick(interaction, "allin")


class JobDecisionView(View):
    def __init__(self, user_id: str, job_key: str, base_pay: int):
        super().__init__(timeout=60)
        self.user_id  = user_id
        self.job_key  = job_key
        self.base_pay = base_pay

    async def _resolve(self, interaction: discord.Interaction, pushed: bool):
        uid = self.user_id; p = get_player(uid); job = _effective_job(p, self.job_key)
        self.stop(); self.clear_items()
        if pushed:
            fail_chance = job["fail"] + 0.20
            pay         = int(self.base_pay * 1.4)
            if random.random() < fail_chance:
                fine = random.randint(*job["fine"]) if job["fine"][1] > 0 else 0
                fine = min(fine, p["money"])
                p["money"] -= fine
                p["heat"]   = min(10, p.get("heat", 0) + job["heat"])
                add_xp(p, job["xp"] // 3); save_data()
                embed = discord.Embed(title=f"💀 {job['emoji']} Pushed Too Far — CAUGHT!", color=discord.Color.red())
                if fine: embed.add_field(name="💸 Fine",   value=f"-**${fine:,}**",                                   inline=True)
                embed.add_field(name="🌡️ Heat",  value=f"`{heat_bar(p['heat'])}` {heat_label(p['heat'])}",            inline=False)
                embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**",                                         inline=True)
                return await interaction.response.edit_message(embed=embed, view=_JobBackView(uid))
        else:
            pay = self.base_pay

        p["money"] += pay; leveled = add_xp(p, job["xp"])
        p.setdefault("job_counts", {}); p["job_counts"][self.job_key] = p["job_counts"].get(self.job_key, 0) + 1
        p["stats"]["jobs_done"] = p["stats"].get("jobs_done", 0) + 1
        save_data()
        newly_promotable = _can_promote(p, self.job_key)
        title = f"🎲 {job['emoji']} Risky Call Paid Off!" if pushed else f"✅ {job['emoji']} {job['name']} — Paid Out!"
        embed = discord.Embed(title=title, color=discord.Color.green())
        embed.add_field(name="💰 Earned",  value=f"**+${pay:,}**",      inline=True)
        embed.add_field(name="⭐ XP",      value=f"+**{job['xp']}**",    inline=True)
        embed.add_field(name="💵 Wallet",  value=f"**${p['money']:,}**", inline=True)
        if leveled: embed.add_field(name="🆙 Level Up!", value=f"Now **Level {p['level']}**!", inline=False)
        if newly_promotable:
            pj = JOB_PROMOTIONS[self.job_key]
            embed.add_field(name="⭐ Promotion Unlocked!", value=f"You can now promote to **{pj['emoji']} {pj['name']}**! Go to Jobs → {job['cat'].title()} Jobs.", inline=False)
        await interaction.response.edit_message(embed=embed, view=_JobBackView(uid))

    @discord.ui.button(label="✅ Take the Money",  style=discord.ButtonStyle.green)
    async def take_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.user_id:
            return await interaction.response.send_message("❌ Not your job!", ephemeral=True)
        await self._resolve(interaction, pushed=False)

    @discord.ui.button(label="🎲 Push Your Luck", style=discord.ButtonStyle.red)
    async def push_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.user_id:
            return await interaction.response.send_message("❌ Not your job!", ephemeral=True)
        await self._resolve(interaction, pushed=True)


class _JobBackView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=90); self.uid = uid

    @discord.ui.button(label="◀️ Back to Jobs", style=discord.ButtonStyle.gray)
    async def back_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.uid)
        await interaction.response.edit_message(embed=_job_menu_embed(p), view=JobMenuView(self.uid))


class JobMenuView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, interaction):
        return str(interaction.user.id) == self.user_id

    @discord.ui.button(label="🟢 Safe Jobs",  style=discord.ButtonStyle.green,   row=0)
    async def safe_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id); cr = _cat_rem(p, "safe")
        desc = f"⏳ Category locked — unlocks in `{fmt_cd(cr)}`" if cr > 0 else "No fail chance — guaranteed income!"
        embed = discord.Embed(title="🟢 Safe Jobs", description=desc, color=discord.Color.green() if cr == 0 else discord.Color.dark_gray())
        for key, job in JOBS.items():
            if job["cat"] != "safe": continue
            st = f"🔒 {fmt_cd(cr)}" if cr > 0 else ("⏳ " + fmt_cd(cd_remaining(p, f"job_{key}", job["cd"])) if cd_remaining(p, f"job_{key}", job["cd"]) > 0 else "✅ Ready")
            embed.add_field(name=f"{job['emoji']} {job['name']}", value=f"${job['pay'][0]}–${job['pay'][1]}  •  +{job['xp']} XP  •  {st}", inline=True)
        await interaction.response.edit_message(embed=embed, view=SafeJobsView(self.user_id))

    @discord.ui.button(label="🔴 Risky Jobs", style=discord.ButtonStyle.red,     row=0)
    async def risky_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id); cr = _cat_rem(p, "risky")
        desc = f"⏳ Category locked — unlocks in `{fmt_cd(cr)}`" if cr > 0 else "High reward — get caught = fine + heat!"
        embed = discord.Embed(title="🔴 Risky Jobs", description=desc, color=discord.Color.red() if cr == 0 else discord.Color.dark_gray())
        for key, job in JOBS.items():
            if job["cat"] != "risky": continue
            st = f"🔒 {fmt_cd(cr)}" if cr > 0 else ("⏳ " + fmt_cd(cd_remaining(p, f"job_{key}", job["cd"])) if cd_remaining(p, f"job_{key}", job["cd"]) > 0 else "✅ Ready")
            embed.add_field(name=f"{job['emoji']} {job['name']}", value=f"${job['pay'][0]}–${job['pay'][1]}  •  {int(job['fail']*100)}% fail  •  {st}", inline=True)
        await interaction.response.edit_message(embed=embed, view=RiskyJobsView(self.user_id))

    @discord.ui.button(label="🟣 Skill Jobs", style=discord.ButtonStyle.blurple,  row=0)
    async def skill_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id); cr = _cat_rem(p, "skill")
        desc = f"⏳ Category locked — unlocks in `{fmt_cd(cr)}`" if cr > 0 else "Level-gated elite jobs with push-your-luck decisions!"
        embed = discord.Embed(title="🟣 Skill Jobs", description=desc, color=discord.Color.blurple() if cr == 0 else discord.Color.dark_gray())
        for key, job in JOBS.items():
            if job["cat"] != "skill": continue
            if p["level"] < job["req"]: st = f"🔒 Lv{job['req']}"
            elif cr > 0:                st = f"🔒 {fmt_cd(cr)}"
            else:
                rem = cd_remaining(p, f"job_{key}", job["cd"])
                st  = f"⏳ {fmt_cd(rem)}" if rem > 0 else "✅ Ready"
            embed.add_field(name=f"{job['emoji']} {job['name']}", value=f"${job['pay'][0]}–${job['pay'][1]}  •  Lv{job['req']}+  •  {st}", inline=True)
        await interaction.response.edit_message(embed=embed, view=SkillJobsView(self.user_id))

    @discord.ui.button(label="◀️ Back",        style=discord.ButtonStyle.gray,    row=1)
    async def back_btn(self, interaction: discord.Interaction, button: Button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        await _back_to_main(interaction, self.user_id)


def _build_job_category_view(view_instance, cat: str, btn_style, job_row: int = 0):
    """Shared builder for all three category views — adds job buttons + promote buttons + back."""
    uid = view_instance.user_id
    p   = get_player(uid)
    cr  = _cat_rem(p, cat)

    for key, base_job in JOBS.items():
        if base_job["cat"] != cat: continue
        eff   = _effective_job(p, key)
        locked = cat == "skill" and p["level"] < base_job["req"]
        rem    = cr if cr > 0 else cd_remaining(p, f"job_{key}", eff["cd"])
        count  = _job_count(p, key)
        req    = JOB_PROMOTIONS[key]["req_count"] if key in JOB_PROMOTIONS else 0

        if _is_promoted(p, key):
            prefix = "✨ "
        elif _can_promote(p, key):
            prefix = "⭐ "
        else:
            prefix = ""

        if locked:    label = f"🔒 {eff['emoji']} {eff['name']} [Lv{base_job['req']}]"
        elif rem > 0: label = f"{prefix}{eff['emoji']} {eff['name']} [{fmt_cd(rem)}]"
        else:         label = f"{prefix}{eff['emoji']} {eff['name']}"
        if not locked and not _is_promoted(p, key) and req > 0:
            label += f" ({count}/{req})"

        btn = Button(label=label[:80],
                     style=btn_style if (not locked and rem == 0) else discord.ButtonStyle.gray,
                     disabled=(locked or cr > 0), row=job_row)
        async def cb(inter, k=key):
            if str(inter.user.id) != uid: return await inter.response.send_message("❌ Open your own `!play`.", ephemeral=True)
            await _do_job(inter, k)
        btn.callback = cb; view_instance.add_item(btn)

    # Promote buttons — one per promotable job in this category
    promote_keys = [k for k, j in JOBS.items() if j["cat"] == cat and _can_promote(p, k)]
    for key in promote_keys:
        pj  = JOB_PROMOTIONS[key]
        bj  = JOBS[key]
        btn = Button(label=f"⭐ Promote → {pj['emoji']} {pj['name']}", style=discord.ButtonStyle.blurple, row=job_row + 1)
        async def promote_cb(inter, k=key):
            if str(inter.user.id) != uid: return await inter.response.send_message("❌ Open your own `!play`.", ephemeral=True)
            p2  = get_player(uid)
            pj2 = JOB_PROMOTIONS[k]; bj2 = JOBS[k]
            embed = discord.Embed(
                title=f"⭐ Promote {bj2['emoji']} {bj2['name']} → {pj2['emoji']} {pj2['name']}?",
                description=(
                    f"**{bj2['emoji']} {bj2['name']}** → **{pj2['emoji']} {pj2['name']}**\n\n"
                    f"💰 Pay: **${bj2['pay'][0]:,}–${bj2['pay'][1]:,}** → **${pj2['pay'][0]:,}–${pj2['pay'][1]:,}**\n"
                    f"⭐ XP: **{bj2['xp']}** → **{pj2['xp']}**\n"
                    f"🎲 Fail: **{int(bj2['fail']*100)}%** → **{int(pj2['fail']*100)}%**\n\n"
                    f"*This is permanent and cannot be undone.*"
                ),
                color=discord.Color.gold()
            )
            await inter.response.edit_message(embed=embed, view=JobPromoteConfirmView(uid, k))
        btn.callback = promote_cb; view_instance.add_item(btn)

    back_row = job_row + (2 if promote_keys else 1)
    back = Button(label="◀️ Back to Jobs", style=discord.ButtonStyle.gray, row=min(back_row, 4))
    async def back_cb(inter):
        if str(inter.user.id) != uid: return await inter.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        await inter.response.edit_message(embed=_job_menu_embed(get_player(uid)), view=JobMenuView(uid))
    back.callback = back_cb; view_instance.add_item(back)


class JobPromoteConfirmView(View):
    def __init__(self, user_id: str, job_key: str):
        super().__init__(timeout=30); self.user_id = user_id; self.job_key = job_key

    @discord.ui.button(label="✅ Confirm Promotion", style=discord.ButtonStyle.green, row=0)
    async def confirm_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p  = get_player(self.user_id)
        pj = JOB_PROMOTIONS[self.job_key]
        bj = JOBS[self.job_key]
        p.setdefault("job_promotions", {})[self.job_key] = True
        save_data()
        embed = discord.Embed(
            title=f"🎉 Promoted to {pj['emoji']} {pj['name']}!",
            description=f"**{bj['emoji']} {bj['name']}** has been upgraded to **{pj['emoji']} {pj['name']}**!\nNew pay range: **${pj['pay'][0]:,}–${pj['pay'][1]:,}**",
            color=discord.Color.gold()
        )
        embed.set_footer(text="Your job button now shows the promoted version ✨")
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.gray, row=0)
    async def cancel_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id)
        await interaction.response.edit_message(embed=_job_menu_embed(p), view=JobMenuView(self.user_id))


class SafeJobsView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=90); self.user_id = user_id
        _build_job_category_view(self, "safe", discord.ButtonStyle.green, job_row=0)


class RiskyJobsView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=90); self.user_id = user_id
        _build_job_category_view(self, "risky", discord.ButtonStyle.red, job_row=0)


class SkillJobsView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=90); self.user_id = user_id
        _build_job_category_view(self, "skill", discord.ButtonStyle.blurple, job_row=0)


# ================================================================
# VIEWS — THEME SELECTOR
# ================================================================
class ThemeView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=60)
        self.user_id = user_id
        p     = get_player(user_id)
        owned = owned_themes(p)
        opts  = [
            discord.SelectOption(
                label=f"{THEMES[k]['emoji']} {THEMES[k]['name']}",
                value=k,
                description="✅ Equipped" if (p.get("equipped", {}).get("theme") or "default").replace("theme_", "") == k else "Owned",
                default=(p.get("equipped", {}).get("theme") or "default").replace("theme_", "") == k
            )
            for k in owned
        ]
        if not opts:
            opts = [discord.SelectOption(label="🎨 Default", value="default", description="Owned")]
        select = discord.ui.Select(placeholder="Choose a theme to equip...", options=opts[:25])
        async def cb(inter):
            if str(inter.user.id) != self.user_id:
                return await inter.response.send_message("❌ Open your own menu.", ephemeral=True)
            chosen = select.values[0]
            pp = get_player(self.user_id)
            pp["equipped"]["theme"] = f"theme_{chosen}" if chosen != "default" else None
            save_data()
            t = THEMES[chosen]
            embed = discord.Embed(
                title=f"{t['emoji']} Theme Changed!",
                description=f"Equipped **{t['name']}** theme.\nAll your menus now use this color!",
                color=discord.Color.from_rgb(*t["color"])
            )
            embed.set_footer(text="Visit /shop to buy more themes")
            await inter.response.edit_message(embed=embed, view=ThemeView(self.user_id))
        select.callback = cb
        self.add_item(select)
        back = Button(label="◀️ Back", style=discord.ButtonStyle.gray, row=1)
        async def back_cb(inter):
            if str(inter.user.id) != self.user_id:
                return await inter.response.send_message("❌ Open your own menu.", ephemeral=True)
            pp = get_player(self.user_id)
            await inter.response.edit_message(embed=_main_menu_embed(inter.user, pp), view=MainMenuView(self.user_id))
        back.callback = back_cb
        self.add_item(back)

# ================================================================
# VIEWS — ECONOMY MENU
# ================================================================
async def _execute_crime(interaction: discord.Interaction, uid: str, crime_key: str):
    """Shared crime execution used by both the panel and /crime command."""
    p       = get_player(uid, interaction.user.display_name)
    ct      = CRIME_TYPES[crime_key]
    t_perks = _territory_perks_for_player(uid)

    jail_rem = is_jailed(p)
    if jail_rem > 0:
        return await interaction.response.send_message(f"⛓️ You're in **JAIL**! Released in `{fmt_cd(jail_rem)}`.", ephemeral=True)
    rem = cd_remaining(p, "crime", CRIME_COOLDOWN)
    if rem > 0:
        return await interaction.response.send_message(f"⏳ Crime cooldown: `{fmt_cd(rem)}`", ephemeral=True)
    energy_key = "crime"
    eng_cost = ct["energy"]
    get_energy(p)
    if p["energy"] < eng_cost:
        return await interaction.response.send_message(f"⚡ Need **{eng_cost}** energy for this crime. You have **{p['energy']}**.", ephemeral=True)
    p["energy"] -= eng_cost

    luck_used  = "luck_charm" in p.get("items", [])
    luck_bonus = 0.15 if luck_used else 0.0
    rank       = _crime_rank(p)
    rank_bonus = rank["bonus"]
    terr_bonus = 0.10 if "crime_10" in t_perks else (0.05 if "crime_5" in t_perks else 0.0)
    crime_bonus = 0.20 if "crime_20" in t_perks else terr_bonus

    base_catch   = ct["catch"]
    spree_bonus  = 0.50 if event_active("crime_spree") else 0.0
    crime_train  = p.get("training", {}).get("crime", 0) * TRAINING_STATS["crime"]["bonus"]
    catch = max(0.02, base_catch * (1.0 - spree_bonus) - luck_bonus - rank_bonus - crime_bonus - crime_train)

    if luck_used: p["items"].remove("luck_charm")

    p["cooldowns"]["crime"] = time.time() - CRIME_COOLDOWN * (1.0 - _career_crime_cd_mult(p))
    p["stats"]["crimes_done"] = p["stats"].get("crimes_done", 0) + 1

    caught = random.random() < catch

    heat_reduction = 0
    if "crime_heat_3" in t_perks:   heat_reduction = 3
    elif "crime_heat_2" in t_perks: heat_reduction = 2
    elif "crime_heat_1" in t_perks: heat_reduction = 1
    heat_gain = max(0, int((ct["heat"] - heat_reduction) * _career_heat_mult(p)))
    p["heat"] = min(10, p.get("heat", 0) + heat_gain)

    if caught:
        # Don't commit fine/jail yet — show escape mini-game
        p["stats"]["crimes_done"] = p["stats"].get("crimes_done", 0)  # already incremented
        p["crime_streak"] = 0
        save_data()
        fine_base = random.randint(int(ct["reward"][0] * 0.5), int(ct["reward"][0]))
        jail_secs = int(jail_duration(p["heat"]) * _career_jail_mult(p))
        embed = discord.Embed(
            title="🚔 BUSTED!",
            description=(
                f"Cops caught you during **{ct['name']}**!\n\n"
                f"Potential fine: **${fine_base:,}**"
                + (f"\n⛓️ Potential jail: `{fmt_cd(jail_secs)}`" if jail_secs > 0 else "")
                + f"\n\n**What do you do?**"
            ),
            color=discord.Color.red()
        )
        embed.add_field(name="🌡️ Heat", value=f"`{heat_bar(p['heat'])}` {heat_label(p['heat'])}", inline=False)
        embed.set_footer(text="🏃 Run = 50% escape (+heat)  •  💵 Bribe = $500 guaranteed  •  🙏 Surrender = small fine, no jail")
        view = CrimeEscapeView(uid, fine_base, jail_secs)
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        return

    # Success
    streak_bonus = _crime_streak_bonus(p)
    rmin, rmax   = ct["reward"]
    haul = int(random.randint(rmin, rmax) * (1 + streak_bonus + rank_bonus))
    p["money"] += haul
    p["crime_streak"] = p.get("crime_streak", 0) + 1
    p["stats"]["wins"] = p["stats"].get("wins", 0) + 1
    p["stats"]["total_won"] = p["stats"].get("total_won", 0) + haul

    # Loot drop (10% chance)
    loot_drop = None
    if random.random() < 0.10:
        loot_drop = random.choice(CRIME_LOOT_POOL)
        p.setdefault("items", []).append(loot_drop)

    _weekly_inc(p, "crimes")
    new_ach = check_achievements(p)
    add_xp(p, 20); save_data()

    streak = p["crime_streak"]
    rank_new = _crime_rank(p)
    ranked_up = rank_new["name"] != rank["name"]

    SHOP_EMOJIS = {k: v.get("emoji", "🎁") for k, v in SHOP.items()}
    loot_names  = {k: v.get("name", k.replace("_", " ").title()) for k, v in SHOP.items()}

    embed = discord.Embed(
        title=f"✅ {ct['name']} — Success!",
        description=(
            ("🍀 Luck charm used! " if luck_used else "")
            + (f"🔥 **Streak x{streak}** (+{int(streak_bonus*100)}% bonus)" if streak > 1 else "")
            + (f"\n🎉 **RANK UP → {rank_new['emoji']} {rank_new['name']}!**" if ranked_up else "")
        ),
        color=discord.Color.green()
    )
    embed.add_field(name="💰 Haul",   value=f"**+${haul:,}**",                                    inline=True)
    embed.add_field(name="🌡️ Heat",   value=f"`{heat_bar(p['heat'])}` +{heat_gain}",               inline=True)
    embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**",                                inline=True)
    if loot_drop:
        embed.add_field(name="🎁 Loot Drop!", value=f"{SHOP_EMOJIS.get(loot_drop,'🎁')} **{loot_names.get(loot_drop, loot_drop).title()}**", inline=False)
    if new_ach:
        embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
    embed.set_footer(text=f"{rank_new['emoji']} {rank_new['name']}  •  {p['stats']['crimes_done']} crimes total")
    try:
        await interaction.response.edit_message(embed=embed, view=CrimeBackView(uid))
    except Exception:
        await interaction.response.send_message(embed=embed, ephemeral=True)


class CrimeBackView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id

    @discord.ui.button(label="🔫 Crime Again", style=discord.ButtonStyle.red, row=0)
    async def again_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        p["uid_ref"] = self.user_id
        await interaction.response.edit_message(embed=_crime_menu_embed(p), view=CrimeMenuView(self.user_id))

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=0)
    async def back_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        c_cd = fmt_cd(cd_remaining(p, "crime", CRIME_COOLDOWN))
        embed = discord.Embed(title="💼 Economy", description=f"Wallet: **${p['money']:,}**  •  Bank: **${p['bank']:,}**", color=discord.Color.blurple())
        embed.add_field(name="💼 Jobs",   value="9 jobs across 3 tiers — Safe, Risky & Skill", inline=False)
        embed.add_field(name="🔫 Crime",  value=f"5 crime types  •  `{c_cd}`\nHeat: `{heat_bar(p.get('heat',0))}` {heat_label(p.get('heat',0))}", inline=False)
        embed.add_field(name="🏦 Bank",   value="Deposit & withdraw safely", inline=False)
        embed.add_field(name="🛒 Shop",   value="Buy items & upgrades", inline=False)
        await interaction.response.edit_message(embed=embed, view=EconomyMenuView(self.user_id))


class CrimeEscapeView(View):
    def __init__(self, user_id, fine: int, jail_secs: int):
        super().__init__(timeout=30)
        self.user_id   = user_id
        self.fine      = fine
        self.jail_secs = jail_secs

    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.button(label="🏃 Run", style=discord.ButtonStyle.danger, row=0)
    async def run_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        if random.random() < 0.50:
            # Escaped
            p["heat"] = min(10, p.get("heat", 0) + 1)  # extra heat for running
            save_data()
            embed = discord.Embed(title="🏃 Escaped!", description="You outran the cops!", color=discord.Color.orange())
            embed.add_field(name="🌡️ Heat", value=f"`{heat_bar(p['heat'])}` +1 extra", inline=True)
            embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**", inline=True)
        else:
            # Caught harder
            fine = int(self.fine * 1.5)
            fine = min(fine, p["money"]); p["money"] -= fine
            if self.jail_secs > 0:
                p["jailed_until"] = time.time() + int(self.jail_secs * 1.5)
            save_data()
            embed = discord.Embed(title="🚔 Tackled!", description="They caught you mid-sprint — worse penalty!", color=discord.Color.dark_red())
            embed.add_field(name="💸 Fine",   value=f"-**${fine:,}**",    inline=True)
            if self.jail_secs > 0: embed.add_field(name="⛓️ Jailed", value=f"`{fmt_cd(int(self.jail_secs*1.5))}`", inline=True)
            embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**", inline=True)
        await interaction.response.edit_message(embed=embed, view=CrimeBackView(self.user_id))

    @discord.ui.button(label="💵 Bribe ($500)", style=discord.ButtonStyle.green, row=0)
    async def bribe_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        bribe = 500
        if p["money"] < bribe:
            return await interaction.response.send_message(f"❌ Need **$500** to bribe. You only have **${p['money']:,}**.", ephemeral=True)
        p["money"] -= bribe
        p["heat"] = max(0, p.get("heat", 0) - 1)  # heat reduced for a clean bribe
        save_data()
        embed = discord.Embed(title="💵 Bribed!", description="Officer pocketed the cash and looked away.", color=discord.Color.green())
        embed.add_field(name="💸 Paid",   value=f"-**$500**",              inline=True)
        embed.add_field(name="🌡️ Heat",   value=f"`{heat_bar(p['heat'])}` -1", inline=True)
        embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**",    inline=True)
        await interaction.response.edit_message(embed=embed, view=CrimeBackView(self.user_id))

    @discord.ui.button(label="🙏 Surrender", style=discord.ButtonStyle.gray, row=0)
    async def surrender_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        fine = min(int(self.fine * 0.4), p["money"]); p["money"] -= fine
        save_data()
        embed = discord.Embed(title="🙏 Surrendered", description="You gave yourself up. Small fine, no jail.", color=discord.Color.blurple())
        embed.add_field(name="💸 Fine",   value=f"-**${fine:,}**",         inline=True)
        embed.add_field(name="🌡️ Heat",   value=f"`{heat_bar(p['heat'])}`", inline=True)
        embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**",    inline=True)
        await interaction.response.edit_message(embed=embed, view=CrimeBackView(self.user_id))

    async def on_timeout(self):
        # Auto-surrender on timeout
        p = get_player(self.user_id)
        fine = min(self.fine, p["money"]); p["money"] -= fine
        if self.jail_secs > 0: p["jailed_until"] = time.time() + self.jail_secs
        save_data()


class CrimeMenuView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id

    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.button(label="🏪 Petty Theft",  style=discord.ButtonStyle.gray,    row=0)
    async def petty_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        await _execute_crime(interaction, self.user_id, "petty")

    @discord.ui.button(label="👜 Pickpocket",   style=discord.ButtonStyle.gray,    row=0)
    async def pick_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        await _execute_crime(interaction, self.user_id, "pick")

    @discord.ui.button(label="💻 Cyber Hack",   style=discord.ButtonStyle.blurple, row=0)
    async def hack_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        await _execute_crime(interaction, self.user_id, "hack")

    @discord.ui.button(label="🏦 Bank Job",     style=discord.ButtonStyle.danger,  row=1)
    async def bank_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        await _execute_crime(interaction, self.user_id, "bank")

    @discord.ui.button(label="💎 Jewelry Heist", style=discord.ButtonStyle.danger, row=1)
    async def jewelry_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        await _execute_crime(interaction, self.user_id, "jewelry")

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=2)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        c_cd = fmt_cd(cd_remaining(p, "crime", CRIME_COOLDOWN))
        embed = discord.Embed(title="💼 Economy", description=f"Wallet: **${p['money']:,}**  •  Bank: **${p['bank']:,}**", color=discord.Color.blurple())
        embed.add_field(name="💼 Jobs",   value="9 jobs across 3 tiers — Safe, Risky & Skill", inline=False)
        embed.add_field(name="🔫 Crime",  value=f"5 crime types  •  `{c_cd}`\nHeat: `{heat_bar(p.get('heat',0))}` {heat_label(p.get('heat',0))}", inline=False)
        embed.add_field(name="🏦 Bank",   value="Deposit & withdraw safely", inline=False)
        embed.add_field(name="🛒 Shop",   value="Buy items & upgrades", inline=False)
        await interaction.response.edit_message(embed=embed, view=EconomyMenuView(self.user_id))


class EconomyMenuView(View):
    def __init__(self, user_id):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, interaction):
        return str(interaction.user.id) == self.user_id

    @discord.ui.button(label="💼 Jobs", style=discord.ButtonStyle.green, row=0)
    async def jobs_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_job_menu_embed(p), view=JobMenuView(self.user_id))

    @discord.ui.button(label="🔫 Crime", style=discord.ButtonStyle.red, row=0)
    async def crime_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        p["uid_ref"] = self.user_id
        await interaction.response.edit_message(embed=_crime_menu_embed(p), view=CrimeMenuView(self.user_id))

    @discord.ui.button(label="🏦 Bank", style=discord.ButtonStyle.blurple, row=0)
    async def bank_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        embed = discord.Embed(title="🏦 Your Bank", description=f"```\nWallet  ${p['money']:>10,}\nBank    ${p['bank']:>10,}\n```", color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, view=BankView(self.user_id), ephemeral=True)

    @discord.ui.button(label="🛒 Shop", style=discord.ButtonStyle.gray, row=1)
    async def shop_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.send_message(embed=_shop_embed(p), view=ShopView(self.user_id), ephemeral=True)

    @discord.ui.button(label="🏋️ Train", style=discord.ButtonStyle.danger, row=1)
    async def train_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.send_message(embed=_training_embed(p), view=TrainingView(self.user_id), ephemeral=True)

    @discord.ui.button(label="🏢 Business", style=discord.ButtonStyle.blurple, row=1)
    async def business_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        uid = self.user_id; p = get_player(uid, interaction.user.display_name)
        props    = p.get("properties", [])
        ready_ct = sum(1 for pr in props if _prop_income_ready(pr))
        income_6h = sum(int((PROPERTIES[pr["type"]]["income"][0] + PROPERTIES[pr["type"]]["income"][1]) / 2) for pr in props)
        embed = discord.Embed(
            title="🏢 Business Empire",
            description=(
                f"**{len(props)}/{MAX_PROPERTIES}** property slots used\n"
                f"💰 Avg income per cycle: **~${income_6h:,}** every 6h\n"
                + (f"✅ **{ready_ct}** propert{'ies' if ready_ct != 1 else 'y'} ready to collect!" if ready_ct else "⏳ No income ready yet.")
            ),
            color=discord.Color.blue()
        )
        if not props:
            embed.add_field(name="📭 No Properties Yet", value="Hit **Buy Property** to start earning passive income!", inline=False)
        else:
            for i, prop in enumerate(props):
                info  = PROPERTIES[prop["type"]]
                ready = _prop_income_ready(prop)
                nxt   = int(prop.get("last_income", 0) + info["cd"])
                embed.add_field(name=f"{info['emoji']} {info['name']} #{i+1}", value=("✅ **Ready!**" if ready else f"⏳ <t:{nxt}:R>"), inline=True)
        embed.set_footer(text="Buy up to 5 properties  •  Raid rivals to steal their income")
        await interaction.response.send_message(embed=embed, view=PropertyView(uid), ephemeral=True)

    @discord.ui.button(label="🎯 Career", style=discord.ButtonStyle.blurple, row=2)
    async def career_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.send_message(embed=_career_embed(p), view=CareerView(self.user_id), ephemeral=True)

    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.gray, row=2)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        await _back_to_main(interaction, self.user_id)

# ================================================================
# VIEWS — MAIN MENU
# ================================================================
# Helper: run steal logic (shared by slash command and button flow)
async def _execute_steal(interaction: discord.Interaction, uid: str, uid2: str, member, amount_raw: str, thief_name: str):
    if event_active("immunity"):
        left = event_time_left()
        return await interaction.response.send_message(
            f"🛡️ **Immunity Window** is active — steals are blocked for `{fmt_cd(left)}`!", ephemeral=True)
    p  = get_player(uid,  thief_name)
    t2 = get_player(uid2, member.display_name)
    max_steal = max(10, int(t2["money"] * 0.30))
    steal_amt = t2["money"] if amount_raw == "all" else (int(amount_raw) if amount_raw.isdigit() else -1)
    if steal_amt <= 0: return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
    steal_amt = min(steal_amt, max_steal)
    mask_used   = "mask"   in p.get("items", [])
    gloves_used = "gloves" in p.get("items", [])
    revenge     = p.get("flags", {}).get("active_revenge") == uid2
    chance      = min(0.95, steal_chance(p, t2, mask_used, gloves_used, revenge) + _gang_steal_bonus(uid) + _career_steal_bonus(p) / 100)
    p["cooldowns"]["steal"] = time.time(); p["cooldowns"][f"steal_target_{uid2}"] = time.time()
    if mask_used:   p["items"].remove("mask")
    if gloves_used: p["items"].remove("gloves")
    if revenge:     p.get("flags", {}).pop("active_revenge", None)
    roll = random.random(); escape_threshold = chance + (0.35 if gloves_used else 0.20)
    if roll < chance:
        actual = min(steal_amt, t2["money"]); t2["money"] -= actual; p["money"] += actual
        heat_gain = max(0, int((1 if mask_used else 2) * _career_heat_mult(p))); p["heat"] = min(10, p.get("heat", 0) + heat_gain)
        t2.setdefault("flags", {})["stolen_from_by"] = uid
        p["stats"]["wins"] = p["stats"].get("wins", 0) + 1
        p["stats"]["total_won"] = p["stats"].get("total_won", 0) + actual
        add_xp(p, 25)
        # ── Bounty claim ──────────────────────────────────────────────
        bounty_award = 0
        if _bounty_total(uid2) > 0:
            b = bounties.pop(uid2, None)
            if b:
                bounty_award = b.get("total", 0)
                p["money"] += bounty_award
                delete_bounty_db(uid2)
                p["stats"]["bounties_claimed"] = p["stats"].get("bounties_claimed", 0) + 1
        p["stats"]["wins"] = p["stats"].get("wins", 0)  # ensure key present for ach check
        _weekly_inc(p, "steal_amt", actual)
        new_ach = check_achievements(p)
        save_data()
        embed = discord.Embed(title="🕵️ Steal — SUCCESS!", color=discord.Color.green())
        embed.add_field(name="🎯 Target",  value=member.display_name,   inline=True)
        embed.add_field(name="💰 Stolen",  value=f"**+${actual:,}**",   inline=True)
        embed.add_field(name="📊 Chance",  value=f"{int(chance*100)}%", inline=True)
        embed.add_field(name="🌡️ Heat",    value=f"`{heat_bar(p['heat'])}` +{heat_gain}", inline=False)
        embed.add_field(name="💵 Wallet",  value=f"**${p['money']:,}**", inline=True)
        if mask_used:    embed.add_field(name="🥷 Mask",    value="Heat reduced!",      inline=True)
        if revenge:      embed.add_field(name="🔥 Revenge", value="+20% applied!",      inline=True)
        if bounty_award: embed.add_field(name="🎯 BOUNTY",  value=f"**+${bounty_award:,}** claimed! 🎉", inline=False)
        if new_ach:      embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        public_msg = f"🚨 <@{uid2}> — **{thief_name}** just stole **${actual:,}** from your wallet! Use `/revenge` to fight back."
        if bounty_award:
            public_msg += f"\n🎯 **{thief_name}** also claimed a bounty of **${bounty_award:,}**!"
        try: await interaction.channel.send(public_msg, delete_after=60)
        except Exception: pass
    elif roll < escape_threshold:
        loss = min(random.randint(50, 150), p["money"]); p["money"] -= loss
        p["heat"] = min(10, p.get("heat", 0) + max(0, int(2 * _career_heat_mult(p)))); add_xp(p, 5); save_data()
        embed = discord.Embed(title="💨 Steal — ESCAPED!", color=discord.Color.orange())
        embed.add_field(name="🎯 Target",  value=member.display_name,   inline=True)
        embed.add_field(name="📊 Chance",  value=f"{int(chance*100)}%", inline=True)
        embed.add_field(name="💸 Lost",    value=f"-**${loss:,}**",      inline=True)
        embed.add_field(name="🌡️ Heat",    value=f"`{heat_bar(p['heat'])}` +2", inline=False)
        embed.add_field(name="💵 Wallet",  value=f"**${p['money']:,}**", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        try: await interaction.channel.send(f"⚠️ <@{uid2}> — **{thief_name}** tried to steal from you but escaped!", delete_after=60)
        except Exception: pass
    else:
        loss = min(random.randint(200, 500), p["money"]); p["money"] -= loss
        p["heat"] = min(10, p.get("heat", 0) + max(0, int(2 * _career_heat_mult(p))))
        jail_secs  = int(jail_duration(p["heat"]) * _career_jail_mult(p))
        jailed_now = jail_secs > 0
        if jailed_now: p["jailed_until"] = time.time() + jail_secs
        add_xp(p, 5); save_data()
        embed = discord.Embed(title="🚔 Steal — CAUGHT!", color=discord.Color.red())
        embed.add_field(name="🎯 Target",  value=member.display_name,   inline=True)
        embed.add_field(name="📊 Chance",  value=f"{int(chance*100)}%", inline=True)
        embed.add_field(name="💸 Fine",    value=f"-**${loss:,}**",      inline=True)
        embed.add_field(name="🌡️ Heat",    value=f"`{heat_bar(p['heat'])}` +2", inline=False)
        if jailed_now: embed.add_field(name="⛓️ JAILED", value=f"Released in `{fmt_cd(jail_secs)}`", inline=False)
        embed.add_field(name="💵 Wallet",  value=f"**${p['money']:,}**", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        try: await interaction.channel.send(f"🚔 <@{uid2}> — **{thief_name}** tried to steal from you and got caught by the police!", delete_after=60)
        except Exception: pass


# Amount modals (shown after target is selected from the list)
class StealAmountModal(Modal, title="🕵️ Steal — Enter Amount"):
    amount = TextInput(label="Amount (number or 'all')", placeholder="e.g. 200 or all")
    def __init__(self, user_id, uid2, target_name, member):
        super().__init__(); self.user_id = user_id; self.uid2 = uid2
        self.target_name = target_name; self.member = member
    async def on_submit(self, interaction: discord.Interaction):
        uid = self.user_id; p = get_player(uid, interaction.user.display_name)
        if uid == self.uid2:  return await interaction.response.send_message("❌ Can't steal from yourself.", ephemeral=True)
        jail_rem = is_jailed(p)
        if jail_rem > 0: return await interaction.response.send_message(f"⛓️ You're in **JAIL**! Released in `{fmt_cd(jail_rem)}`.", ephemeral=True)
        if p["level"] < 3: return await interaction.response.send_message("❌ Need **Level 3** to steal.", ephemeral=True)
        rem = cd_remaining(p, "steal", STEAL_COOLDOWN)
        if rem > 0: return await interaction.response.send_message(f"⏳ Steal cooldown: `{fmt_cd(rem)}`", ephemeral=True)
        tgt_rem = cd_remaining(p, f"steal_target_{self.uid2}", TARGET_STEAL_COOLDOWN)
        if tgt_rem > 0: return await interaction.response.send_message(f"⏳ Recently targeted. Wait `{fmt_cd(tgt_rem)}`.", ephemeral=True)
        if "mask" not in p.get("items", []):
            return await interaction.response.send_message("❌ Need a **🥷 Mask** to steal! Buy one at `/shop`.", ephemeral=True)
        t2 = get_player(self.uid2, self.target_name)
        if t2["money"] <= 0: return await interaction.response.send_message("❌ That player has nothing in their wallet.", ephemeral=True)
        if not use_energy(p, "steal"):
            eng = get_energy(p)
            return await interaction.response.send_message(f"⚡ Not enough energy! **{eng}/{MAX_ENERGY}** (need **{ENERGY_COST['steal']}**).", ephemeral=True)
        await _execute_steal(interaction, uid, self.uid2, self.member, self.amount.value.strip().lower(), interaction.user.display_name)


class DuelAmountModal(Modal, title="⚔️ Duel — Enter Bet"):
    bet = TextInput(label="Bet amount (number or 'all')", placeholder="e.g. 500 or all")
    def __init__(self, user_id, uid2, member):
        super().__init__(); self.user_id = user_id; self.uid2 = uid2; self.member = member
    async def on_submit(self, interaction: discord.Interaction):
        uid = self.user_id; p = get_player(uid, interaction.user.display_name)
        bet_raw = self.bet.value.strip().lower()
        bet_amt = p["money"] if bet_raw == "all" else (int(bet_raw) if bet_raw.isdigit() else -1)
        if bet_amt <= 0:         return await interaction.response.send_message("❌ Invalid bet.", ephemeral=True)
        if bet_amt < 10:         return await interaction.response.send_message("❌ Min bet is **$10**.", ephemeral=True)
        if bet_amt > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}** in wallet.", ephemeral=True)
        p["money"] -= bet_amt; save_data()
        did = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        duels[did] = {"challenger": uid, "challenged": self.uid2, "bet": bet_amt, "challenger_move": None,
                      "challenged_move": None, "accepted": False, "resolved": False, "channel_id": interaction.channel_id}
        embed = discord.Embed(title="⚔️ Duel Challenge!",
            description=f"<@{uid}> challenges <@{self.uid2}> to a **strategic duel**!\n💰 Pot: **${bet_amt*2:,}** — winner takes all!",
            color=discord.Color.orange())
        embed.add_field(name="⚔️ Attack",  value="Beats 🛡️ Defend",  inline=True)
        embed.add_field(name="🛡️ Defend",  value="Beats 🌀 Special", inline=True)
        embed.add_field(name="🌀 Special", value="Beats ⚔️ Attack",  inline=True)
        embed.set_footer(text="⏳ Expires in 2 minutes")
        await interaction.response.send_message(embed=embed, view=DuelChallengeView(did))
        msg = await interaction.original_response()
        duels[did]["msg_id"] = msg.id
        await interaction.followup.send("🤫 Your move (secret — pick now!):", view=DuelPickView(did, "challenger"), ephemeral=True)


# Target picker views (UserSelect — shows Discord's native player list)
def _steal_menu_embed(p: dict) -> discord.Embed:
    s_cd = fmt_cd(cd_remaining(p, "steal", STEAL_COOLDOWN))
    jail_rem = is_jailed(p)
    embed = discord.Embed(title="🥷 Steal System", description=f"Wallet: **${p['money']:,}**  •  Heat: `{heat_bar(p.get('heat',0))}` {heat_label(p.get('heat',0))}", color=discord.Color.red())
    if jail_rem: embed.add_field(name="⛓️ JAILED", value=f"Released in `{fmt_cd(jail_rem)}`", inline=False)
    embed.add_field(name="🕵️ /steal @target <amount>", value=f"Steal up to 30% of target's wallet  •  `{s_cd}`\nRequires Lv3+, 20 energy", inline=False)
    embed.add_field(name="🔍 /scan @target",            value="Preview your success chance for free",                         inline=False)
    embed.add_field(name="🔥 /revenge @target",         value="+20% steal bonus vs. the last person who robbed you",          inline=False)
    embed.add_field(name="⚔️ /duel @user <bet>",        value="Strategic PvP — Attack / Defend / Special — winner takes pot", inline=False)
    has_mask = "mask" in p.get("items", []); has_gloves = "gloves" in p.get("items", []); has_bail = "bail_bond" in p.get("items", [])
    if has_mask or has_gloves or has_bail:
        embed.add_field(name="🎒 Items", value=("🥷 Mask  " if has_mask else "") + ("🧤 Gloves  " if has_gloves else "") + ("⛓️ Bail Bond" if has_bail else ""), inline=False)
    return embed


class StealTargetView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id
    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="🕵️ Choose a target to steal from...", min_values=1, max_values=1)
    async def target_select(self, interaction, select):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        member  = select.values[0]
        uid2    = str(member.id)
        p       = get_player(self.user_id, interaction.user.display_name)
        t2      = get_player(uid2, member.display_name)
        bounty  = _bounty_total(uid2)
        chance  = steal_chance(p, t2, "mask" in p.get("items",[]), False, False)
        max_amt = max(10, int(t2["money"] * 0.30))

        embed = discord.Embed(
            title=f"🕵️ Target: {member.display_name}",
            color=discord.Color.red() if bounty > 0 else discord.Color.orange()
        )
        embed.add_field(name="💵 Wallet",      value=f"**${t2['money']:,}**",          inline=True)
        embed.add_field(name="📊 Steal Chance", value=f"**{int(chance*100)}%**",        inline=True)
        embed.add_field(name="💰 Max Steal",    value=f"**${max_amt:,}**",              inline=True)
        if bounty > 0:
            embed.add_field(
                name="🎯 BOUNTY ACTIVE",
                value=f"**${bounty:,}** will be added to your wallet if you succeed!",
                inline=False
            )
        embed.set_footer(text="Press Steal to enter the amount  •  Requires 🥷 Mask + 20 energy")
        await interaction.response.edit_message(
            embed=embed,
            view=StealConfirmView(self.user_id, uid2, member.display_name, member)
        )

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_steal_menu_embed(p), view=StealMenuView(self.user_id))


class StealConfirmView(View):
    """Shown after target is selected — confirms target info + bounty, then opens the amount modal."""
    def __init__(self, user_id, uid2, target_name, member):
        super().__init__(timeout=60)
        self.user_id     = user_id
        self.uid2        = uid2
        self.target_name = target_name
        self.member      = member

    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.button(label="🕵️ Steal — Enter Amount", style=discord.ButtonStyle.danger, row=0)
    async def steal_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        await interaction.response.send_modal(
            StealAmountModal(self.user_id, self.uid2, self.target_name, self.member))

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=0)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_steal_menu_embed(p), view=StealMenuView(self.user_id))


class ScanTargetView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id
    def _ok(self, i): return str(i.user.id) == self.user_id
    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="🔍 Choose a player to scan...", min_values=1, max_values=1)
    async def target_select(self, interaction, select):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; p = get_player(uid, interaction.user.display_name)
        if p["level"] < 3: return await interaction.response.send_message("❌ Need **Level 3** to scan.", ephemeral=True)
        if "scanner" not in p.get("items", []):
            return await interaction.response.send_message("❌ Need a **🔭 Scanner** to scan! Buy one at `/shop`.", ephemeral=True)
        member = select.values[0]; uid2 = str(member.id)
        if uid == uid2: return await interaction.response.send_message("❌ Can't scan yourself.", ephemeral=True)
        t2 = get_player(uid2, member.display_name)
        revenge = p.get("flags", {}).get("active_revenge") == uid2
        base_c = steal_chance(p, t2, False, False, revenge)
        mask_c = steal_chance(p, t2, True, False, revenge)
        gloves_c = steal_chance(p, t2, False, True, revenge)
        shield = "insurance_shield" in t2.get("items", [])
        has_mask = "mask" in p.get("items", []); has_gloves = "gloves" in p.get("items", [])
        p["items"].remove("scanner"); save_data()
        embed = discord.Embed(title=f"🔍 Scan — {member.display_name}", color=discord.Color.blurple())
        embed.add_field(name="💵 Wallet",     value=f"**${t2['money']:,}**" if t2["money"] > 0 else "**$0**", inline=True)
        embed.add_field(name="⭐ Level",       value=f"**{t2['level']}**",                                     inline=True)
        embed.add_field(name="🛡️ Shield",     value="⚠️ Active" if shield else "None",                        inline=True)
        embed.add_field(name="🎲 Base Chance", value=f"**{int(base_c*100)}%**",                                inline=True)
        if has_mask:   embed.add_field(name="🥷 With Mask",   value=f"**{int(mask_c*100)}%**",   inline=True)
        if has_gloves: embed.add_field(name="🧤 With Gloves", value=f"**{int(gloves_c*100)}%**", inline=True)
        if revenge:    embed.add_field(name="🔥 Revenge",     value="+20% ✅",                    inline=True)
        embed.add_field(name="💰 Max Steal",  value=f"**${max(10, int(t2['money']*0.30)):,}**",  inline=True)
        bounty = _bounty_total(uid2)
        if bounty > 0:
            embed.add_field(name="🎯 BOUNTY",  value=f"**${bounty:,}** — steal from them to claim it!",  inline=False)
        embed.set_footer(text="🔭 Scanner used — steal costs 20 energy + 🥷 Mask + 15min cooldown")
        await interaction.response.edit_message(embed=embed, view=ScanBackView(self.user_id))
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_steal_menu_embed(p), view=StealMenuView(self.user_id))


class ScanBackView(View):
    def __init__(self, user_id): super().__init__(timeout=60); self.user_id = user_id
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray)
    async def back_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_steal_menu_embed(p), view=StealMenuView(self.user_id))


class RevengeTargetView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id
    def _ok(self, i): return str(i.user.id) == self.user_id
    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="🔥 Choose the player who robbed you...", min_values=1, max_values=1)
    async def target_select(self, interaction, select):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; p = get_player(uid, interaction.user.display_name)
        member = select.values[0]; uid2 = str(member.id)
        if uid == uid2: return await interaction.response.send_message("❌ Can't revenge yourself.", ephemeral=True)
        stolen_from = p.get("flags", {}).get("stolen_from_by")
        if not stolen_from: return await interaction.response.send_message("❌ Nobody has stolen from you recently.", ephemeral=True)
        if stolen_from != uid2: return await interaction.response.send_message(f"❌ **{member.display_name}** hasn't stolen from you.", ephemeral=True)
        if "tracker" not in p.get("items", []):
            return await interaction.response.send_message("❌ Need a **📡 Tracker** to activate revenge! Buy one at `/shop`.", ephemeral=True)
        p["items"].remove("tracker")
        p.setdefault("flags", {})["active_revenge"] = uid2; p["flags"].pop("stolen_from_by", None); save_data()
        embed = discord.Embed(title="🔥 Revenge Activated!", description=f"Your next steal against **{member.display_name}** gets **+20% success**!", color=discord.Color.red())
        embed.set_footer(text="📡 Tracker used — bonus consumed on your next steal against them")
        await interaction.response.edit_message(embed=embed, view=ScanBackView(self.user_id))
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_steal_menu_embed(p), view=StealMenuView(self.user_id))


class DuelTargetView(View):
    def __init__(self, user_id):
        super().__init__(timeout=60); self.user_id = user_id
    def _ok(self, i): return str(i.user.id) == self.user_id
    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="⚔️ Choose a player to duel...", min_values=1, max_values=1)
    async def target_select(self, interaction, select):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; member = select.values[0]; uid2 = str(member.id)
        if uid == uid2: return await interaction.response.send_message("❌ Can't duel yourself.", ephemeral=True)
        if member.bot:  return await interaction.response.send_message("❌ Can't duel a bot.", ephemeral=True)
        await interaction.response.send_modal(DuelAmountModal(self.user_id, uid2, member))
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_steal_menu_embed(p), view=StealMenuView(self.user_id))


class StealMenuView(View):
    def __init__(self, user_id):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, interaction):
        return str(interaction.user.id) == self.user_id

    @discord.ui.button(label="🕵️ Steal", style=discord.ButtonStyle.danger, row=0)
    async def steal_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        if "mask" not in p.get("items", []):
            return await interaction.response.send_message("❌ Need a **🥷 Mask** to steal! Buy one at `/shop`.", ephemeral=True)
        await interaction.response.edit_message(embed=discord.Embed(title="🕵️ Steal — Pick Target", description="Select a player to steal from:", color=discord.Color.red()), view=StealTargetView(self.user_id))

    @discord.ui.button(label="🔍 Scan", style=discord.ButtonStyle.blurple, row=0)
    async def scan_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        if "scanner" not in p.get("items", []):
            return await interaction.response.send_message("❌ Need a **🔭 Scanner** to scan! Buy one at `/shop`.", ephemeral=True)
        await interaction.response.edit_message(embed=discord.Embed(title="🔍 Scan — Pick Target", description="Select a player to scan:", color=discord.Color.blurple()), view=ScanTargetView(self.user_id))

    @discord.ui.button(label="🔥 Revenge", style=discord.ButtonStyle.danger, row=1)
    async def revenge_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        thief_uid = p.get("flags", {}).get("stolen_from_by")
        if not thief_uid: return await interaction.response.send_message("❌ Nobody has stolen from you recently.", ephemeral=True)
        if "tracker" not in p.get("items", []):
            return await interaction.response.send_message("❌ Need a **📡 Tracker** to activate revenge! Buy one at `/shop`.", ephemeral=True)
        await interaction.response.edit_message(embed=discord.Embed(title="🔥 Revenge — Pick Target", description="Select the player who robbed you:", color=discord.Color.red()), view=RevengeTargetView(self.user_id))

    @discord.ui.button(label="⚔️ Duel", style=discord.ButtonStyle.blurple, row=1)
    async def duel_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        await interaction.response.edit_message(embed=discord.Embed(title="⚔️ Duel — Pick Opponent", description="Select a player to challenge:", color=discord.Color.orange()), view=DuelTargetView(self.user_id))

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=2)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


class MainMenuView(View):
    def __init__(self, user_id):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, interaction):
        return str(interaction.user.id) == self.user_id

    @discord.ui.button(label="🃏 Casino", style=discord.ButtonStyle.green, row=0)
    async def casino_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name); worth = p["money"] + p["bank"]
        lt_pot = lottery_state.get("pot", 0) + LOTTERY_SEED
        embed = discord.Embed(title="🎰 Casino", description=f"Wallet: **${p['money']:,}**  •  Net worth: **${worth:,}**\n🎰 Jackpot pool: **${jackpot_pool:,}**", color=discord.Color.dark_green())
        embed.add_field(name="🃏 Blackjack",      value="Beat the dealer to 21 — min $10",                    inline=False)
        embed.add_field(name="🎰 Slots",           value="Spin the reels — jackpot grows!",                    inline=False)
        embed.add_field(name="🎡 Roulette",        value="Live shared rounds — all bets resolved together",    inline=False)
        embed.add_field(name="🎲 Coin Flip",       value="50/50 — double or nothing",                          inline=False)
        embed.add_field(name="👥 Multiplayer",     value="Private table with friends",                          inline=False)
        embed.add_field(name="🎟️ Lottery",         value=f"Buy tickets — jackpot **${lt_pot:,}** • Drawing Sunday", inline=False)
        embed.add_field(name="🎫 Scratch Card",    value=f"Instant prizes — **${SCRATCH_PRICE:,}** per card",   inline=False)
        embed.add_field(name="💣 Mines",           value=f"Pick tiles, dodge bombs, cash out — min **${MINES_MIN_BET:,}**", inline=False)
        embed.add_field(name="🐔 Chicken Cross",   value=f"Cross lanes for multipliers, don't get squashed — min **${CHICKEN_MIN_BET:,}**", inline=False)
        await interaction.response.edit_message(embed=embed, view=CasinoMenuView(self.user_id))

    @discord.ui.button(label="💼 Economy", style=discord.ButtonStyle.blurple, row=0)
    async def economy_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p    = get_player(self.user_id, interaction.user.display_name)
        c_cd = fmt_cd(cd_remaining(p, "crime", CRIME_COOLDOWN))
        embed = discord.Embed(title="💼 Economy", description=f"Wallet: **${p['money']:,}**  •  Bank: **${p['bank']:,}**", color=discord.Color.blurple())
        props    = p.get("properties", [])
        ready_ct = sum(1 for pr in props if _prop_income_ready(pr))
        biz_val  = f"{len(props)}/{MAX_PROPERTIES} properties" + (f"  •  ✅ {ready_ct} ready!" if ready_ct else "")
        embed.add_field(name="💼 Jobs",    value="9 jobs across 3 tiers — Safe, Risky & Skill",                                  inline=False)
        embed.add_field(name="🔫 Crime",   value=f"Earn $200–700 (risky!)  •  `{c_cd}`\nHeat: `{heat_bar(p.get('heat',0))}` {heat_label(p.get('heat',0))}", inline=False)
        embed.add_field(name="🏦 Bank",    value="Deposit & withdraw safely",                                                     inline=False)
        embed.add_field(name="🛒 Shop",    value="Buy items & upgrades",                                                          inline=False)
        embed.add_field(name="🏢 Business", value=biz_val or "Buy properties for passive income every 6h",                       inline=False)
        tr = p.get("training", {}); total_lvls = sum(tr.get(k, 0) for k in TRAINING_STATS)
        embed.add_field(name="🏋️ Train",    value=f"{total_lvls}/{TRAINING_MAX * len(TRAINING_STATS)} levels — boost steal, crime, jobs & XP", inline=False)
        await interaction.response.edit_message(embed=embed, view=EconomyMenuView(self.user_id))

    @discord.ui.button(label="🏴 Gang", style=discord.ButtonStyle.blurple, row=0)
    async def gang_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if g:
            embed = _make_gang_embed(g, gid, interaction.guild)
        else:
            embed = discord.Embed(title="🏴 Gang System", description="You're not in a gang yet.", color=discord.Color.dark_gray())
            embed.add_field(name="📋 How to join", value="`/gang create` — Found a gang (Level 5+, $2,000)\n`/gang accept` — Accept a pending invite\n`/gang info <name>` — Search for a gang\n`/gang leaderboard` — View top gangs", inline=False)
            embed.add_field(name="⚡ Perks by Level", value="**Lv2:** +5% job pay\n**Lv3:** Gang Heist\n**Lv4:** +10% steal success\n**Lv5:** +15% all income + Gang War", inline=False)
        await interaction.response.edit_message(embed=embed, view=GangMenuView(uid))

    @discord.ui.button(label="🦹 Heist", style=discord.ButtonStyle.danger, row=1)
    async def steal_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        jail_rem = is_jailed(p)
        s_cd  = fmt_cd(cd_remaining(p, "steal", STEAL_COOLDOWN))
        embed = discord.Embed(title="🦹 Heist System", description=f"Wallet: **${p['money']:,}**  •  Heat: `{heat_bar(p.get('heat',0))}` {heat_label(p.get('heat',0))}", color=discord.Color.red())
        if jail_rem:
            embed.add_field(name="⛓️ JAILED", value=f"Released in `{fmt_cd(jail_rem)}`", inline=False)
        embed.add_field(name="🕵️ Steal", value=f"Steal up to 30% of target's wallet  •  `{s_cd}`\nRequires Lv3+, 20 energy + **🥷 Mask**", inline=False)
        embed.add_field(name="🔍 Scan",   value="Reveal target stats & steal chance\nRequires **🔭 Scanner** (consumed on use)",               inline=False)
        embed.add_field(name="🔥 Revenge", value="+20% steal bonus vs. the last person who robbed you\nRequires **📡 Tracker** (consumed on use)", inline=False)
        embed.add_field(name="⚔️ Duel",   value="Strategic PvP — Attack / Defend / Special — winner takes pot",                                inline=False)
        has_mask    = "mask"     in p.get("items", [])
        has_gloves  = "gloves"   in p.get("items", [])
        has_bail    = "bail_bond" in p.get("items", [])
        has_scanner = "scanner"  in p.get("items", [])
        has_tracker = "tracker"  in p.get("items", [])
        inv_parts = []
        if has_mask:    inv_parts.append("🥷 Mask")
        if has_gloves:  inv_parts.append("🧤 Gloves")
        if has_bail:    inv_parts.append("⛓️ Bail Bond")
        if has_scanner: inv_parts.append("🔭 Scanner")
        if has_tracker: inv_parts.append("📡 Tracker")
        if inv_parts:
            embed.add_field(name="🎒 Inventory", value="  ".join(inv_parts), inline=False)
        await interaction.response.edit_message(embed=embed, view=StealMenuView(self.user_id))

    @discord.ui.button(label="👤 Profile", style=discord.ButtonStyle.gray, row=1)
    async def profile_btn(self, interaction, button):
        get_player(str(interaction.user.id), interaction.user.display_name)
        await interaction.response.send_message(embed=private_profile_embed(interaction.user), ephemeral=True)

    @discord.ui.button(label="🏆 Leaderboard", style=discord.ButtonStyle.gray, row=1)
    async def lb_btn_row1(self, interaction, button):
        await interaction.response.send_message(embed=leaderboard_embed(_get_guild(interaction, self.user_id)), ephemeral=True)

    @discord.ui.button(label="📅 Daily", style=discord.ButtonStyle.green, row=2)
    async def daily_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name); now = time.time()
        rem = cd_remaining(p, "daily", 86400)
        if rem > 0: return await interaction.response.send_message(f"⏳ Daily cooldown: `{fmt_cd(rem)}`", ephemeral=True)
        last = p.get("last_daily", 0); hours_since = (now - last) / 3600
        p["streak"] = (p.get("streak", 0) + 1) if 20 <= hours_since <= 48 else 1
        streak = p["streak"]; bonus = 500 + p["level"] * 50 + (min(streak, 7) - 1) * 100
        p["money"] += bonus; p["heat"] = max(0, p.get("heat", 0) - 2)
        p["cooldowns"]["daily"] = now; p["last_daily"] = now; leveled = add_xp(p, 50 + streak * 5); save_data()
        embed = discord.Embed(title="📅 Daily Bonus!", color=discord.Color.gold())
        embed.add_field(name="💰 Bonus",  value=f"**+${bonus:,}**",                               inline=True)
        embed.add_field(name="🔥 Streak", value=f"**{streak} day{'s' if streak>1 else ''}**",     inline=True)
        if streak >= 7: embed.add_field(name="🎉 Max Streak!", value="Week streak bonus active!", inline=False)
        if leveled:     embed.add_field(name="⭐ Level Up!",   value=f"Now level **{p['level']}**!", inline=False)
        embed.set_footer(text="Come back tomorrow to keep your streak!")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="📋 Challenges", style=discord.ButtonStyle.blurple, row=2)
    async def challenges_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        _ensure_weekly(p)
        await interaction.response.send_message(embed=_weekly_embed(p), view=WeeklyChallengesView(self.user_id), ephemeral=True)

    @discord.ui.button(label="🎨 Themes", style=discord.ButtonStyle.gray, row=2)
    async def theme_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `!play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        owned = owned_themes(p)
        t_key = (p.get("equipped", {}).get("theme") or "default").replace("theme_", "")
        t = THEMES.get(t_key, THEMES["default"])
        embed = discord.Embed(
            title="🎨 Theme Selector",
            description=f"Current theme: **{t['emoji']} {t['name']}**\nBuy themes in `/shop` — then equip them here!",
            color=get_theme_color(p)
        )
        for k in owned:
            th = THEMES[k]
            active = "✅ Equipped" if t_key == k else "Owned"
            embed.add_field(name=f"{th['emoji']} {th['name']}", value=active, inline=True)
        await interaction.response.edit_message(embed=embed, view=ThemeView(self.user_id))

# ================================================================
# MINES GAME
# ================================================================
def _mines_mult(picks: int, mines: int) -> float:
    if picks == 0:
        return 1.0
    prob = 1.0
    total = MINES_TOTAL
    for i in range(picks):
        safe = total - mines - i
        left = total - i
        if left <= 0 or safe <= 0:
            break
        prob *= safe / left
    return round(0.97 / max(prob, 1e-9), 2)


def _mines_embed(state: dict) -> discord.Embed:
    picks      = len(state["revealed"])
    mines      = state["mines"]
    bet        = state["bet"]
    mult       = _mines_mult(picks, mines)
    payout     = int(bet * mult)
    safe_total = MINES_TOTAL - mines
    remaining  = MINES_TOTAL - picks
    risk_pct   = int(mines / remaining * 100) if remaining > 0 else 100
    embed = discord.Embed(
        title=f"💣 Mines  —  {picks}/{safe_total} safe tiles found",
        color=discord.Color.dark_green()
    )
    embed.add_field(name="💵 Bet",          value=f"**${bet:,}**",          inline=True)
    embed.add_field(name="💣 Mines hidden", value=f"**{mines}**",            inline=True)
    embed.add_field(name="📈 Multiplier",   value=f"**×{mult}**",            inline=True)
    embed.add_field(name="💰 Cash Out Now", value=f"**${payout:,}**",        inline=True)
    embed.add_field(name="⚠️ Next tile",    value=f"**{risk_pct}%** mine",   inline=True)
    embed.add_field(name="✅ Progress",     value=f"{picks}/{safe_total}",    inline=True)
    embed.set_footer(text="Pick tiles to grow multiplier — Cash Out before you hit a 💣!")
    return embed


# ================================================================
# CHICKEN CROSS GAME
# ================================================================
def _chicken_embed(state: dict) -> discord.Embed:
    bet     = state["bet"]
    lane    = state["lane"]
    diff    = state["difficulty"]
    done    = state["done"]
    crashed = state.get("crashed", False)

    mult   = CHICKEN_MULTS[lane - 1] if lane > 0 else 1.0
    payout = int(bet * mult) if lane > 0 else bet

    road = ""
    for i in range(CHICKEN_LANES):
        if i < lane:
            road += "✅"
        elif i == lane and not done:
            road += "🐔"
        else:
            road += "🟫"
    road_str = " ".join(road)

    if crashed:
        title = "💥 Squashed! Your chicken got run over!"
        color = discord.Color.red()
        desc  = (
            f"{road_str}\n\n"
            f"You crossed **{lane - 1}** lane(s) then got wiped on lane **{lane}**.\n"
            f"You lost **${bet:,}**."
        )
    elif done:
        title = "🏁 Cashed Out!"
        color = discord.Color.green()
        desc  = (
            f"{road_str}\n\n"
            f"You safely crossed **{lane}** lane(s) and cashed out!\n"
            f"You won **${payout:,}** (**{mult}×**)."
        )
    else:
        title = "🐔 Chicken Cross"
        color = discord.Color.orange()
        if lane == 0:
            desc = (
                f"{road_str}\n\n"
                f"**Bet:** ${bet:,}  •  **Difficulty:** {diff.title()}\n"
                f"Cross lanes to multiply your bet — cash out before the crash!"
            )
        else:
            next_mult   = CHICKEN_MULTS[lane] if lane < CHICKEN_LANES else None
            next_payout = int(bet * next_mult) if next_mult else None
            desc = (
                f"{road_str}\n\n"
                f"**Lane {lane}/{CHICKEN_LANES}** crossed!\n"
                f"**Now:** {mult}× = **${payout:,}**"
            )
            if next_mult:
                desc += f"\n**Next:** {next_mult}× = **${next_payout:,}**"

    embed = discord.Embed(title=title, description=desc, color=color)
    embed.set_footer(text=f"Crash chance per lane: {int(CHICKEN_CRASH_CHANCE[diff]*100)}%  •  Difficulty: {diff.title()}")
    return embed


class ChickenBetModal(discord.ui.Modal, title="🐔 Chicken Cross"):
    bet_in  = discord.ui.TextInput(label="Bet Amount",  placeholder=f"Min ${CHICKEN_MIN_BET}", max_length=10)
    diff_in = discord.ui.TextInput(label="Difficulty",  placeholder="easy / medium / hard",    max_length=10)

    def __init__(self, user_id: str):
        super().__init__(); self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        uid = self.user_id
        if str(interaction.user.id) != uid:
            return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
        try:
            bet = int(self.bet_in.value.strip())
        except ValueError:
            return await interaction.response.send_message("❌ Enter a valid number.", ephemeral=True)
        diff = self.diff_in.value.strip().lower()
        if diff not in CHICKEN_CRASH_CHANCE:
            return await interaction.response.send_message("❌ Difficulty must be `easy`, `medium`, or `hard`.", ephemeral=True)
        if bet < CHICKEN_MIN_BET:
            return await interaction.response.send_message(f"❌ Minimum bet is **${CHICKEN_MIN_BET:,}**.", ephemeral=True)
        p = get_player(uid, interaction.user.display_name)
        if p["money"] < bet:
            return await interaction.response.send_message(f"❌ You only have **${p['money']:,}**.", ephemeral=True)
        if uid in active_chicken:
            return await interaction.response.send_message("❌ You already have an active Chicken game! Finish it first.", ephemeral=True)
        p["money"] -= bet
        active_chicken[uid] = {"bet": bet, "difficulty": diff, "lane": 0, "done": False, "crashed": False}
        save_data()
        await interaction.response.send_message(
            embed=_chicken_embed(active_chicken[uid]),
            view=ChickenCrossView(uid),
            ephemeral=True
        )


class ChickenCrossView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=300)
        self.uid   = uid
        state      = active_chicken.get(uid, {})
        done       = state.get("done", False)
        lane       = state.get("lane", 0)
        # Keep last settings so the replay button can restart without a modal
        self.last_bet  = state.get("bet", 0)
        self.last_diff = state.get("difficulty", "easy")

        cashout_label = (
            f"💰 Cash Out ({CHICKEN_MULTS[lane - 1]}×)" if lane > 0
            else "💰 Cash Out"
        )
        cross_btn = discord.ui.Button(
            label="🐔 Cross Lane",
            style=discord.ButtonStyle.danger,
            disabled=done,
            row=0
        )
        cross_btn.callback = self._cross
        self.add_item(cross_btn)

        co_btn = discord.ui.Button(
            label=cashout_label,
            style=discord.ButtonStyle.green,
            disabled=(done or lane == 0),
            row=0
        )
        co_btn.callback = self._cashout
        self.add_item(co_btn)

        if done and self.last_bet > 0:
            replay_btn = discord.ui.Button(
                label=f"🔄 Play Again (${self.last_bet:,})",
                style=discord.ButtonStyle.blurple,
                row=1
            )
            replay_btn.callback = self._replay
            self.add_item(replay_btn)

    async def _cross(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your game!", ephemeral=True)
        state = active_chicken.get(self.uid)
        if not state or state.get("done"):
            return await interaction.response.send_message("❌ No active game.", ephemeral=True)
        await interaction.response.defer()
        diff = state["difficulty"]
        state["lane"] += 1
        lane = state["lane"]
        if random.random() < CHICKEN_CRASH_CHANCE[diff]:
            state["done"] = True; state["crashed"] = True
            view = ChickenCrossView(self.uid)
            active_chicken.pop(self.uid, None)
            await interaction.edit_original_response(embed=_chicken_embed(state), view=view)
        elif lane >= CHICKEN_LANES:
            p      = get_player(self.uid, interaction.user.display_name)
            mult   = CHICKEN_MULTS[lane - 1]
            payout = int(state["bet"] * mult)
            p["money"] += payout
            save_data()
            state["done"] = True
            view = ChickenCrossView(self.uid)
            active_chicken.pop(self.uid, None)
            await interaction.edit_original_response(embed=_chicken_embed(state), view=view)
        else:
            await interaction.edit_original_response(
                embed=_chicken_embed(state), view=ChickenCrossView(self.uid)
            )

    async def _cashout(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your game!", ephemeral=True)
        state = active_chicken.get(self.uid)
        if not state or state.get("done"):
            return await interaction.response.send_message("❌ No active game.", ephemeral=True)
        await interaction.response.defer()
        lane   = state["lane"]
        mult   = CHICKEN_MULTS[lane - 1]
        payout = int(state["bet"] * mult)
        p = get_player(self.uid, interaction.user.display_name)
        p["money"] += payout
        save_data()
        state["done"] = True
        view = ChickenCrossView(self.uid)
        active_chicken.pop(self.uid, None)
        await interaction.edit_original_response(embed=_chicken_embed(state), view=view)

    async def _replay(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your game!", ephemeral=True)
        if self.uid in active_chicken:
            return await interaction.response.send_message("❌ You already have an active game!", ephemeral=True)
        bet  = self.last_bet
        diff = self.last_diff
        p = get_player(self.uid, interaction.user.display_name)
        if p["money"] < bet:
            return await interaction.response.send_message(
                f"❌ Not enough money! Need **${bet:,}** but you have **${p['money']:,}**.", ephemeral=True
            )
        p["money"] -= bet
        active_chicken[self.uid] = {"bet": bet, "difficulty": diff, "lane": 0, "done": False, "crashed": False}
        save_data()
        await interaction.response.edit_message(
            embed=_chicken_embed(active_chicken[self.uid]),
            view=ChickenCrossView(self.uid)
        )


class MinesBetModal(discord.ui.Modal, title="💣 Mines — Start Game"):
    bet_input   = discord.ui.TextInput(
        label="Bet Amount (min $50)",
        placeholder="e.g. 500",
        min_length=1, max_length=9, required=True
    )
    mines_input = discord.ui.TextInput(
        label="Mines count:  2 / 3 / 5 / 8 / 14",
        placeholder="e.g. 3",
        min_length=1, max_length=2, required=True
    )

    def __init__(self, uid: str):
        super().__init__()
        self.uid = uid

    async def on_submit(self, interaction: discord.Interaction):
        uid = self.uid
        p   = get_player(uid)

        try:
            bet = int(self.bet_input.value.strip())
        except ValueError:
            return await interaction.response.send_message("❌ Invalid bet amount.", ephemeral=True)
        try:
            mines = int(self.mines_input.value.strip())
        except ValueError:
            return await interaction.response.send_message("❌ Invalid mine count.", ephemeral=True)

        if mines not in MINES_MINE_OPTS:
            return await interaction.response.send_message(
                f"❌ Mines must be: **{' / '.join(map(str, MINES_MINE_OPTS))}**", ephemeral=True)
        if bet < MINES_MIN_BET:
            return await interaction.response.send_message(
                f"❌ Minimum bet is **${MINES_MIN_BET:,}**.", ephemeral=True)
        if bet > p["money"]:
            return await interaction.response.send_message(
                f"❌ You only have **${p['money']:,}**.", ephemeral=True)
        if uid in active_mines:
            return await interaction.response.send_message(
                "❌ You already have an active Mines game! Finish it first.", ephemeral=True)

        p["money"] -= bet
        active_mines[uid] = {
            "bet":            bet,
            "mines":          mines,
            "mine_positions": set(random.sample(range(MINES_TOTAL), mines)),
            "revealed":       set(),
            "done":           False,
            "exploded_at":    None,
        }
        save_data()
        await interaction.response.send_message(
            embed=_mines_embed(active_mines[uid]),
            view=MinesGameView(uid),
            ephemeral=True
        )


class MinesGameView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=300)
        self.uid  = uid
        state     = active_mines.get(uid, {})
        done      = state.get("done", False)
        revealed  = state.get("revealed", set())
        mines_pos = state.get("mine_positions", set())
        exploded  = state.get("exploded_at")
        # Keep last settings for replay
        self.last_bet   = state.get("bet", 0)
        self.last_mines = state.get("mines", 3)

        for i in range(MINES_TOTAL):
            is_exploded = (i == exploded)
            is_mine     = done and (i in mines_pos) and not is_exploded
            is_revealed = i in revealed

            if is_exploded:
                label = "💥"; style = discord.ButtonStyle.danger;    dis = True
            elif is_mine:
                label = "💣"; style = discord.ButtonStyle.danger;    dis = True
            elif is_revealed:
                label = "💎"; style = discord.ButtonStyle.success;   dis = True
            else:
                label = "⬜"; style = discord.ButtonStyle.secondary; dis = done

            btn = discord.ui.Button(
                label=label, style=style,
                row=i // 5, custom_id=f"mt_{i}", disabled=dis
            )
            btn.callback = self._tile
            self.add_item(btn)

        picks  = len(revealed)
        mines  = state.get("mines", 3)
        bet    = state.get("bet", 0)
        mult   = _mines_mult(picks, mines)
        payout = int(bet * mult)

        co = discord.ui.Button(
            label=(f"💰 Cash Out  ×{mult}  (${payout:,})" if picks > 0
                   else "💰 Cash Out (open a tile first)"),
            style=discord.ButtonStyle.blurple,
            row=4, custom_id="mt_co",
            disabled=(picks == 0 or done)
        )
        co.callback = self._cashout
        self.add_item(co)

        if done and self.last_bet > 0:
            replay_btn = discord.ui.Button(
                label=f"🔄 Play Again (${self.last_bet:,} · {self.last_mines} mines)",
                style=discord.ButtonStyle.blurple,
                row=4, custom_id="mt_replay"
            )
            replay_btn.callback = self._replay
            self.add_item(replay_btn)

    async def _tile(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your game!", ephemeral=True)
        state = active_mines.get(self.uid)
        if not state or state.get("done"):
            return await interaction.response.send_message("❌ No active game.", ephemeral=True)

        idx = int(interaction.data["custom_id"].split("_")[1])
        if idx in state["revealed"]:
            return await interaction.response.send_message("❌ Already revealed.", ephemeral=True)

        await interaction.response.defer()

        if idx in state["mine_positions"]:
            state["done"] = True; state["exploded_at"] = idx
            p = get_player(self.uid)
            p["stats"]["losses"] = p["stats"].get("losses", 0) + 1
            save_data()

            embed = discord.Embed(title="💥 BOOM! You hit a mine!", color=discord.Color.red())
            embed.add_field(name="💸 Lost",    value=f"**-${state['bet']:,}**",      inline=True)
            embed.add_field(name="💣 Mines",   value=f"**{state['mines']}**",         inline=True)
            embed.add_field(name="✅ Found",   value=f"**{len(state['revealed'])}**", inline=True)
            embed.add_field(name="💵 Wallet",  value=f"**${p['money']:,}**",          inline=True)
            embed.set_footer(text="All mines revealed — try again!")

            view = MinesGameView(self.uid)
            active_mines.pop(self.uid, None)
            await interaction.edit_original_response(embed=embed, view=view)

        else:
            state["revealed"].add(idx)
            picks      = len(state["revealed"])
            safe_total = MINES_TOTAL - state["mines"]

            if picks >= safe_total:
                mult   = _mines_mult(picks, state["mines"])
                payout = int(state["bet"] * mult)
                state["done"] = True
                p = get_player(self.uid)
                p["money"] += payout
                p["stats"]["wins"] = p["stats"].get("wins", 0) + 1
                save_data()

                embed = discord.Embed(title="🏆 ALL tiles found — AUTO WIN!", color=discord.Color.gold())
                embed.add_field(name="💰 Payout",     value=f"**+${payout:,}**",  inline=True)
                embed.add_field(name="📈 Multiplier", value=f"**×{mult}**",         inline=True)
                embed.add_field(name="💵 Wallet",     value=f"**${p['money']:,}**", inline=True)
                embed.set_footer(text="Perfect run — no mines hit!")

                view = MinesGameView(self.uid)
                active_mines.pop(self.uid, None)
                await interaction.edit_original_response(embed=embed, view=view)
            else:
                await interaction.edit_original_response(
                    embed=_mines_embed(state),
                    view=MinesGameView(self.uid)
                )

    async def _cashout(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your game!", ephemeral=True)
        state = active_mines.get(self.uid)
        if not state or state.get("done"):
            return await interaction.response.send_message("❌ No active game.", ephemeral=True)

        picks = len(state["revealed"])
        if picks == 0:
            return await interaction.response.send_message("❌ Reveal at least 1 tile first!", ephemeral=True)

        state["done"] = True
        mult   = _mines_mult(picks, state["mines"])
        payout = int(state["bet"] * mult)

        p = get_player(self.uid)
        p["money"] += payout
        p["stats"]["wins"] = p["stats"].get("wins", 0) + 1
        save_data()

        embed = discord.Embed(title=f"💰 Cashed Out — ×{mult}", color=discord.Color.gold())
        embed.add_field(name="💵 Bet",        value=f"**${state['bet']:,}**",  inline=True)
        embed.add_field(name="📈 Multiplier", value=f"**×{mult}**",             inline=True)
        embed.add_field(name="💰 Payout",     value=f"**+${payout:,}**",        inline=True)
        embed.add_field(name="✅ Found",      value=f"**{picks}** tiles",        inline=True)
        embed.add_field(name="💣 Mines",      value=f"**{state['mines']}**",     inline=True)
        embed.add_field(name="💵 Wallet",     value=f"**${p['money']:,}**",      inline=True)
        embed.set_footer(text="Smart cashout! Mines are now revealed.")

        view = MinesGameView(self.uid)
        active_mines.pop(self.uid, None)
        await interaction.response.edit_message(embed=embed, view=view)

    async def _replay(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your game!", ephemeral=True)
        if self.uid in active_mines:
            return await interaction.response.send_message("❌ You already have an active Mines game!", ephemeral=True)
        bet   = self.last_bet
        mines = self.last_mines
        p = get_player(self.uid, interaction.user.display_name)
        if p["money"] < bet:
            return await interaction.response.send_message(
                f"❌ Not enough money! Need **${bet:,}** but you have **${p['money']:,}**.", ephemeral=True
            )
        p["money"] -= bet
        active_mines[self.uid] = {
            "bet":            bet,
            "mines":          mines,
            "mine_positions": set(random.sample(range(MINES_TOTAL), mines)),
            "revealed":       set(),
            "done":           False,
        }
        save_data()
        await interaction.response.edit_message(
            embed=_mines_embed(active_mines[self.uid]),
            view=MinesGameView(self.uid)
        )


# ================================================================
# SCRATCH CARD
# ================================================================
def _scratch_result(symbols: list) -> tuple[int, str]:
    """Returns (payout_multiplier, result_label). multiplier is applied to SCRATCH_PRICE."""
    counts = Counter(symbols)
    max_count = max(counts.values())
    if max_count == 3:
        return 10, "🎉 JACKPOT! Triple match!"
    if max_count == 2:
        return 2,  "✅ Double match! You win!"
    return 0, "❌ No match. Better luck next time!"


class ScratchCardView(View):
    def __init__(self, uid: str, symbols: list):
        super().__init__(timeout=60)
        self.uid     = uid
        self.symbols = symbols
        self.used    = False

    def _build_embed(self, revealed: int, result_label: str = "", payout: int = 0) -> discord.Embed:
        hidden = "❓"
        slots  = [self.symbols[i] if i < revealed else hidden for i in range(3)]
        display = "  ╎  ".join(f"**{s}**" for s in slots)

        if result_label:
            color = discord.Color.gold() if payout > 0 else discord.Color.dark_gray()
            embed = discord.Embed(title="🎫 Scratch Card — Result!", color=color)
            embed.description = f"## {display}\n\n{result_label}"
            if payout > 0:
                embed.add_field(name="💰 Payout", value=f"**+${payout:,}**", inline=True)
            else:
                embed.add_field(name="💸 Lost",   value=f"**-${SCRATCH_PRICE:,}**", inline=True)
            embed.set_footer(text="Buy more scratch cards at /shop!")
        else:
            embed = discord.Embed(
                title="🎫 Scratch Card",
                description=f"## {display}\n\nPress **Scratch!** to reveal your symbols.",
                color=discord.Color.blurple()
            )
            embed.set_footer(text=f"Cost: ${SCRATCH_PRICE:,}  •  Match 2 = 2× · Match 3 = 10×")
        return embed

    @discord.ui.button(label="✨ Scratch!", style=discord.ButtonStyle.green)
    async def scratch_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your card!", ephemeral=True)
        if self.used:
            return await interaction.response.send_message("❌ Already scratched!", ephemeral=True)
        self.used = True
        button.disabled = True

        await interaction.response.defer()

        # Animate: reveal one symbol at a time with 1s delay
        for i in range(1, 4):
            await interaction.edit_original_response(
                embed=self._build_embed(i),
                view=self
            )
            if i < 3:
                await asyncio.sleep(1.0)

        # Final result
        mult, label = _scratch_result(self.symbols)
        payout      = SCRATCH_PRICE * mult
        uid         = self.uid
        p           = get_player(uid)
        if payout > 0:
            p["money"] += payout
            p["stats"]["scratch_wins"] = p["stats"].get("scratch_wins", 0) + 1
        new_ach = check_achievements(p)
        save_data()

        self.clear_items()
        final_embed = self._build_embed(3, label, payout)
        if new_ach:
            final_embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
        await interaction.edit_original_response(embed=final_embed, view=self)


# ================================================================
# WEEKLY CHALLENGES PANEL
# ================================================================
class WeeklyChallengesView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=90)
        self.uid = uid
        p        = get_player(uid)
        _ensure_weekly(p)
        all_done = _weekly_all_done(p)
        claimed  = p["weekly"]["claimed"]
        if claimed:
            self.claim_btn.label    = "✅ Claimed"
            self.claim_btn.disabled = True
        elif all_done:
            self.claim_btn.label    = "🎁 Claim Reward!"
            self.claim_btn.disabled = False
        else:
            self.claim_btn.disabled = True

    @discord.ui.button(label="🎁 Claim Reward!", style=discord.ButtonStyle.green, row=0)
    async def claim_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p = get_player(self.uid)
        _ensure_weekly(p)
        if p["weekly"]["claimed"]:
            return await interaction.response.send_message("❌ Already claimed this week!", ephemeral=True)
        if not _weekly_all_done(p):
            return await interaction.response.send_message("❌ Not all challenges complete yet!", ephemeral=True)
        cash    = random.randint(*WEEKLY_CASH_REWARD_RANGE)
        p["money"] += cash
        leveled = add_xp(p, WEEKLY_XP_REWARD)
        p["weekly"]["claimed"] = True
        new_ach = check_achievements(p)
        save_data()
        embed = discord.Embed(
            title="🎉 Weekly Challenges — COMPLETE!",
            description="All 5 challenges done! Here's your reward:",
            color=discord.Color.gold()
        )
        embed.add_field(name="💰 Cash Reward", value=f"**+${cash:,}**",          inline=True)
        embed.add_field(name="⭐ XP Reward",   value=f"**+{WEEKLY_XP_REWARD}**", inline=True)
        if leveled:
            embed.add_field(name="🆙 Level Up!", value=f"Now **Level {p['level']}**!", inline=False)
        if new_ach:
            embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
        embed.set_footer(text="Challenges reset every Monday — come back next week!")
        button.label    = "✅ Claimed"
        button.disabled = True
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.gray, row=0)
    async def refresh_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p = get_player(self.uid)
        new_view = WeeklyChallengesView(self.uid)
        await interaction.response.edit_message(embed=_weekly_embed(p), view=new_view)

    @discord.ui.button(label="🔙 Close", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        await interaction.response.defer()
        await interaction.delete_original_response()


# ================================================================
# TRAINING PANEL
# ================================================================
def _training_embed(p: dict) -> discord.Embed:
    tr    = p.get("training", {})
    total = sum(tr.get(k, 0) for k in TRAINING_STATS)
    embed = discord.Embed(
        title="🏋️ Training Center",
        description=(
            f"**{total}/{TRAINING_MAX * len(TRAINING_STATS)}** total levels invested\n"
            "Permanently boost your stats. Each level costs more than the last."
        ),
        color=discord.Color.orange()
    )
    for key, info in TRAINING_STATS.items():
        lvl      = tr.get(key, 0)
        bonus    = lvl * info["bonus"]
        bar      = "█" * lvl + "░" * (TRAINING_MAX - lvl)
        next_cost = f"**${TRAINING_COSTS[lvl]:,}**" if lvl < TRAINING_MAX else "**MAX**"
        if key == "steal":
            bonus_txt = f"+{int(bonus*100)}% steal chance"
        elif key == "crime":
            bonus_txt = f"-{int(bonus*100)}% catch chance"
        elif key == "jobs":
            bonus_txt = f"+{int(bonus*100)}% job pay"
        else:
            bonus_txt = f"+{int(bonus*100)}% XP gain"
        embed.add_field(
            name=f"{info['emoji']} {info['name']}  `{bar}` Lv{lvl}",
            value=f"{bonus_txt}\nNext level: {next_cost}",
            inline=True
        )
    embed.set_footer(text=f"Wallet: ${p['money']:,}  •  Max level {TRAINING_MAX} per stat")
    return embed


class TrainingView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=90)
        self.uid = uid
        p  = get_player(uid)
        tr = p.get("training", {})
        for btn in [self.steal_btn, self.crime_btn, self.jobs_btn, self.xp_btn]:
            key = btn.custom_id
            lvl = tr.get(key, 0)
            if lvl >= TRAINING_MAX:
                btn.disabled = True
                btn.label    = f"{TRAINING_STATS[key]['emoji']} {TRAINING_STATS[key]['name']} — MAX"
            else:
                cost = TRAINING_COSTS[lvl]
                btn.label    = f"{TRAINING_STATS[key]['emoji']} {TRAINING_STATS[key]['name']} — ${cost:,}"
                btn.disabled = p["money"] < cost

    async def _upgrade(self, interaction: discord.Interaction, key: str):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p  = get_player(self.uid)
        tr = p.setdefault("training", {k: 0 for k in TRAINING_STATS})
        lvl = tr.get(key, 0)
        if lvl >= TRAINING_MAX:
            return await interaction.response.send_message("❌ Already at max level!", ephemeral=True)
        cost = TRAINING_COSTS[lvl]
        if p["money"] < cost:
            return await interaction.response.send_message(f"❌ Need **${cost:,}** — you have **${p['money']:,}**.", ephemeral=True)
        p["money"] -= cost
        tr[key]     = lvl + 1
        save_data()
        new_lvl   = tr[key]
        bonus     = new_lvl * TRAINING_STATS[key]["bonus"]
        embed = discord.Embed(
            title=f"{TRAINING_STATS[key]['emoji']} {TRAINING_STATS[key]['name']} — Level {new_lvl}!",
            color=discord.Color.orange()
        )
        embed.add_field(name="💸 Cost",    value=f"**-${cost:,}**",           inline=True)
        embed.add_field(name="💵 Wallet",  value=f"**${p['money']:,}**",      inline=True)
        if key == "steal":
            embed.add_field(name="📈 New Bonus", value=f"+**{int(bonus*100)}%** steal chance", inline=False)
        elif key == "crime":
            embed.add_field(name="📈 New Bonus", value=f"-**{int(bonus*100)}%** crime catch chance", inline=False)
        elif key == "jobs":
            embed.add_field(name="📈 New Bonus", value=f"+**{int(bonus*100)}%** job pay", inline=False)
        else:
            embed.add_field(name="📈 New Bonus", value=f"+**{int(bonus*100)}%** XP gain", inline=False)
        await interaction.response.edit_message(embed=embed, view=TrainingView(self.uid))

    @discord.ui.button(label="🥷 Steal Mastery", style=discord.ButtonStyle.blurple, row=0, custom_id="steal")
    async def steal_btn(self, interaction: discord.Interaction, button: Button):
        await self._upgrade(interaction, "steal")

    @discord.ui.button(label="🔫 Crime Expertise", style=discord.ButtonStyle.blurple, row=0, custom_id="crime")
    async def crime_btn(self, interaction: discord.Interaction, button: Button):
        await self._upgrade(interaction, "crime")

    @discord.ui.button(label="💼 Work Ethic", style=discord.ButtonStyle.blurple, row=1, custom_id="jobs")
    async def jobs_btn(self, interaction: discord.Interaction, button: Button):
        await self._upgrade(interaction, "jobs")

    @discord.ui.button(label="⭐ Fast Learner", style=discord.ButtonStyle.blurple, row=1, custom_id="xp")
    async def xp_btn(self, interaction: discord.Interaction, button: Button):
        await self._upgrade(interaction, "xp")

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.gray, row=2)
    async def refresh_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p = get_player(self.uid)
        await interaction.response.edit_message(embed=_training_embed(p), view=TrainingView(self.uid))


# ================================================================
# CAREER PATH PANEL
# ================================================================
def _career_embed(p: dict) -> discord.Embed:
    career = p.get("career")
    level  = p.get("level", 1)
    embed  = discord.Embed(
        title="🎯 Career Path",
        color=discord.Color.gold() if career else discord.Color.greyple()
    )
    if career:
        cp = CAREER_PATHS[career]
        embed.description = (
            f"**Active Path: {cp['emoji']} {cp['name']}**\n\n"
            + "\n".join(f"• {perk}" for perk in cp["perks"])
        )
        embed.set_footer(text="Career path is permanent — choose wisely!")
    else:
        if level < CAREER_UNLOCK_LEVEL:
            embed.description = (
                f"🔒 **Locked** — Reach Level **{CAREER_UNLOCK_LEVEL}** to unlock a Career Path.\n\n"
                f"You are Level **{level}** ({CAREER_UNLOCK_LEVEL - level} levels to go)."
            )
        else:
            embed.description = (
                "**Choose your Career Path — permanent bonus!**\n\n"
                + "\n\n".join(
                    f"{cp['emoji']} **{cp['name']}**\n" + "\n".join(f"  • {perk}" for perk in cp["perks"])
                    for cp in CAREER_PATHS.values()
                )
                + "\n\n⚠️ This choice is **permanent** and cannot be changed!"
            )
    return embed


class CareerView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=120)
        self.uid = uid
        p        = get_player(uid)
        career   = p.get("career")
        level    = p.get("level", 1)
        locked   = level < CAREER_UNLOCK_LEVEL or career is not None
        self.corporate_btn.disabled = locked
        self.hustler_btn.disabled   = locked
        self.shadow_btn.disabled    = locked
        if career:
            for btn in [self.corporate_btn, self.hustler_btn, self.shadow_btn]:
                if btn.custom_id == career:
                    btn.style = discord.ButtonStyle.success
                    btn.label = f"{CAREER_PATHS[career]['emoji']} {CAREER_PATHS[career]['name']} ✅ (Active)"

    async def _pick(self, interaction: discord.Interaction, path_key: str):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p = get_player(self.uid)
        if p.get("career"):
            return await interaction.response.send_message("❌ Career already chosen!", ephemeral=True)
        if p.get("level", 1) < CAREER_UNLOCK_LEVEL:
            return await interaction.response.send_message(f"❌ Need Level {CAREER_UNLOCK_LEVEL}!", ephemeral=True)
        p["career"] = path_key
        save_data()
        cp    = CAREER_PATHS[path_key]
        embed = discord.Embed(
            title=f"{cp['emoji']} Career Chosen: {cp['name']}!",
            description="\n".join(f"• {perk}" for perk in cp["perks"]),
            color=cp["color"]
        )
        embed.set_footer(text="Your career perks are now permanently active.")
        await interaction.response.edit_message(embed=embed, view=CareerView(self.uid))

    @discord.ui.button(label="💼 Corporate", style=discord.ButtonStyle.blurple, row=0, custom_id="corporate")
    async def corporate_btn(self, interaction: discord.Interaction, button: Button):
        await self._pick(interaction, "corporate")

    @discord.ui.button(label="🤑 Hustler",   style=discord.ButtonStyle.green,   row=0, custom_id="hustler")
    async def hustler_btn(self, interaction: discord.Interaction, button: Button):
        await self._pick(interaction, "hustler")

    @discord.ui.button(label="🕶️ Shadow",    style=discord.ButtonStyle.danger,  row=0, custom_id="shadow")
    async def shadow_btn(self, interaction: discord.Interaction, button: Button):
        await self._pick(interaction, "shadow")


# ================================================================
# LOTTERY PANEL
# ================================================================
def _lottery_embed() -> discord.Embed:
    ls      = lottery_state
    pot     = ls.get("pot", 0) + LOTTERY_SEED
    tickets = ls.get("tickets", {})
    total_t = sum(tickets.values())
    draw_ts = ls.get("draw_at", 0)
    lw      = ls.get("last_winner", {})
    embed   = discord.Embed(
        title="🎟️ Weekly Lottery",
        description=(
            f"**Jackpot: ${pot:,}**\n"
            f"🎫 Tickets sold: **{total_t}**  •  Drawing <t:{int(draw_ts)}:R>\n\n"
            f"Each ticket costs **${LOTTERY_TICKET_PRICE:,}**. "
            f"More tickets = better odds. Winner takes **{int(LOTTERY_WINNER_CUT*100)}%** of the pot!"
        ),
        color=discord.Color.gold()
    )
    if lw:
        embed.add_field(
            name="🏆 Last Winner",
            value=f"**{lw.get('name','Unknown')}** won **${lw.get('amount',0):,}**",
            inline=False
        )
    embed.set_footer(text="Drawing every Sunday at midnight UTC  •  20% of pot goes to winner's gang bank")
    return embed


class LotteryBuyModal(discord.ui.Modal, title="🎟️ Buy Lottery Tickets"):
    amount = discord.ui.TextInput(label="How many tickets?", placeholder="e.g. 5", min_length=1, max_length=4)

    def __init__(self, uid: str):
        super().__init__(); self.uid = uid

    async def on_submit(self, interaction: discord.Interaction):
        uid = self.uid
        p   = get_player(uid)
        raw = self.amount.value.strip()
        if not raw.isdigit() or int(raw) < 1:
            return await interaction.response.send_message("❌ Enter a valid number of tickets.", ephemeral=True)
        count = int(raw)
        cost  = count * LOTTERY_TICKET_PRICE
        if p["money"] < cost:
            return await interaction.response.send_message(f"❌ Need **${cost:,}** — you only have **${p['money']:,}**.", ephemeral=True)
        p["money"] -= cost
        lottery_state["pot"] = lottery_state.get("pot", 0) + cost
        lottery_state["tickets"][uid] = lottery_state["tickets"].get(uid, 0) + count
        save_data(); save_lottery()
        my_tickets = lottery_state["tickets"][uid]
        total_t    = sum(lottery_state["tickets"].values())
        odds_pct   = round(my_tickets / total_t * 100, 1) if total_t else 100
        embed = discord.Embed(
            title="🎟️ Tickets Purchased!",
            description=f"You bought **{count}** ticket{'s' if count > 1 else ''}!",
            color=discord.Color.green()
        )
        embed.add_field(name="💸 Cost",       value=f"**-${cost:,}**",            inline=True)
        embed.add_field(name="🎫 Your Total", value=f"**{my_tickets}** tickets",   inline=True)
        embed.add_field(name="📊 Your Odds",  value=f"**{odds_pct}%** win chance", inline=True)
        embed.add_field(name="💰 Jackpot",    value=f"**${lottery_state['pot'] + LOTTERY_SEED:,}**", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class LotteryView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=90); self.uid = uid

    @discord.ui.button(label="🎟️ Buy Tickets", style=discord.ButtonStyle.green, row=0)
    async def buy_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        await interaction.response.send_modal(LotteryBuyModal(self.uid))

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.gray, row=0)
    async def refresh_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        await interaction.response.edit_message(embed=_lottery_embed(), view=self)

    @discord.ui.button(label="📊 My Tickets", style=discord.ButtonStyle.blurple, row=0)
    async def my_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        uid      = self.uid
        my_t     = lottery_state["tickets"].get(uid, 0)
        total_t  = sum(lottery_state["tickets"].values())
        odds_pct = round(my_t / total_t * 100, 1) if total_t and my_t else 0
        msg = (f"🎫 **{my_t}** ticket{'s' if my_t != 1 else ''}  •  "
               f"**{odds_pct}%** win chance" if my_t else "You have no tickets yet — buy some!")
        await interaction.response.send_message(msg, ephemeral=True)


# ================================================================
# PROPERTIES PANEL
# ================================================================
def _prop_income_ready(prop: dict) -> bool:
    return time.time() - prop.get("last_income", 0) >= PROPERTIES[prop["type"]]["cd"]

def _prop_embed(p: dict) -> discord.Embed:
    props    = p.get("properties", [])
    slots    = len(props)
    embed    = discord.Embed(
        title="🏠 Your Properties",
        description=(
            f"**{slots}/{MAX_PROPERTIES}** slots used\n"
            "Properties generate income every **6 hours**. Collect with the button below!"
        ),
        color=discord.Color.blue()
    )
    if not props:
        embed.add_field(name="📭 No Properties", value="Buy your first property below!", inline=False)
    for i, prop in enumerate(props):
        info    = PROPERTIES[prop["type"]]
        ready   = _prop_income_ready(prop)
        next_at = int(prop.get("last_income", 0) + info["cd"])
        status  = "✅ **Ready to collect!**" if ready else f"⏳ Next: <t:{next_at}:R>"
        embed.add_field(
            name=f"{info['emoji']} {info['name']} #{i+1}",
            value=(
                f"Income: **${info['income'][0]:,}–${info['income'][1]:,}**\n{status}"
            ),
            inline=True
        )
    embed.set_footer(text="Upgrade by buying more properties  •  Rivals can raid your properties!")
    return embed


class PropertyBuyView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=60); self.uid = uid

    async def _buy(self, interaction: discord.Interaction, ptype: str):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p    = get_player(self.uid)
        info = PROPERTIES[ptype]
        if len(p.get("properties", [])) >= MAX_PROPERTIES:
            return await interaction.response.send_message(f"❌ Max **{MAX_PROPERTIES}** properties reached.", ephemeral=True)
        if p["money"] < info["cost"]:
            return await interaction.response.send_message(f"❌ Need **${info['cost']:,}** — you have **${p['money']:,}**.", ephemeral=True)
        p["money"] -= info["cost"]
        p["properties"].append({"type": ptype, "bought_at": time.time(), "last_income": 0})
        save_data()
        embed = discord.Embed(
            title=f"🏠 {info['emoji']} {info['name']} Purchased!",
            description=f"You now own a **{info['name']}**!",
            color=discord.Color.green()
        )
        embed.add_field(name="💸 Cost",   value=f"**-${info['cost']:,}**",          inline=True)
        embed.add_field(name="💰 Income", value=f"**${info['income'][0]:,}–${info['income'][1]:,}**/6h", inline=True)
        embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**",              inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="🏠 Apartment ($2,000)",   style=discord.ButtonStyle.blurple, row=0)
    async def apt_btn(self, i, b): await self._buy(i, "apartment")

    @discord.ui.button(label="🏪 Storefront ($8,000)",  style=discord.ButtonStyle.blurple, row=0)
    async def store_btn(self, i, b): await self._buy(i, "storefront")

    @discord.ui.button(label="🏭 Warehouse ($25,000)",  style=discord.ButtonStyle.green, row=1)
    async def wh_btn(self, i, b): await self._buy(i, "warehouse")

    @discord.ui.button(label="🏙️ Penthouse ($80,000)",  style=discord.ButtonStyle.green, row=1)
    async def ph_btn(self, i, b): await self._buy(i, "penthouse")

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=2)
    async def back_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p = get_player(self.uid)
        await interaction.response.edit_message(embed=_prop_embed(p), view=PropertyView(self.uid))


class PropertyView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=90); self.uid = uid

    @discord.ui.button(label="💰 Collect Income", style=discord.ButtonStyle.green, row=0)
    async def collect_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p     = get_player(self.uid)
        props = p.get("properties", [])
        total = 0
        now   = time.time()
        collected = 0
        for prop in props:
            if _prop_income_ready(prop):
                info = PROPERTIES[prop["type"]]
                amt  = random.randint(*info["income"])
                total += amt
                prop["last_income"] = now
                collected += 1
        if total == 0:
            return await interaction.response.send_message("⏳ No properties are ready to collect yet!", ephemeral=True)
        p["money"] += total
        new_ach = check_achievements(p)
        save_data()
        result_embed = discord.Embed(
            title="💰 Income Collected!",
            description=f"Collected from **{collected}** propert{'ies' if collected != 1 else 'y'}!",
            color=discord.Color.green()
        )
        result_embed.add_field(name="💰 Earned", value=f"**+${total:,}**",     inline=True)
        result_embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**", inline=True)
        if new_ach:
            result_embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
        result_embed.set_footer(text="Refreshing panel...")
        await interaction.response.edit_message(embed=result_embed, view=self)
        await asyncio.sleep(2)
        await interaction.edit_original_response(embed=_prop_embed(get_player(self.uid)), view=PropertyView(self.uid))

    @discord.ui.button(label="🏠 Buy Property", style=discord.ButtonStyle.blurple, row=0)
    async def buy_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p = get_player(self.uid)
        embed = discord.Embed(
            title="🏠 Buy a Property",
            description=(
                f"Choose a property to purchase.\n"
                f"**{len(p.get('properties', []))}/{MAX_PROPERTIES}** slots used.\n\n"
                "All properties generate income every **6 hours**."
            ),
            color=discord.Color.blue()
        )
        for ptype, info in PROPERTIES.items():
            embed.add_field(
                name=f"{info['emoji']} {info['name']} — **${info['cost']:,}**",
                value=f"Income: **${info['income'][0]:,}–${info['income'][1]:,}**/6h",
                inline=True
            )
        await interaction.response.edit_message(embed=embed, view=PropertyBuyView(self.uid))

    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.gray, row=0)
    async def back_btn(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        p    = get_player(self.uid, interaction.user.display_name)
        c_cd = fmt_cd(cd_remaining(p, "crime", CRIME_COOLDOWN))
        props    = p.get("properties", [])
        ready_ct = sum(1 for pr in props if _prop_income_ready(pr))
        biz_val  = f"{len(props)}/{MAX_PROPERTIES} properties" + (f"  •  ✅ {ready_ct} ready!" if ready_ct else "")
        embed = discord.Embed(title="💼 Economy", description=f"Wallet: **${p['money']:,}**  •  Bank: **${p['bank']:,}**", color=discord.Color.blurple())
        embed.add_field(name="💼 Jobs",    value="9 jobs across 3 tiers — Safe, Risky & Skill", inline=False)
        embed.add_field(name="🔫 Crime",   value=f"Earn $200–700 (risky!)  •  `{c_cd}`\nHeat: `{heat_bar(p.get('heat',0))}` {heat_label(p.get('heat',0))}", inline=False)
        embed.add_field(name="🏦 Bank",    value="Deposit & withdraw safely", inline=False)
        embed.add_field(name="🛒 Shop",    value="Buy items & upgrades", inline=False)
        embed.add_field(name="🏢 Business", value=biz_val or "Buy properties for passive income every 6h", inline=False)
        await interaction.response.edit_message(embed=embed, view=EconomyMenuView(self.uid))


class PropertyRaidTargetView(View):
    def __init__(self, uid: str):
        super().__init__(timeout=60); self.uid = uid

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="🏠 Choose a player to raid...", min_values=1, max_values=1)
    async def target_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        if str(interaction.user.id) != self.uid:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        uid     = self.uid
        member  = select.values[0]
        uid2    = str(member.id)
        p       = get_player(uid)
        t2      = get_player(uid2, member.display_name)
        if uid == uid2:
            return await interaction.response.send_message("❌ Can't raid yourself.", ephemeral=True)
        if member.bot:
            return await interaction.response.send_message("❌ Can't raid a bot.", ephemeral=True)
        if not t2.get("properties"):
            return await interaction.response.send_message(f"❌ **{member.display_name}** has no properties to raid.", ephemeral=True)
        rem = cd_remaining(p, f"prop_raid_{uid2}", PROPERTY_RAID_CD)
        if rem > 0:
            return await interaction.response.send_message(f"⏳ Raided this player recently. Wait `{fmt_cd(rem)}`.", ephemeral=True)

        # Execute raid (40% success, costs mask)
        p["items"].remove("mask")
        p["cooldowns"][f"prop_raid_{uid2}"] = time.time()
        success = random.random() < 0.40

        if success:
            ready_props = [pr for pr in t2["properties"] if _prop_income_ready(pr)]
            if not ready_props:
                # Steal from a random property's next cycle
                target_prop = random.choice(t2["properties"])
            else:
                target_prop = random.choice(ready_props)
            info    = PROPERTIES[target_prop["type"]]
            haul    = random.randint(*info["income"])
            p["money"] += haul
            target_prop["last_income"] = time.time()  # reset their collection timer
            p["stats"]["properties_raided"] = p["stats"].get("properties_raided", 0) + 1
            p["heat"] = min(10, p.get("heat", 0) + 2)
            save_data()
            embed = discord.Embed(title="🔫 Property Raid — SUCCESS!", color=discord.Color.green())
            embed.add_field(name="🎯 Target",   value=member.display_name,          inline=True)
            embed.add_field(name="🏠 Property", value=f"{info['emoji']} {info['name']}", inline=True)
            embed.add_field(name="💰 Haul",     value=f"**+${haul:,}**",            inline=True)
            embed.add_field(name="💵 Wallet",   value=f"**${p['money']:,}**",        inline=True)
            try:
                await interaction.channel.send(
                    f"🔫 <@{uid2}> — **{interaction.user.display_name}** raided your **{info['name']}** and stole **${haul:,}**!", delete_after=60)
            except Exception: pass
        else:
            fine = min(200, p["money"]); p["money"] -= fine
            p["heat"] = min(10, p.get("heat", 0) + 1)
            save_data()
            embed = discord.Embed(title="🚔 Property Raid — FAILED!", color=discord.Color.red())
            embed.add_field(name="🎯 Target", value=member.display_name,  inline=True)
            embed.add_field(name="💸 Fine",   value=f"-**${fine:,}**",    inline=True)
            embed.add_field(name="💵 Wallet", value=f"**${p['money']:,}**", inline=True)
        await interaction.response.edit_message(embed=embed, view=PropertyView(uid))


# ================================================================
# SOLO BLACKJACK
# ================================================================
class SoloBlackjackView(View):
    def __init__(self, user_id, bet, player_hand, dealer_hand, deck):
        super().__init__(timeout=120)
        self.user_id = user_id; self.bet = bet; self.player_hand = player_hand
        self.dealer_hand = dealer_hand; self.deck = deck; self.done = False

    def build_embed(self, user=None, reveal=False):
        p    = get_player(self.user_id)
        pval = hand_value(self.player_hand)
        d_display = f"`{hand_str(self.dealer_hand)}` = **{hand_value(self.dealer_hand)}**" if reveal else f"`{hand_str(self.dealer_hand, hide_second=True)}`"
        embed = discord.Embed(title=f"🃏 {user.display_name}'s Blackjack" if user else "🃏 Blackjack", color=discord.Color.dark_green())
        embed.add_field(name="🏠 Dealer",    value=d_display,                                        inline=False)
        embed.add_field(name="🎴 Your Hand", value=f"`{hand_str(self.player_hand)}` = **{pval}**",   inline=False)
        embed.add_field(name="💰 Bet",       value=f"**${self.bet:,}**",                             inline=True)
        embed.add_field(name="💵 Wallet",    value=f"**${p['money']:,}**",                           inline=True)
        return embed

    def _finish(self, result, amount, user):
        p = get_player(self.user_id)
        colors = {"win": discord.Color.green(), "blackjack": discord.Color.gold(), "push": discord.Color.blurple(), "lose": discord.Color.red(), "bust": discord.Color.red()}
        labels = {"win": f"🏆 Win **+${amount:,}**!", "blackjack": f"🌟 BLACKJACK! **+${amount:,}**!", "push": "🤝 Push — bet returned.", "lose": f"❌ Lose **-${self.bet:,}**.", "bust": f"💥 BUST! **-${self.bet:,}**."}
        embed = discord.Embed(title=f"🃏 {user.display_name} — {labels[result]}", color=colors[result])
        embed.add_field(name="🏠 Dealer",    value=f"`{hand_str(self.dealer_hand)}` = **{hand_value(self.dealer_hand)}**", inline=False)
        embed.add_field(name="🎴 Your Hand", value=f"`{hand_str(self.player_hand)}` = **{hand_value(self.player_hand)}**", inline=False)
        embed.add_field(name="💰 Bet",       value=f"**${self.bet:,}**",  inline=True)
        embed.add_field(name="💵 Wallet",    value=f"**${p['money']:,}**", inline=True)
        return embed

    async def _end(self, interaction):
        self.done = True; uid = self.user_id; p = get_player(uid)
        pval = hand_value(self.player_hand)
        while hand_value(self.dealer_hand) < 17: self.dealer_hand.append(self.deck.pop())
        dval = hand_value(self.dealer_hand)
        if len(self.player_hand) == 2 and pval == 21:
            win = int(self.bet * 1.5); p["money"] += self.bet + win; p["stats"]["wins"] += 1; p["stats"]["total_won"] += win; add_xp(p, 30); result, amount = "blackjack", win
        elif pval > 21:
            p["stats"]["losses"] += 1; p["stats"]["total_lost"] += self.bet; add_xp(p, 5); result, amount = "bust", -self.bet
        elif dval > 21 or pval > dval:
            p["money"] += self.bet * 2; p["stats"]["wins"] += 1; p["stats"]["total_won"] += self.bet; add_xp(p, 20); result, amount = "win", self.bet
        elif pval == dval:
            p["money"] += self.bet; add_xp(p, 10); result, amount = "push", 0
        else:
            p["stats"]["losses"] += 1; p["stats"]["total_lost"] += self.bet; add_xp(p, 5); result, amount = "lose", -self.bet
        save_data(); self.clear_items()
        await interaction.response.edit_message(embed=self._finish(result, abs(amount), interaction.user), view=self)

    @discord.ui.button(label="👆 Hit", style=discord.ButtonStyle.green, row=0)
    async def hit_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Not your game.", ephemeral=True)
        if self.done: return await interaction.response.send_message("Game ended.", ephemeral=True)
        self.player_hand.append(self.deck.pop()); pval = hand_value(self.player_hand)
        if pval > 21:
            self.done = True; p = get_player(self.user_id)
            p["stats"]["losses"] += 1; p["stats"]["total_lost"] += self.bet; add_xp(p, 5); save_data(); self.clear_items()
            while hand_value(self.dealer_hand) < 17: self.dealer_hand.append(self.deck.pop())
            return await interaction.response.edit_message(embed=self._finish("bust", self.bet, interaction.user), view=self)
        await interaction.response.edit_message(embed=self.build_embed(interaction.user), view=self)

    @discord.ui.button(label="✋ Stand", style=discord.ButtonStyle.red, row=0)
    async def stand_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Not your game.", ephemeral=True)
        if self.done: return await interaction.response.send_message("Game ended.", ephemeral=True)
        await self._end(interaction)

    @discord.ui.button(label="⚡ Double Down", style=discord.ButtonStyle.blurple, row=0)
    async def double_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id: return await interaction.response.send_message("❌ Not your game.", ephemeral=True)
        if self.done: return await interaction.response.send_message("Game ended.", ephemeral=True)
        p = get_player(self.user_id)
        if len(self.player_hand) != 2: return await interaction.response.send_message("❌ Only on first 2 cards.", ephemeral=True)
        if p["money"] < self.bet:      return await interaction.response.send_message(f"❌ Need **${self.bet:,}**.", ephemeral=True)
        p["money"] -= self.bet; self.bet *= 2; save_data()
        self.player_hand.append(self.deck.pop()); await self._end(interaction)

# ================================================================
# PvP RPS
# ================================================================
rps_challenges = {}

RPS_BEATS = {"rock": "scissors", "scissors": "paper", "paper": "rock"}
RPS_EMOJI = {"rock": "✊", "paper": "✋", "scissors": "✌️"}

class RpsPickView(View):
    def __init__(self, challenge_id: str, role: str):
        super().__init__(timeout=60)
        self.challenge_id = challenge_id
        self.role         = role   # "challenger" or "challenged"

    async def _pick(self, interaction: discord.Interaction, choice: str):
        ch  = rps_challenges.get(self.challenge_id)
        uid = str(interaction.user.id)
        if not ch:
            return await interaction.response.send_message("❌ Challenge expired.", ephemeral=True)
        expected = ch["challenger"] if self.role == "challenger" else ch["challenged"]
        if uid != expected:
            return await interaction.response.send_message("❌ Not your pick.", ephemeral=True)
        if ch.get(f"{self.role}_choice"):
            return await interaction.response.send_message("✅ Already locked in!", ephemeral=True)

        ch[f"{self.role}_choice"] = choice
        self.stop()
        await interaction.response.send_message(
            f"🔒 Locked in: **{RPS_EMOJI[choice]} {choice.capitalize()}** — waiting for opponent...",
            ephemeral=True
        )
        if ch.get("challenger_choice") and ch.get("challenged_choice"):
            await _rps_resolve(interaction, self.challenge_id)

    @discord.ui.button(label="✊ Rock",     style=discord.ButtonStyle.gray)
    async def rock_btn(self, i, b):     await self._pick(i, "rock")

    @discord.ui.button(label="✋ Paper",    style=discord.ButtonStyle.blurple)
    async def paper_btn(self, i, b):    await self._pick(i, "paper")

    @discord.ui.button(label="✌️ Scissors", style=discord.ButtonStyle.red)
    async def scissors_btn(self, i, b): await self._pick(i, "scissors")


class RpsChallengeView(View):
    def __init__(self, challenge_id: str):
        super().__init__(timeout=120)
        self.challenge_id = challenge_id

    @discord.ui.button(label="🎯 Lock In My Move", style=discord.ButtonStyle.blurple, row=0)
    async def challenger_pick(self, interaction: discord.Interaction, button: Button):
        ch = rps_challenges.get(self.challenge_id)
        if not ch: return await interaction.response.send_message("❌ Expired.", ephemeral=True)
        if str(interaction.user.id) != ch["challenger"]:
            return await interaction.response.send_message("❌ Only the challenger can use this.", ephemeral=True)
        if ch.get("challenger_choice"):
            return await interaction.response.send_message("✅ Already locked in — waiting for opponent.", ephemeral=True)
        await interaction.response.send_message("🤫 Your move (secret!):", view=RpsPickView(self.challenge_id, "challenger"), ephemeral=True)

    @discord.ui.button(label="⚔️ Accept & Pick", style=discord.ButtonStyle.green, row=0)
    async def accept_btn(self, interaction: discord.Interaction, button: Button):
        ch  = rps_challenges.get(self.challenge_id)
        uid = str(interaction.user.id)
        if not ch: return await interaction.response.send_message("❌ Expired.", ephemeral=True)
        if uid != ch["challenged"]:
            return await interaction.response.send_message("❌ This challenge isn't for you.", ephemeral=True)
        if ch.get("accepted"):
            return await interaction.response.send_message("✅ Already accepted!", ephemeral=True)
        p = get_player(uid, interaction.user.display_name)
        if p["money"] < ch["bet"]:
            return await interaction.response.send_message(f"❌ Need **${ch['bet']:,}** to accept. You have **${p['money']:,}**.", ephemeral=True)

        p["money"] -= ch["bet"]; ch["accepted"] = True; save_data()

        embed = discord.Embed(
            title="⚔️ RPS Battle — In Progress",
            description=(f"<@{ch['challenger']}> **VS** <@{ch['challenged']}>\n"
                         f"💰 Pot: **${ch['bet']*2:,}** — winner takes all!\n"
                         f"🤫 Both players are picking secretly..."),
            color=discord.Color.orange()
        )
        embed.set_footer(text="Waiting for both moves to lock in...")
        await interaction.response.edit_message(embed=embed, view=self)
        await interaction.followup.send("🤫 Your move (secret!):", view=RpsPickView(self.challenge_id, "challenged"), ephemeral=True)

    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.red, row=0)
    async def decline_btn(self, interaction: discord.Interaction, button: Button):
        ch  = rps_challenges.get(self.challenge_id)
        uid = str(interaction.user.id)
        if not ch: return await interaction.response.send_message("❌ Expired.", ephemeral=True)
        if uid != ch["challenged"]:
            return await interaction.response.send_message("❌ Not your challenge.", ephemeral=True)

        get_player(ch["challenger"])["money"] += ch["bet"]; save_data()
        del rps_challenges[self.challenge_id]

        embed = discord.Embed(
            title="❌ Challenge Declined",
            description=f"<@{uid}> declined. Bet returned to <@{ch['challenger']}>.",
            color=discord.Color.red()
        )
        await interaction.response.edit_message(embed=embed, view=None)


async def _rps_resolve(interaction: discord.Interaction, challenge_id: str):
    ch = rps_challenges.get(challenge_id)
    if not ch or ch.get("resolved"): return
    ch["resolved"] = True

    c1, c2  = ch["challenger_choice"], ch["challenged_choice"]
    bet     = ch["bet"]
    p1      = get_player(ch["challenger"])
    p2      = get_player(ch["challenged"])

    if RPS_BEATS[c1] == c2:
        p1["money"] += bet * 2
        p1["stats"]["wins"]  += 1; p1["stats"]["total_won"]  += bet
        p2["stats"]["losses"] += 1; p2["stats"]["total_lost"] += bet
        add_xp(p1, 25); add_xp(p2, 10)
        result = f"🏆 <@{ch['challenger']}> wins **+${bet:,}**!"
        color  = discord.Color.green()
    elif RPS_BEATS[c2] == c1:
        p2["money"] += bet * 2
        p2["stats"]["wins"]  += 1; p2["stats"]["total_won"]  += bet
        p1["stats"]["losses"] += 1; p1["stats"]["total_lost"] += bet
        add_xp(p2, 25); add_xp(p1, 10)
        result = f"🏆 <@{ch['challenged']}> wins **+${bet:,}**!"
        color  = discord.Color.red()
    else:
        p1["money"] += bet; p2["money"] += bet
        add_xp(p1, 10); add_xp(p2, 10)
        result = "🤝 **Tie!** Both bets returned."
        color  = discord.Color.blurple()

    save_data()

    guild = interaction.guild or (bot.get_channel(ch.get("channel_id")) and bot.get_channel(ch["channel_id"]).guild)
    m1 = guild.get_member(ch["challenger"]) if guild else None
    m2 = guild.get_member(ch["challenged"]) if guild else None
    n1 = m1.display_name if m1 else f"<@{ch['challenger']}>"
    n2 = m2.display_name if m2 else f"<@{ch['challenged']}>"

    if RPS_BEATS[c1] == c2:
        result = f"🏆 **{n1}** wins **+${bet:,}**!"
    elif RPS_BEATS[c2] == c1:
        result = f"🏆 **{n2}** wins **+${bet:,}**!"

    del rps_challenges[challenge_id]

    embed = discord.Embed(title="⚔️ RPS — REVEAL! 🔥", color=color)
    embed.add_field(name=n1,          value=f"**{RPS_EMOJI[c1]} {c1.capitalize()}**", inline=True)
    embed.add_field(name="⚔️",        value="VS",                                     inline=True)
    embed.add_field(name=n2,          value=f"**{RPS_EMOJI[c2]} {c2.capitalize()}**", inline=True)
    embed.add_field(name="🏆 Result", value=result,                                   inline=False)

    try:
        mid    = ch.get("msg_id")
        rps_ch = bot.get_channel(ch.get("channel_id")) or interaction.channel
        if mid:
            msg = await rps_ch.fetch_message(mid)
            await msg.edit(embed=embed, view=None)
        else:
            await rps_ch.send(embed=embed)
    except Exception as e:
        log.error(f"_rps_resolve: {e}")
        try: await (bot.get_channel(ch.get("channel_id")) or interaction.channel).send(embed=embed)
        except Exception: pass


# ================================================================
# DUEL SYSTEM
# ================================================================
duels = {}
DUEL_BEATS = {"attack": "defend", "defend": "special", "special": "attack"}
DUEL_EMOJI = {"attack": "⚔️", "defend": "🛡️", "special": "🌀"}

class DuelPickView(View):
    def __init__(self, duel_id: str, role: str):
        super().__init__(timeout=60)
        self.duel_id = duel_id; self.role = role

    async def _pick(self, interaction: discord.Interaction, choice: str):
        d   = duels.get(self.duel_id)
        uid = str(interaction.user.id)
        if not d: return await interaction.response.send_message("❌ Duel expired.", ephemeral=True)
        expected = d["challenger"] if self.role == "challenger" else d["challenged"]
        if uid != expected: return await interaction.response.send_message("❌ Not your duel.", ephemeral=True)
        if d.get(f"{self.role}_move"): return await interaction.response.send_message("✅ Already locked in!", ephemeral=True)
        d[f"{self.role}_move"] = choice; self.stop()
        await interaction.response.send_message(f"🔒 Locked: **{DUEL_EMOJI[choice]} {choice.capitalize()}** — waiting for opponent...", ephemeral=True)
        if d.get("challenger_move") and d.get("challenged_move"):
            await _duel_resolve(interaction, self.duel_id)

    @discord.ui.button(label="⚔️ Attack",  style=discord.ButtonStyle.red)
    async def atk_btn(self, i, b): await self._pick(i, "attack")
    @discord.ui.button(label="🛡️ Defend",  style=discord.ButtonStyle.blurple)
    async def def_btn(self, i, b): await self._pick(i, "defend")
    @discord.ui.button(label="🌀 Special", style=discord.ButtonStyle.green)
    async def spc_btn(self, i, b): await self._pick(i, "special")


class DuelChallengeView(View):
    def __init__(self, duel_id: str):
        super().__init__(timeout=120); self.duel_id = duel_id

    @discord.ui.button(label="⚔️ Accept & Fight", style=discord.ButtonStyle.green)
    async def accept_btn(self, interaction: discord.Interaction, button: Button):
        d   = duels.get(self.duel_id)
        uid = str(interaction.user.id)
        if not d:               return await interaction.response.send_message("❌ Expired.", ephemeral=True)
        if uid != d["challenged"]: return await interaction.response.send_message("❌ Not your duel.", ephemeral=True)
        if d.get("accepted"):   return await interaction.response.send_message("✅ Already accepted!", ephemeral=True)
        p = get_player(uid, interaction.user.display_name)
        if p["money"] < d["bet"]: return await interaction.response.send_message(f"❌ Need **${d['bet']:,}**.", ephemeral=True)
        p["money"] -= d["bet"]; d["accepted"] = True; save_data()
        embed = discord.Embed(
            title="⚔️ Duel — Pick Your Move!",
            description=(f"<@{d['challenger']}> **VS** <@{d['challenged']}>\n"
                         f"💰 Pot: **${d['bet']*2:,}**\n🤫 Both players — pick secretly!"),
            color=discord.Color.orange()
        )
        embed.add_field(name="⚔️ Attack",  value="Beats 🛡️ Defend",  inline=True)
        embed.add_field(name="🛡️ Defend",  value="Beats 🌀 Special", inline=True)
        embed.add_field(name="🌀 Special", value="Beats ⚔️ Attack",  inline=True)
        await interaction.response.edit_message(embed=embed, view=self)
        await interaction.followup.send("🤫 Your move (secret!):", view=DuelPickView(self.duel_id, "challenged"), ephemeral=True)

    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.red)
    async def decline_btn(self, interaction: discord.Interaction, button: Button):
        d   = duels.get(self.duel_id)
        uid = str(interaction.user.id)
        if not d:               return await interaction.response.send_message("❌ Expired.", ephemeral=True)
        if uid != d["challenged"]: return await interaction.response.send_message("❌ Not your duel.", ephemeral=True)
        get_player(d["challenger"])["money"] += d["bet"]; save_data()
        del duels[self.duel_id]
        await interaction.response.edit_message(
            embed=discord.Embed(title="❌ Duel Declined", description=f"<@{uid}> declined. Bet returned.", color=discord.Color.red()), view=None)


async def _duel_resolve(interaction: discord.Interaction, duel_id: str):
    d = duels.get(duel_id)
    if not d or d.get("resolved"): return
    d["resolved"] = True
    c1, c2 = d["challenger_move"], d["challenged_move"]
    p1 = get_player(d["challenger"]); p2 = get_player(d["challenged"])
    bet = d["bet"]
    if DUEL_BEATS[c1] == c2:
        p1["money"] += bet * 2
        p1["stats"]["wins"] += 1; p1["stats"]["total_won"] += bet
        p2["stats"]["losses"] += 1; p2["stats"]["total_lost"] += bet
        _weekly_inc(p1, "duel_wins")
        add_xp(p1, 30); add_xp(p2, 10)
        result = f"🏆 <@{d['challenger']}> wins **+${bet:,}**!"; color = discord.Color.green()
    elif DUEL_BEATS[c2] == c1:
        p2["money"] += bet * 2
        p2["stats"]["wins"] += 1; p2["stats"]["total_won"] += bet
        p1["stats"]["losses"] += 1; p1["stats"]["total_lost"] += bet
        _weekly_inc(p2, "duel_wins")
        add_xp(p2, 30); add_xp(p1, 10)
        result = f"🏆 <@{d['challenged']}> wins **+${bet:,}**!"; color = discord.Color.red()
    else:
        winner = d["challenger"] if p1.get("level", 1) >= p2.get("level", 1) else d["challenged"]
        pw = p1 if winner == d["challenger"] else p2
        pl = p2 if winner == d["challenger"] else p1
        pw["money"] += bet * 2
        pw["stats"]["wins"] += 1; pw["stats"]["total_won"] += bet
        pl["stats"]["losses"] += 1; pl["stats"]["total_lost"] += bet
        _weekly_inc(pw, "duel_wins")
        add_xp(pw, 20); add_xp(pl, 10)
        result = f"🤝 Tie — <@{winner}> wins by level! **+${bet:,}**"; color = discord.Color.gold()
    save_data(); del duels[duel_id]
    embed = discord.Embed(title="⚔️ DUEL RESULT! 🥊", color=color)
    embed.add_field(name=f"<@{d['challenger']}>", value=f"**{DUEL_EMOJI[c1]} {c1.capitalize()}**", inline=True)
    embed.add_field(name="⚔️ VS ⚔️",              value="​",                                        inline=True)
    embed.add_field(name=f"<@{d['challenged']}>", value=f"**{DUEL_EMOJI[c2]} {c2.capitalize()}**", inline=True)
    embed.add_field(name="🏆 Result",              value=result,                                     inline=False)
    embed.set_footer(text="⚔️ Attack > 🛡️ Defend > 🌀 Special > ⚔️ Attack")
    try:
        duel_ch = bot.get_channel(d.get("channel_id")) or interaction.channel
        mid     = d.get("msg_id")
        if mid:
            msg = await duel_ch.fetch_message(mid)
            await msg.edit(embed=embed, view=None)
        else:
            await duel_ch.send(embed=embed)
    except Exception as e:
        log.error(f"_duel_resolve: {e}")
        try: await (bot.get_channel(d.get("channel_id")) or interaction.channel).send(embed=embed)
        except Exception: pass


# ================================================================
# COMMANDS (slash)
# ================================================================
async def _back_to_main(interaction: discord.Interaction, user_id: str):
    p = get_player(user_id)
    await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(user_id))
    menu_messages[user_id] = interaction.message

async def _ensure_user(interaction: discord.Interaction):
    uid = str(interaction.user.id)
    p   = get_player(uid, interaction.user.display_name)
    if interaction.guild:
        user_context[uid] = {"guild_id": interaction.guild.id, "channel_id": interaction.channel_id}
    return uid, p

@bot.tree.command(name="play", description="Open your private game menu — only you can see it")
async def play_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    await interaction.response.send_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(uid), ephemeral=True)
    old = menu_messages.pop(uid, None)
    if old:
        try: await old.delete()
        except Exception: pass

@bot.tree.command(name="profile", description="View your stats or another player's public info")
async def profile_slash(interaction: discord.Interaction, member: discord.Member = None):
    await _ensure_user(interaction)
    if member and member != interaction.user:
        uid = str(member.id)
        if uid not in players:
            return await interaction.response.send_message("❌ That player has no account.", ephemeral=True)
        p = get_player(uid, member.display_name)
        b_total = _bounty_total(uid)
        desc    = f"{_rank_title(p['level'])}  •  Level **{p['level']}**"
        if b_total > 0:
            desc += f"\n🎯 **WANTED** — Bounty: **${b_total:,}**"
        embed = discord.Embed(title=f"{player_icon(uid)} {member.display_name}", description=desc, color=_rank_color(p["level"]))
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.set_footer(text="Full stats & inventory are private")
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        get_player(str(interaction.user.id), interaction.user.display_name)
        await interaction.response.send_message(embed=private_profile_embed(interaction.user), ephemeral=True)

@bot.tree.command(name="daily", description="Claim your daily bonus and streak reward")
async def daily_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction); now = time.time()
    rem = cd_remaining(p, "daily", 86400)
    if rem > 0: return await interaction.response.send_message(f"⏳ Daily cooldown: `{fmt_cd(rem)}`", ephemeral=True)
    last = p.get("last_daily", 0); hours_since = (now - last) / 3600
    p["streak"] = (p.get("streak", 0) + 1) if 20 <= hours_since <= 48 else 1
    streak = p["streak"]; bonus = 500 + p["level"] * 50 + (min(streak, 7) - 1) * 100
    p["money"] += bonus; p["heat"] = max(0, p.get("heat", 0) - 2)
    p["cooldowns"]["daily"] = now; p["last_daily"] = now; leveled = add_xp(p, 50 + streak * 5)
    new_ach = check_achievements(p); save_data()
    embed = discord.Embed(title="📅 Daily Bonus!", color=discord.Color.gold())
    embed.add_field(name="💰 Bonus",  value=f"**+${bonus:,}**",                             inline=True)
    embed.add_field(name="🔥 Streak", value=f"**{streak} day{'s' if streak>1 else ''}**",   inline=True)
    if streak >= 7: embed.add_field(name="🎉 Max Streak!", value="Week streak bonus active!", inline=False)
    if leveled:     embed.add_field(name="⭐ Level Up!",   value=f"Now level **{p['level']}**!", inline=False)
    if new_ach:     embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
    embed.set_footer(text="Come back tomorrow to keep your streak!")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="work", description="Open the jobs menu — 9 jobs across 3 tiers")
async def work_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    await interaction.response.send_message(embed=_job_menu_embed(p), view=JobMenuView(uid), ephemeral=True)

@bot.tree.command(name="crime", description="Pick your crime and pull it off (risky!)")
async def crime_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    p["uid_ref"] = uid
    await interaction.response.send_message(embed=_crime_menu_embed(p), view=CrimeMenuView(uid), ephemeral=True)

@bot.tree.command(name="bank", description="Deposit or withdraw from your bank")
async def bank_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    embed = discord.Embed(title="🏦 Your Bank", description=f"```\nWallet  ${p['money']:>10,}\nBank    ${p['bank']:>10,}\n```", color=discord.Color.blue())
    await interaction.response.send_message(embed=embed, view=BankView(uid), ephemeral=True)

@bot.tree.command(name="shop", description="Browse and buy items from the shop")
async def shop_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    await interaction.response.send_message(embed=_shop_embed(p), view=ShopView(uid), ephemeral=True)

@bot.tree.command(name="bail", description="Help reduce a jailed friend's sentence by 45 seconds")
@app_commands.describe(member="The jailed player to bail out")
async def bail_slash(interaction: discord.Interaction, member: discord.Member):
    uid  = str(interaction.user.id)
    tid  = str(member.id)
    now  = time.time()
    if tid == uid:
        return await interaction.response.send_message("❌ You can't bail yourself out — buy a ⛓️ Bail Bond from the shop.", ephemeral=True)
    p    = get_player(uid, interaction.user.display_name)
    if is_jailed(p) > 0:
        return await interaction.response.send_message("❌ You're in jail yourself — can't bail anyone out.", ephemeral=True)
    target = players.get(tid)
    if not target or is_jailed(target) <= 0:
        return await interaction.response.send_message(f"❌ **{member.display_name}** is not in jail.", ephemeral=True)
    bail_cd_key = f"bail_{tid}"
    cd_rem = cd_remaining(p, bail_cd_key, 600)
    if cd_rem > 0:
        return await interaction.response.send_message(f"⏳ Already bailed them recently. Wait `{fmt_cd(cd_rem)}`.", ephemeral=True)
    target["jailed_until"] = max(now, target["jailed_until"] - 45)
    p["cooldowns"][bail_cd_key] = now
    rem = is_jailed(target)
    save_data()
    msg = (f"✅ You helped bail out **{member.display_name}**! −45 seconds from their sentence."
           + (f"\n⛓️ They have `{fmt_cd(rem)}` remaining." if rem > 0 else "\n✅ They are now **free**!"))
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="leaderboard", description="View leaderboards — richest, most wanted, best gamblers, most active")
async def leaderboard_slash(interaction: discord.Interaction):
    uid, _ = await _ensure_user(interaction)
    guild = _get_guild(interaction, uid)
    await interaction.response.send_message(embed=leaderboard_embed(guild, "rich"), view=LeaderboardView(guild, uid), ephemeral=True)

@bot.tree.command(name="blackjack", description="Play solo Blackjack — type 'all' to bet everything")
async def blackjack_slash(interaction: discord.Interaction, bet: str):
    uid, p = await _ensure_user(interaction)
    if not use_energy(p, "blackjack"):
        eng = get_energy(p)
        return await interaction.response.send_message(f"⚡ Not enough energy! **{eng}/{MAX_ENERGY}** (need **{ENERGY_COST['blackjack']}**).", ephemeral=True)
    bet_amt = p["money"] if bet.lower() == "all" else (int(bet) if bet.isdigit() else -1)
    if bet_amt <= 0:         return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
    if bet_amt < 10:         return await interaction.response.send_message("❌ Min bet is **$10**.", ephemeral=True)
    if bet_amt > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
    p["money"] -= bet_amt; save_data()
    deck = make_deck(); ph = [deck.pop(), deck.pop()]; dh = [deck.pop(), deck.pop()]
    view = SoloBlackjackView(uid, bet_amt, ph, dh, deck)
    await interaction.response.send_message(embed=view.build_embed(interaction.user), view=view, ephemeral=True)

@bot.tree.command(name="scratch", description="Scratch a card for instant prizes — requires 🎫 Scratch Card from /shop")
async def scratch_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    if "scratch_card" not in p.get("items", []):
        return await interaction.response.send_message(
            "❌ You need a **🎫 Scratch Card**! Buy one at `/shop` for **$200**.", ephemeral=True)
    p["items"].remove("scratch_card")
    save_data()
    symbols = random.choices(SCRATCH_SYMBOLS, weights=SCRATCH_WEIGHTS, k=3)
    view    = ScratchCardView(uid, symbols)
    await interaction.response.send_message(embed=view._build_embed(0), view=view, ephemeral=True)


@bot.tree.command(name="mines", description="Play Mines — pick tiles, dodge bombs, cash out anytime")
async def mines_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    if uid in active_mines:
        return await interaction.response.send_message(
            embed=_mines_embed(active_mines[uid]),
            view=MinesGameView(uid), ephemeral=True
        )
    await interaction.response.send_modal(MinesBetModal(uid))


@bot.tree.command(name="chicken", description="Play Chicken Cross — cross lanes to multiply your bet, don't get squashed")
async def chicken_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    if uid in active_chicken:
        return await interaction.response.send_message(
            embed=_chicken_embed(active_chicken[uid]),
            view=ChickenCrossView(uid), ephemeral=True
        )
    await interaction.response.send_modal(ChickenBetModal(uid))


@bot.tree.command(name="slots", description="Spin the slot machine — type 'all' to bet everything")
async def slots_slash(interaction: discord.Interaction, bet: str):
    uid, p = await _ensure_user(interaction)
    if not use_energy(p, "slots"):
        eng = get_energy(p)
        return await interaction.response.send_message(f"⚡ Not enough energy! **{eng}/{MAX_ENERGY}** (need **{ENERGY_COST['slots']}**).", ephemeral=True)
    bet_amt = p["money"] if bet.lower() == "all" else (int(bet) if bet.isdigit() else -1)
    if bet_amt <= 0:         return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
    if bet_amt < 10:         return await interaction.response.send_message("❌ Min bet is **$10**.", ephemeral=True)
    if bet_amt > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
    if _casino_loss_blocked(p):
        return await interaction.response.send_message("🚫 Daily casino loss limit ($75,000) reached. Resets in 24h.", ephemeral=True)
    cs = _casino_session(p)
    if cs["slots"] >= CASINO_SLOTS_LIMIT:
        rem = int(cs["reset_at"] - time.time())
        return await interaction.response.send_message(f"🚫 Slots limit ({CASINO_SLOTS_LIMIT} spins/4h). Resets in `{fmt_cd(rem)}`.", ephemeral=True)
    result = _slots_result(bet_amt, uid)
    if result is None:
        return await interaction.response.send_message("🚫 Casino limit reached.", ephemeral=True)
    await interaction.response.send_message(embed=result, ephemeral=True)

@bot.tree.command(name="rps", description="Challenge someone to Rock Paper Scissors with a bet")
async def rps_slash(interaction: discord.Interaction, member: discord.Member, bet: str):
    uid = str(interaction.user.id); uid2 = str(member.id)
    if uid == uid2:   return await interaction.response.send_message("❌ Can't challenge yourself.", ephemeral=True)
    if member.bot:    return await interaction.response.send_message("❌ Can't challenge a bot.", ephemeral=True)
    p = get_player(uid, interaction.user.display_name)
    if not use_energy(p, "gamble"):
        eng = get_energy(p)
        return await interaction.response.send_message(f"⚡ Not enough energy! **{eng}/{MAX_ENERGY}** (need **{ENERGY_COST['gamble']}**).", ephemeral=True)
    bet_amt = p["money"] if bet.lower() == "all" else (int(bet) if bet.isdigit() else -1)
    if bet_amt <= 0:         return await interaction.response.send_message("❌ Invalid bet.", ephemeral=True)
    if bet_amt < 10:         return await interaction.response.send_message("❌ Min bet is **$10**.", ephemeral=True)
    if bet_amt > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
    p["money"] -= bet_amt; save_data()
    cid = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    rps_challenges[cid] = {
        "challenger": uid, "challenged": uid2, "bet": bet_amt,
        "challenger_choice": None, "challenged_choice": None,
        "accepted": False, "resolved": False,
        "channel_id": interaction.channel_id,
    }
    embed = discord.Embed(
        title="⚔️ RPS Challenge!",
        description=(f"<@{uid}> challenges <@{uid2}> to **Rock Paper Scissors**!\n"
                     f"💰 Pot: **${bet_amt*2:,}** — winner takes all!"),
        color=discord.Color.orange()
    )
    embed.add_field(name="Bet each side", value=f"**${bet_amt:,}**", inline=True)
    embed.add_field(name="How to play",   value="Challenger locks in → Opponent accepts & picks → Reveal!", inline=False)
    embed.set_footer(text="⏳ Expires in 2 minutes")
    await interaction.response.send_message(embed=embed, view=RpsChallengeView(cid))
    msg = await interaction.original_response()
    rps_challenges[cid]["msg_id"] = msg.id

@bot.tree.command(name="create", description="Create a multiplayer Blackjack room")
async def create_slash(interaction: discord.Interaction):
    uid, _ = await _ensure_user(interaction)
    for c, r in list(rooms.items()):
        if r["host"] == uid and r["state"] == "waiting": del rooms[c]
    code = gen_room_code()
    rooms[code] = {"host": uid, "players": [uid], "state": "waiting", "deck": [], "hands": {}, "bets": {},
                   "dealer_hand": [], "player_status": {}, "used_items": {}, "current_idx": 0,
                   "channel_id": interaction.channel_id, "game_mode": "blackjack"}
    save_data()
    msg = await interaction.channel.send(embed=room_embed(code, interaction.guild), view=RoomLobbyView(code))
    rooms[code]["message_id"] = msg.id
    await interaction.response.send_message(f"✅ Room `{code}` created! Friends: `/join {code}`", ephemeral=True)

@bot.tree.command(name="join", description="Join a multiplayer room by its code")
async def join_slash(interaction: discord.Interaction, code: str):
    uid, _ = await _ensure_user(interaction)
    code = code.upper(); room = rooms.get(code)
    if not room:                    return await interaction.response.send_message(f"❌ Room `{code}` not found.", ephemeral=True)
    if room["state"] != "waiting":  return await interaction.response.send_message("❌ Game already started.", ephemeral=True)
    if uid in room["players"]:      return await interaction.response.send_message("✅ Already in that room.", ephemeral=True)
    max_players = 8 if room.get("game_mode") == "roulette_pvt" else 6
    if len(room["players"]) >= max_players:
        return await interaction.response.send_message("❌ Room is full.", ephemeral=True)
    room["players"].append(uid); save_data()

    # Update the room lobby embed so everyone sees the new player
    if room.get("game_mode") == "roulette_pvt":
        try:
            channel = bot.get_channel(room["channel_id"])
            msg = await channel.fetch_message(room["message_id"])
            await msg.edit(embed=_pvt_rou_embed(code))
        except Exception: pass

    await interaction.response.send_message(f"✅ Joined roulette room `{code}`!" if room.get("game_mode") == "roulette_pvt" else f"✅ Joined room `{code}`!", ephemeral=True)

@bot.tree.command(name="prestige", description="Reset to Level 1 for permanent bonuses (requires Level 30)")
async def prestige_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    if p["level"] < PRESTIGE_LEVEL_REQ:
        return await interaction.response.send_message(
            f"❌ You need **Level {PRESTIGE_LEVEL_REQ}** to prestige. You are Level **{p['level']}**.", ephemeral=True)
    p["prestige"] = p.get("prestige", 0) + 1
    p["level"] = 1; p["xp"] = 0
    new_ach = check_achievements(p); save_data()
    prestige = p["prestige"]
    embed = discord.Embed(
        title=f"⭐ PRESTIGE {prestige}!",
        description=f"You sacrificed your level for **permanent power**!",
        color=discord.Color.gold()
    )
    embed.add_field(name="🔄 Reset",    value="Level → **1** | XP → **0**", inline=False)
    embed.add_field(name="⚡ Bonuses",  value=f"+**{int(prestige * PRESTIGE_JOB_BONUS * 100)}%** job pay\n+**{int(prestige * PRESTIGE_CRIME_BONUS * 100)}%** crime success", inline=False)
    embed.add_field(name="🏆 Badge",    value="⭐" * min(prestige, 5) + f"  Prestige **{prestige}**", inline=False)
    if prestige >= 5:
        embed.add_field(name="💎 Elite", value="You've reached **Elite Prestige 5+**!", inline=False)
    if new_ach:
        embed.add_field(name="🏆 Achievement Unlocked!", value=_ach_notify(new_ach), inline=False)
    embed.set_footer(text="Keep grinding — prestige stacks!")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="achievements", description="View your achievement progress — unlocked and locked")
async def achievements_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    unlocked = set(p.get("achievements", []))
    # Group by category
    cats: dict = {}
    for aid, ach in ACHIEVEMENTS.items():
        cat = ach["cat"]
        cats.setdefault(cat, []).append((aid, ach))
    total   = len(ACHIEVEMENTS)
    done    = len(unlocked)
    pct     = int(done / total * 100) if total else 0
    bar_len = 20
    filled  = int(bar_len * done / total) if total else 0
    bar     = "█" * filled + "░" * (bar_len - filled)
    embed = discord.Embed(
        title="🏆 Achievements",
        description=f"`{bar}` **{done}/{total}** ({pct}%)",
        color=discord.Color.gold()
    )
    for cat, items in cats.items():
        lines = []
        for aid, ach in items:
            if aid in unlocked:
                lines.append(f"{ach['emoji']} ~~{ach['name']}~~ ✅")
            else:
                lines.append(f"🔒 ~~{ach['name']}~~ — *{ach['desc']}*")
        embed.add_field(name=cat, value="\n".join(lines), inline=False)
    embed.set_footer(text="Achievements unlock automatically as you play!")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="challenges", description="View and claim your weekly challenge rewards")
async def challenges_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    _ensure_weekly(p)
    await interaction.response.send_message(embed=_weekly_embed(p), view=WeeklyChallengesView(uid), ephemeral=True)


@bot.tree.command(name="train", description="Permanently upgrade your stats — steal, crime, jobs, XP")
async def train_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    await interaction.response.send_message(embed=_training_embed(p), view=TrainingView(uid), ephemeral=True)


@bot.tree.command(name="career", description="Choose your permanent Career Path (unlocks at Level 10)")
async def career_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    await interaction.response.send_message(embed=_career_embed(p), view=CareerView(uid), ephemeral=True)


@bot.tree.command(name="lottery", description="Buy lottery tickets and check the jackpot — drawing every Sunday")
async def lottery_slash(interaction: discord.Interaction):
    uid, _ = await _ensure_user(interaction)
    await interaction.response.send_message(embed=_lottery_embed(), view=LotteryView(uid), ephemeral=True)


@bot.tree.command(name="property", description="Manage your properties — buy, collect income, and raid rivals")
async def property_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    await interaction.response.send_message(embed=_prop_embed(p), view=PropertyView(uid), ephemeral=True)


@bot.tree.command(name="business", description="Open your business empire — properties, income, and raids")
async def business_slash(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    props   = p.get("properties", [])
    slots   = len(props)
    income_6h = sum(
        int((PROPERTIES[pr["type"]]["income"][0] + PROPERTIES[pr["type"]]["income"][1]) / 2)
        for pr in props
    )
    ready_ct = sum(1 for pr in props if _prop_income_ready(pr))
    embed = discord.Embed(
        title="🏢 Business Empire",
        description=(
            f"**{slots}/{MAX_PROPERTIES}** property slots used\n"
            f"💰 Avg income per cycle: **~${income_6h:,}** every 6h\n"
            + (f"✅ **{ready_ct}** propert{'ies' if ready_ct != 1 else 'y'} ready to collect!**" if ready_ct else "⏳ No income ready yet.")
        ),
        color=discord.Color.blue()
    )
    if not props:
        embed.add_field(name="📭 No Properties Yet", value="Hit **Buy Property** to start earning passive income!", inline=False)
    else:
        for i, prop in enumerate(props):
            info  = PROPERTIES[prop["type"]]
            ready = _prop_income_ready(prop)
            nxt   = int(prop.get("last_income", 0) + info["cd"])
            embed.add_field(
                name=f"{info['emoji']} {info['name']} #{i+1}",
                value=("✅ **Ready!**" if ready else f"⏳ <t:{nxt}:R>"),
                inline=True
            )
    embed.set_footer(text="Buy up to 5 properties  •  Raid rivals to steal their income")
    await interaction.response.send_message(embed=embed, view=PropertyView(uid), ephemeral=True)


@bot.tree.command(name="steal", description="Attempt to steal money from another player's wallet")
async def steal_slash(interaction: discord.Interaction, member: discord.Member, amount: str):
    uid, p = await _ensure_user(interaction)
    uid2   = str(member.id)
    if uid == uid2:  return await interaction.response.send_message("❌ Can't steal from yourself.", ephemeral=True)
    if member.bot:   return await interaction.response.send_message("❌ Can't steal from a bot.", ephemeral=True)
    jail_rem = is_jailed(p)
    if jail_rem > 0: return await interaction.response.send_message(f"⛓️ You're in **JAIL**! Released in `{fmt_cd(jail_rem)}`.", ephemeral=True)
    if p["level"] < 3: return await interaction.response.send_message("❌ Need **Level 3** to steal.", ephemeral=True)
    rem = cd_remaining(p, "steal", STEAL_COOLDOWN)
    if rem > 0: return await interaction.response.send_message(f"⏳ Steal cooldown: `{fmt_cd(rem)}`", ephemeral=True)
    tgt_rem = cd_remaining(p, f"steal_target_{uid2}", TARGET_STEAL_COOLDOWN)
    if tgt_rem > 0: return await interaction.response.send_message(f"⏳ You recently targeted this player. Wait `{fmt_cd(tgt_rem)}`.", ephemeral=True)
    if "mask" not in p.get("items", []):
        return await interaction.response.send_message("❌ Need a **🥷 Mask** to steal! Buy one at `/shop`.", ephemeral=True)
    t2 = get_player(uid2, member.display_name)
    if t2["money"] <= 0: return await interaction.response.send_message("❌ That player has nothing in their wallet.", ephemeral=True)
    if not use_energy(p, "steal"):
        eng = get_energy(p)
        return await interaction.response.send_message(f"⚡ Not enough energy! **{eng}/{MAX_ENERGY}** (need **{ENERGY_COST['steal']}**).", ephemeral=True)

    await _execute_steal(interaction, uid, uid2, member, amount.strip().lower(), interaction.user.display_name)


@bot.tree.command(name="scan", description="Scan a target to estimate your steal success chance (requires 🔭 Scanner)")
async def scan_slash(interaction: discord.Interaction, member: discord.Member):
    uid, p = await _ensure_user(interaction)
    uid2   = str(member.id)
    if uid == uid2: return await interaction.response.send_message("❌ Can't scan yourself.", ephemeral=True)
    if p["level"] < 3: return await interaction.response.send_message("❌ Need **Level 3** to scan.", ephemeral=True)
    if "scanner" not in p.get("items", []):
        return await interaction.response.send_message("❌ Need a **🔭 Scanner** to scan! Buy one at `/shop`.", ephemeral=True)
    t2 = get_player(uid2, member.display_name)
    revenge  = p.get("flags", {}).get("active_revenge") == uid2
    base_c   = steal_chance(p, t2, False, False, revenge)
    mask_c   = steal_chance(p, t2, True,  False, revenge)
    gloves_c = steal_chance(p, t2, False, True,  revenge)
    shield   = "insurance_shield" in t2.get("items", [])
    has_mask   = "mask"   in p.get("items", [])
    has_gloves = "gloves" in p.get("items", [])
    p["items"].remove("scanner"); save_data()
    embed = discord.Embed(title=f"🔍 Scan — {member.display_name}", color=discord.Color.blurple())
    embed.add_field(name="💵 Wallet",       value=f"**${t2['money']:,}**" if t2["money"] > 0 else "**$0**", inline=True)
    embed.add_field(name="⭐ Level",         value=f"**{t2['level']}**",                                     inline=True)
    embed.add_field(name="🛡️ Shield",       value="⚠️ Active" if shield else "None",                        inline=True)
    embed.add_field(name="🎲 Base Chance",   value=f"**{int(base_c*100)}%**",                                inline=True)
    if has_mask:   embed.add_field(name="🥷 With Mask",   value=f"**{int(mask_c*100)}%**",   inline=True)
    if has_gloves: embed.add_field(name="🧤 With Gloves", value=f"**{int(gloves_c*100)}%**", inline=True)
    if revenge:    embed.add_field(name="🔥 Revenge",     value="+20% ✅",                    inline=True)
    embed.add_field(name="💰 Max Steal",    value=f"**${max(10, int(t2['money']*0.30)):,}**", inline=True)
    embed.set_footer(text="🔭 Scanner used — /steal costs 20 energy + 🥷 Mask + 15min cooldown")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="revenge", description="Activate +20% steal bonus against the last person who robbed you (requires 📡 Tracker)")
async def revenge_slash(interaction: discord.Interaction, member: discord.Member):
    uid, p = await _ensure_user(interaction)
    uid2   = str(member.id)
    if uid == uid2: return await interaction.response.send_message("❌ Can't revenge yourself.", ephemeral=True)
    stolen_from = p.get("flags", {}).get("stolen_from_by")
    if not stolen_from:
        return await interaction.response.send_message("❌ Nobody has stolen from you recently.", ephemeral=True)
    if stolen_from != uid2:
        return await interaction.response.send_message(f"❌ **{member.display_name}** hasn't stolen from you. Your thief is someone else.", ephemeral=True)
    if "tracker" not in p.get("items", []):
        return await interaction.response.send_message("❌ Need a **📡 Tracker** to activate revenge! Buy one at `/shop`.", ephemeral=True)
    p["items"].remove("tracker")
    p.setdefault("flags", {})["active_revenge"] = uid2
    p["flags"].pop("stolen_from_by", None)
    save_data()
    embed = discord.Embed(title="🔥 Revenge Activated!", description=f"Your next `/steal` against **{member.display_name}** gets **+20% success chance**!", color=discord.Color.red())
    embed.set_footer(text="📡 Tracker used — bonus consumed on your next steal against them")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="duel", description="Challenge someone to a strategic duel — winner takes the pot")
async def duel_slash(interaction: discord.Interaction, member: discord.Member, bet: str):
    uid = str(interaction.user.id); uid2 = str(member.id)
    if uid == uid2: return await interaction.response.send_message("❌ Can't duel yourself.", ephemeral=True)
    if member.bot:  return await interaction.response.send_message("❌ Can't duel a bot.", ephemeral=True)
    p = get_player(uid, interaction.user.display_name)
    bet_amt = p["money"] if bet.lower() == "all" else (int(bet) if bet.isdigit() else -1)
    if bet_amt <= 0:         return await interaction.response.send_message("❌ Invalid bet.", ephemeral=True)
    if bet_amt < 10:         return await interaction.response.send_message("❌ Min bet is **$10**.", ephemeral=True)
    if bet_amt > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}**.", ephemeral=True)
    p["money"] -= bet_amt; save_data()
    did = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    duels[did] = {
        "challenger": uid, "challenged": uid2, "bet": bet_amt,
        "challenger_move": None, "challenged_move": None,
        "accepted": False, "resolved": False, "channel_id": interaction.channel_id,
    }
    embed = discord.Embed(
        title="⚔️ Duel Challenge!",
        description=(f"<@{uid}> challenges <@{uid2}> to a **strategic duel**!\n"
                     f"💰 Pot: **${bet_amt*2:,}** — winner takes all!"),
        color=discord.Color.orange()
    )
    embed.add_field(name="⚔️ Attack",  value="Beats 🛡️ Defend",  inline=True)
    embed.add_field(name="🛡️ Defend",  value="Beats 🌀 Special", inline=True)
    embed.add_field(name="🌀 Special", value="Beats ⚔️ Attack",  inline=True)
    embed.set_footer(text="⏳ Expires in 2 minutes")
    await interaction.response.send_message(embed=embed, view=DuelChallengeView(did))
    msg = await interaction.original_response()
    duels[did]["msg_id"] = msg.id
    await interaction.followup.send("🤫 Your move (secret — pick now!):", view=DuelPickView(did, "challenger"), ephemeral=True)


@bot.tree.command(name="help", description="Show all available commands")
async def help_slash(interaction: discord.Interaction):
    embed = discord.Embed(title="🎰 LifeBot — Commands", color=discord.Color.dark_green())
    embed.add_field(name="🎮 Game Hub",    value="`/play` — Private game menu\n`/blackjack <bet>` — Solo Blackjack\n`/slots <bet>` — Slot machine\n`/create` / `/join <code>` — Multiplayer rooms\n`/rps @user <bet>` — PvP Rock Paper Scissors", inline=False)
    embed.add_field(name="💼 Economy",     value="`/work` — Jobs menu (9 jobs, 3 tiers)\n`/crime` — Risk it for $200–700\n`/daily` — Daily bonus + streak\n`/bank` — Deposit/withdraw\n`/shop` — Buy items\n`/leaderboard` — Top 10", inline=False)
    embed.add_field(name="🥷 Steal System",
        value=("`/steal @user <amt>` — Steal up to 30% of wallet (Lv3+, 20 energy, 🥷 Mask, 15m CD)\n"
               "`/scan @user` — Preview steal success chance — requires **🔭 Scanner** ($400)\n"
               "`/revenge @user` — +20% steal bonus vs your last thief — requires **📡 Tracker** ($350)\n"
               "`/duel @user <bet>` — PvP: Attack / Defend / Special — winner takes the pot"),
        inline=False)
    embed.add_field(name="🏴 Gang System",
        value=("`/gang create` — Found a gang ($2,000, Lv5+) • choose **public** or **private**\n"
               "`/gang join <name>` — Join public gang instantly, or request private\n"
               "`/gang invite @user` — Invite via DM • `/gang accept` / `/gang decline`\n"
               "`/gang approve @user` / `/gang deny @user` — Handle join requests\n"
               "`/gang kick/promote/demote/transfer @user` — Manage members\n"
               "`/gang deposit/withdraw <amt>` — Gang treasury • `/gang privacy` — Toggle public/private\n"
               "`/gang heist` — Share loot with all members (Lv3+, 6h CD)\n"
               "`/gang war @user` — Declare war (Lv5+) • `/gang attack` — Fight once/hour\n"
               "`/gang info [name]` — View gang • `/gang leaderboard` — Top gangs"),
        inline=False)
    embed.add_field(name="🗺️ Territory System",
        value=("`/chest` — Open a chest ($1,500 from treasury) → random territory  *(leader/officer only)*\n"
               "  ⬜ Common · 🟦 Rare · 🟣 Epic · 🟡 Legendary · 🔴 Extra Legendary\n"
               "  **20 zones** — from 🏚️ Slums to 👁️ Shadow Empire (+75% all income)\n"
               "  • 1 territory per gang · lasts 12–24h · rivals can attack to steal it\n"
               "`/territory list` — Full map with owners, rarity & expiry timers\n"
               "`/territory info <zone>` — Perks, income, defense, battle status\n"
               "`/territory attack <zone>` — 30-min battle to seize a zone (leader/officer, 2h CD)\n"
               "`/territory contribute <zone>` — Add attack/defense points (1h CD per member)\n"
               "`/territory collect <zone>` — Collect passive income into treasury (6h CD)\n"
               "`/territory leaderboard` — Top gangs by territories held"),
        inline=False)
    embed.add_field(name="🃏 Blackjack",   value="Beat dealer without busting **21**  •  BJ pays **3:2**  •  Dealer draws to **17**", inline=False)
    embed.add_field(name="🎰 Slots Pays",  value="🍒🍒🍒=2x  🍋/🍊🍊🍊=3x  🍇🍇🍇=4x\n⭐⭐⭐=6x  💎💎💎=12x  7️⃣7️⃣7️⃣=25x  •  Jackpot: 0.5%", inline=False)
    embed.add_field(name="🌡️ Heat & Jail", value="Crime/steal raises heat → higher catch chance (up to 80%)\nCaught stealing at heat ≥7 → ⛓️ **JAIL** (heat×60s)\nReduce: `/daily` −2  •  🧊 heat_shield −3  •  ⛓️ bail_bond instant", inline=False)
    embed.set_footer(text=f"Starting balance: ${STARTING_MONEY:,}  •  All responses are private")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ================================================================
# GANG SYSTEM — MODALS, VIEWS, COMMANDS
# ================================================================

class GangCreateModal(Modal, title="🏴 Create a Gang"):
    name_   = TextInput(label="Gang Name",   placeholder="The Shadow Wolves",                   max_length=30)
    tag     = TextInput(label="Gang Tag",    placeholder="[WLF]",                               max_length=6)
    emoji_  = TextInput(label="Gang Emoji",  placeholder="🐺",                                  max_length=4)
    color   = TextInput(label="Color (red/blue/green/gold/purple/orange)", placeholder="red",   max_length=10)
    privacy = TextInput(label="Privacy (public / private)",                placeholder="public", max_length=10)

    def __init__(self, user_id): super().__init__(); self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        uid = self.user_id; p = get_player(uid, interaction.user.display_name)
        if p.get("gang_id"): return await interaction.response.send_message("❌ Already in a gang. `/gang leave` first.", ephemeral=True)
        valid_colors = ["red","blue","green","gold","purple","orange"]
        col     = self.color.value.strip().lower() if self.color.value.strip().lower() in valid_colors else "red"
        priv    = "private" if self.privacy.value.strip().lower() == "private" else "public"
        name_   = self.name_.value.strip(); tag_ = self.tag.value.strip(); emj = self.emoji_.value.strip() or "🏴"
        if any(g.get("name","").lower() == name_.lower() for g in gangs.values()):
            return await interaction.response.send_message("❌ A gang with that name already exists.", ephemeral=True)
        gid = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        gangs[gid] = {
            "name": name_, "tag": tag_, "emoji": emj, "color": col, "privacy": priv,
            "leader": uid, "officers": [], "members": [uid], "join_requests": [],
            "bank": 0, "xp": 0, "created_at": time.time(),
            "at_war_with": None, "war_ends_at": 0, "war_wins": 0, "war_losses": 0,
            "heist_cooldown": 0,
        }
        p["money"] -= GANG_CREATE_COST; p["gang_id"] = gid
        save_data(); save_gangs()
        priv_icon = "🔓 Public" if priv == "public" else "🔒 Private"
        embed = _make_gang_embed(gangs[gid], gid, interaction.guild)
        embed.description = f"✅ Gang created! **${GANG_CREATE_COST:,}** deducted  •  {priv_icon}\n" + (embed.description or "")
        await interaction.response.send_message(embed=embed, ephemeral=True)


# ── Gang Panel — helper embeds ────────────────────────────────────

def _gang_resources_embed(g: dict, gid: str) -> discord.Embed:
    owned = [(tid, territories.get(tid, {})) for tid in TERRITORIES if territories.get(tid, {}).get("owner_gid") == gid]
    embed = discord.Embed(
        title=f"💰 Resources — {g.get('emoji','')} {g['name']}",
        color=_gang_color_obj(g)
    )
    embed.add_field(name="🏦 Treasury",   value=f"**${g.get('bank',0):,}**",                                    inline=True)
    embed.add_field(name="👥 Members",    value=f"**{len(g.get('members',[]))}/{_gang_max_members(g)}**",        inline=True)
    embed.add_field(name="⭐ Gang Level", value=f"Lv.**{_gang_level(g)}**  •  {g.get('xp',0):,} XP",           inline=True)
    if owned:
        tid, td = owned[0]; tinfo = TERRITORIES[tid]; now = time.time()
        last = td.get("last_income", 0); cd_rem = max(0, int(tinfo["income_cd"] - (now - last)))
        income_status = "✅ Ready" if cd_rem == 0 else f"⏳ {cd_rem//3600}h {(cd_rem%3600)//60}m"
        embed.add_field(
            name=f"🗺️ Territory — {tinfo['emoji']} {tinfo['name']}",
            value=(f"💰 **${tinfo['income'][0]:,}–${tinfo['income'][1]:,}/6h**  •  {income_status}\n"
                   "⚡ " + " | ".join(tinfo.get("perks", []))),
            inline=False
        )
    else:
        embed.add_field(name="🗺️ Territory", value="*None held — open a chest to claim one*", inline=False)
    perks = _gang_perks(g)
    if perks:
        embed.add_field(name="⚡ Gang Perks", value="\n".join(perks), inline=False)
    embed.set_footer(text=f"Chest cost ${CHEST_COST:,} from treasury  •  War stake: {int(GANG_WAR_STAKE*100)}% of treasury")
    return embed


def _gang_war_embed(g: dict, gid: str) -> discord.Embed:
    if not g.get("at_war_with"):
        embed = discord.Embed(
            title=f"⚔️ War Panel — {g.get('emoji','')} {g['name']}",
            description=(
                "☮️ Your gang is **not at war**.\n\n"
                "Use the dropdown below to declare war on a rival gang.\n"
                f"Winner claims **{int(GANG_WAR_STAKE*100)}%** of the loser's treasury.\n"
                "*Requires both gangs to be Gang Level 5+.*"
            ),
            color=discord.Color.dark_gray()
        )
        embed.set_footer(text="War lasts 24 hours · 1 attack per member per hour")
        return embed
    eid  = g["at_war_with"]; eg = gangs.get(eid, {})
    wend = g.get("war_ends_at", 0); now = time.time(); ended = now > wend
    gw = g.get("war_wins", 0); gl = g.get("war_losses", 0)
    ew = eg.get("war_wins", 0); el = eg.get("war_losses", 0)
    total = max(1, gw + gl + ew + el)
    g_pct = min(10, int(gw / total * 10)); e_pct = min(10, int(ew / total * 10))
    g_bar = "🟩" * g_pct + "⬛" * (10 - g_pct)
    e_bar = "🟥" * e_pct + "⬛" * (10 - e_pct)
    embed = discord.Embed(
        title=f"⚔️ {g.get('emoji','')} {g['name']}  vs  {eg.get('emoji','')} {eg.get('name','?')}",
        description=f"**Status:** {'🔴 Ended' if ended else '🟢 Active'}",
        color=discord.Color.dark_gray() if ended else discord.Color.red()
    )
    embed.add_field(name=f"{g.get('emoji','')} {g['name']}",           value=f"`{g_bar}`\n**{gw}W — {gl}L**", inline=True)
    embed.add_field(name="​",                                       value="**⚔️**",                       inline=True)
    embed.add_field(name=f"{eg.get('emoji','')} {eg.get('name','?')}", value=f"`{e_bar}`\n**{ew}W — {el}L**", inline=True)
    if not ended:
        embed.add_field(name="⏳ War Ends", value=f"<t:{int(wend)}:R>", inline=False)
        embed.set_footer(text="Use /gang attack to fight! 1 attack per hour per member")
    else:
        embed.set_footer(text="War ended — use /gang attack or check war panel to resolve")
    return embed


# ── Gang Panel — shared back helper ──────────────────────────────

class GangPanelBackView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=60); self.user_id = user_id

    @discord.ui.button(label="🔙 Back to Gang", style=discord.ButtonStyle.gray)
    async def back_btn(self, interaction, button):
        if str(interaction.user.id) != self.user_id:
            return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if g:
            await interaction.response.edit_message(embed=_make_gang_embed(g, gid, interaction.guild), view=GangMenuView(self.user_id))
        else:
            p = get_player(self.user_id, interaction.user.display_name)
            await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


# ── Gang Bank ─────────────────────────────────────────────────────

class GangBankView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.button(label="📥 Deposit", style=discord.ButtonStyle.green, row=0)
    async def deposit_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        p = get_player(self.user_id)
        if p["money"] <= 0: return await interaction.response.send_message("❌ No money to deposit.", ephemeral=True)
        await interaction.response.send_modal(_GangDepositModal(self.user_id, gid))

    @discord.ui.button(label="📤 Withdraw", style=discord.ButtonStyle.danger, row=0)
    async def withdraw_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can withdraw.", ephemeral=True)
        await interaction.response.send_modal(_GangWithdrawModal(uid, gid))

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if g:
            await interaction.response.edit_message(embed=_make_gang_embed(g, gid, interaction.guild), view=GangMenuView(self.user_id))
        else:
            p = get_player(self.user_id, interaction.user.display_name)
            await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


# ── War Panel ─────────────────────────────────────────────────────

class GangWarView(View):
    def __init__(self, user_id: str, gid: str):
        super().__init__(timeout=90); self.user_id = user_id; self.gid = gid
        g = gangs.get(gid, {})
        is_officer = g.get("leader") == user_id or user_id in g.get("officers", [])
        can_declare = is_officer and any("War" in p_ for p_ in _gang_perks(g)) and not g.get("at_war_with")
        if can_declare:
            opts = []
            for egid, eg in gangs.items():
                if egid == gid or eg.get("at_war_with"): continue
                if not any("War" in p_ for p_ in _gang_perks(eg)): continue
                label = f"{eg.get('emoji','')} {eg['name']} (Lv.{_gang_level(eg)})".strip()[:25]
                desc  = f"{len(eg.get('members',[]))} members · ${eg.get('bank',0):,} treasury"[:50]
                opts.append(discord.SelectOption(label=label, value=egid, description=desc))
            if opts:
                sel = discord.ui.Select(placeholder="⚔️ Choose a gang to declare war on…", options=opts[:25], row=0)
                sel.callback = self._declare_cb
                self.add_item(sel)

    def _ok(self, i): return str(i.user.id) == self.user_id

    async def _declare_cb(self, interaction: discord.Interaction):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        g = gangs.get(self.gid)
        if not g: return await interaction.response.send_message("❌ Gang not found.", ephemeral=True)
        uid = self.user_id
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can declare war.", ephemeral=True)
        if g.get("at_war_with"): return await interaction.response.send_message("❌ Already at war!", ephemeral=True)
        egid = interaction.data["values"][0]; eg = gangs.get(egid)
        if not eg: return await interaction.response.send_message("❌ Target gang not found.", ephemeral=True)
        if eg.get("at_war_with"): return await interaction.response.send_message("❌ That gang is already at war.", ephemeral=True)
        if not any("War" in p_ for p_ in _gang_perks(eg)):
            return await interaction.response.send_message("❌ That gang hasn't reached Level 5 yet.", ephemeral=True)
        wend = time.time() + GANG_WAR_DURATION
        for gg_, eid_ in [(g, egid), (eg, self.gid)]:
            gg_["at_war_with"] = eid_; gg_["war_ends_at"] = wend; gg_["war_wins"] = 0; gg_["war_losses"] = 0
        save_gangs()
        await interaction.response.edit_message(embed=_gang_war_embed(g, self.gid), view=GangWarView(uid, self.gid))

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=4)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if g:
            await interaction.response.edit_message(embed=_make_gang_embed(g, gid, interaction.guild), view=GangMenuView(self.user_id))
        else:
            p = get_player(self.user_id, interaction.user.display_name)
            await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


# ── Requests Panel ────────────────────────────────────────────────

class GangRequestsView(View):
    def __init__(self, user_id: str, gid: str, reqs: list, idx: int = 0):
        super().__init__(timeout=90)
        self.user_id = user_id; self.gid = gid; self.reqs = list(reqs); self.idx = idx

    def _ok(self, i): return str(i.user.id) == self.user_id

    def _embed(self) -> discord.Embed:
        g = gangs.get(self.gid, {}); req_uid = self.reqs[self.idx]
        rp = players.get(req_uid, {}); name = rp.get("name", f"Player {req_uid[:4]}")
        embed = discord.Embed(
            title=f"📩 Join Request  ({self.idx + 1}/{len(self.reqs)})",
            description=f"**{name}** wants to join **{g.get('emoji','')} {g['name']}**",
            color=_gang_color_obj(g)
        )
        embed.add_field(name="⭐ Level",  value=f"**{rp.get('level', 1)}**",                       inline=True)
        embed.add_field(name="💵 Wallet", value=f"**${rp.get('money', 0):,}**",                    inline=True)
        embed.add_field(name="🏦 Bank",   value=f"**${rp.get('bank', 0):,}**",                     inline=True)
        embed.add_field(name="🏆 Wins",   value=f"**{rp.get('stats',{}).get('wins',0)}**",         inline=True)
        embed.add_field(name="🦹 Crimes", value=f"**{rp.get('stats',{}).get('crimes_done',0)}**",  inline=True)
        embed.set_footer(text=f"Request {self.idx+1} of {len(self.reqs)}  •  Only visible to you")
        return embed

    @discord.ui.button(label="✅ Accept", style=discord.ButtonStyle.green, row=0)
    async def accept_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        g = gangs.get(self.gid)
        if not g: return await interaction.response.send_message("❌ Gang not found.", ephemeral=True)
        req_uid = self.reqs[self.idx]; reqs = g.setdefault("join_requests", [])
        if req_uid not in reqs: return await interaction.response.send_message("❌ Request no longer valid.", ephemeral=True)
        if len(g.get("members", [])) >= _gang_max_members(g):
            return await interaction.response.send_message(f"❌ Gang is full ({_gang_max_members(g)} members max).", ephemeral=True)
        p2 = get_player(req_uid)
        if p2.get("gang_id"):
            reqs.remove(req_uid); save_gangs()
            return await interaction.response.send_message("❌ Player already joined another gang.", ephemeral=True)
        reqs.remove(req_uid); g.setdefault("members", []).append(req_uid); p2["gang_id"] = self.gid
        add_gang_xp(g, 50); save_data(); save_gangs()
        name = p2.get("name", f"Player {req_uid[:4]}")
        self.reqs.pop(self.idx)
        if not self.reqs:
            await interaction.response.edit_message(
                embed=discord.Embed(title="📩 All Requests Handled", description="No more pending requests.", color=discord.Color.green()),
                view=GangPanelBackView(self.user_id))
        else:
            self.idx = min(self.idx, len(self.reqs) - 1)
            await interaction.response.edit_message(embed=self._embed(), view=self)
        await interaction.followup.send(f"✅ **{name}** has been accepted into the gang!", ephemeral=True)

    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.danger, row=0)
    async def decline_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        g = gangs.get(self.gid)
        if not g: return await interaction.response.send_message("❌ Gang not found.", ephemeral=True)
        req_uid = self.reqs[self.idx]; reqs = g.setdefault("join_requests", [])
        if req_uid in reqs: reqs.remove(req_uid)
        save_gangs(); name = players.get(req_uid, {}).get("name", f"Player {req_uid[:4]}")
        self.reqs.pop(self.idx)
        if not self.reqs:
            await interaction.response.edit_message(
                embed=discord.Embed(title="📩 All Requests Handled", description="No more pending requests.", color=discord.Color.blurple()),
                view=GangPanelBackView(self.user_id))
        else:
            self.idx = min(self.idx, len(self.reqs) - 1)
            await interaction.response.edit_message(embed=self._embed(), view=self)
        await interaction.followup.send(f"❌ **{name}**'s request declined.", ephemeral=True)

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.gray, row=1)
    async def prev_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        if self.idx > 0: self.idx -= 1
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="▶ Next", style=discord.ButtonStyle.gray, row=1)
    async def next_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        if self.idx < len(self.reqs) - 1: self.idx += 1
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if g:
            await interaction.response.edit_message(embed=_make_gang_embed(g, gid, interaction.guild), view=GangMenuView(self.user_id))
        else:
            p = get_player(self.user_id, interaction.user.display_name)
            await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


# ── Invite Panel ──────────────────────────────────────────────────

class GangInviteView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=60); self.user_id = user_id

    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="👥 Select a player to invite…", min_values=1, max_values=1, row=0)
    async def invite_select(self, interaction, select):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can send invites.", ephemeral=True)
        member = select.values[0]; uid2 = str(member.id)
        if uid2 == uid: return await interaction.response.send_message("❌ Can't invite yourself.", ephemeral=True)
        if member.bot:  return await interaction.response.send_message("❌ Can't invite bots.", ephemeral=True)
        p2 = get_player(uid2, member.display_name)
        if p2.get("gang_id"):
            return await interaction.response.send_message(f"❌ **{member.display_name}** is already in a gang.", ephemeral=True)
        if len(g.get("members", [])) >= _gang_max_members(g):
            return await interaction.response.send_message(f"❌ Gang is full ({_gang_max_members(g)} members max).", ephemeral=True)
        p2.setdefault("flags", {})["gang_invite"] = {"gid": gid, "expires": time.time() + GANG_INVITE_TTL}
        save_data()
        dm_embed = discord.Embed(
            title=f"📩 Gang Invite — {g.get('emoji','')} {g['name']}",
            description=(f"You've been invited to join **{g.get('emoji','')} {g['name']}**!\n"
                         f"Use `/gang accept` to join or `/gang decline` to refuse.\n⏳ Expires in 1 hour."),
            color=_gang_color_obj(g)
        )
        try:
            await member.send(embed=dm_embed)
            await interaction.response.send_message(f"✅ Invite sent to **{member.display_name}** via DM!", ephemeral=True)
        except Exception:
            await interaction.response.send_message(
                f"✅ Invite recorded for **{member.display_name}** (DMs closed — they can use `/gang accept`).", ephemeral=True)

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if g:
            await interaction.response.edit_message(embed=_make_gang_embed(g, gid, interaction.guild), view=GangMenuView(self.user_id))
        else:
            p = get_player(self.user_id, interaction.user.display_name)
            await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


# ── Heist Execution ───────────────────────────────────────────────
async def _execute_gang_heist(uid: str, tier_key: str, role_key: str, g: dict, gid: str) -> discord.Embed:
    t    = HEIST_TIERS[tier_key]
    role = HEIST_ROLES[role_key]

    # Build effective stats from role
    fail_chance  = max(0.05, t["fail_chance"] - role["fail_reduce"])
    loot_mult    = 1.0 + role["loot_bonus"]
    cd_mult      = 1.0 - role["cd_reduce"]
    guard_active = role["guard"]

    success = random.random() > fail_chance

    members     = g.get("members", [])
    mc          = max(1, len(members))
    new_ach_map = {}

    if success:
        base_loot = random.randint(*t["loot"])
        total     = int(base_loot * (1 + mc * 0.10))   # bonus per extra member
        share     = int((total // mc) * loot_mult)      # bag_man applies to leader share

        for mid in members:
            mp = players.get(mid)
            if not mp:
                continue
            mp["money"] += share if mid == uid else total // mc
            mp["stats"]["heists_done"] = mp["stats"].get("heists_done", 0) + 1
            _weekly_inc(mp, "heists")
            new_ach_map[mid] = check_achievements(mp)

        effective_cd = int(t["cd"] * cd_mult)
        g.setdefault("heist_cds", {})[tier_key] = time.time() + effective_cd
        add_gang_xp(g, t["gang_xp"])
        save_data(); save_gangs()

        embed = discord.Embed(
            title=f"✅ {t['emoji']} {t['name']} — SUCCESS!",
            color=discord.Color.green()
        )
        embed.add_field(name="💰 Total Loot",    value=f"**${total:,}**",                      inline=True)
        embed.add_field(name="👥 Members",        value=f"**{mc}**",                             inline=True)
        embed.add_field(name="💵 Your Share",     value=f"**+${share:,}**",                     inline=True)
        embed.add_field(name=f"{role['emoji']} Role", value=role["name"],                       inline=True)
        embed.add_field(name="⭐ Gang XP",        value=f"+**{t['gang_xp']}**",                 inline=True)
        embed.add_field(name="⏱️ Next Heist",     value=fmt_cd(effective_cd),                   inline=True)
        my_ach = new_ach_map.get(uid, [])
        if my_ach:
            embed.add_field(name="🏆 Achievement!", value=_ach_notify(my_ach),                  inline=False)
    else:
        penalty = t["penalty"]
        actual_penalty = penalty // 2 if guard_active else penalty
        g["bank"] = max(0, g.get("bank", 0) - actual_penalty)
        g.setdefault("heist_cds", {})[tier_key] = time.time() + t["cd"]
        save_data(); save_gangs()

        embed = discord.Embed(
            title=f"❌ {t['emoji']} {t['name']} — FAILED!",
            color=discord.Color.red()
        )
        embed.add_field(name="💸 Gang Bank Penalty", value=f"**-${actual_penalty:,}**",         inline=True)
        embed.add_field(name=f"{role['emoji']} Role",value=role["name"],                        inline=True)
        if guard_active:
            embed.add_field(name="🛡️ Guard",         value="Penalty halved!",                   inline=True)
        embed.add_field(name="⏱️ Next Heist",        value=fmt_cd(t["cd"]),                     inline=True)
        embed.set_footer(text="Tip: A Hacker reduces fail chance next time")

    return embed


# ── Heist Tier Select ─────────────────────────────────────────────
def _heist_tier_embed(g: dict, gid: str) -> discord.Embed:
    embed = discord.Embed(
        title="🎯 Gang Heist — Choose Your Target",
        description="Pick a target based on your gang's size and level. Higher risk = bigger reward.",
        color=discord.Color.dark_red()
    )
    mc        = len(g.get("members", []))
    glv       = _gang_level(g)
    heist_cds = g.get("heist_cds", {})
    for key, t in HEIST_TIERS.items():
        rem      = max(0, int(heist_cds.get(key, 0) - time.time()))
        cd_txt   = f"⏳ `{fmt_cd(rem)}`" if rem > 0 else "✅ Ready"
        locked   = mc < t["min_members"] or glv < t["min_gang_level"]
        lock_txt = ""
        if glv < t["min_gang_level"]:
            lock_txt = f"  🔒 Gang Lv{t['min_gang_level']} required"
        elif mc < t["min_members"]:
            lock_txt = f"  🔒 Need {t['min_members']} members (have {mc})"
        loot_min, loot_max = t["loot"]
        embed.add_field(
            name=f"{t['emoji']} {t['name']}{lock_txt}",
            value=(
                f"💰 Loot: **${loot_min:,}–${loot_max:,}**\n"
                f"👥 Members: **{t['min_members']}+**  •  ❌ Fail: **{int(t['fail_chance']*100)}%**\n"
                f"⏱️ CD: **{fmt_cd(t['cd'])}**  •  {cd_txt}"
            ),
            inline=False
        )
    embed.set_footer(text=f"Gang Level {glv}  •  {mc} members  •  Roles reduce fail chance & boost loot")
    return embed


class HeistTierSelectView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=60)
        self.user_id = user_id
        gid, g = _find_player_gang(user_id)
        if not g:
            return
        mc        = len(g.get("members", []))
        glv       = _gang_level(g)
        heist_cds = g.get("heist_cds", {})
        for btn in [self.corner_btn, self.bank_btn, self.vault_btn, self.federal_btn]:
            key = btn.custom_id
            t   = HEIST_TIERS[key]
            rem = max(0, int(heist_cds.get(key, 0) - time.time()))
            btn.disabled = rem > 0 or mc < t["min_members"] or glv < t["min_gang_level"]

    def _ok(self, i): return str(i.user.id) == self.user_id

    async def _pick_tier(self, interaction: discord.Interaction, tier_key: str):
        if not self._ok(interaction):
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        uid = self.user_id
        gid, g = _find_player_gang(uid)
        if not g:
            return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can launch heists.", ephemeral=True)
        t   = HEIST_TIERS[tier_key]
        mc  = len(g.get("members", []))
        glv = _gang_level(g)
        rem = max(0, int(g.get("heist_cds", {}).get(tier_key, 0) - time.time()))
        if rem > 0:
            return await interaction.response.send_message(f"⏳ Cooldown: `{fmt_cd(rem)}`", ephemeral=True)
        if mc < t["min_members"]:
            return await interaction.response.send_message(f"❌ Need **{t['min_members']}** members.", ephemeral=True)
        if glv < t["min_gang_level"]:
            return await interaction.response.send_message(f"❌ Need Gang Level **{t['min_gang_level']}**.", ephemeral=True)
        await interaction.response.edit_message(
            embed=_heist_role_embed(tier_key, g),
            view=HeistRoleView(uid, tier_key)
        )

    @discord.ui.button(label="🏪 Corner Store",    style=discord.ButtonStyle.green,  row=0, custom_id="corner_store")
    async def corner_btn(self, interaction, button):
        await self._pick_tier(interaction, "corner_store")

    @discord.ui.button(label="🏦 City Bank",       style=discord.ButtonStyle.blurple, row=0, custom_id="city_bank")
    async def bank_btn(self, interaction, button):
        await self._pick_tier(interaction, "city_bank")

    @discord.ui.button(label="💎 Diamond Vault",   style=discord.ButtonStyle.danger,  row=1, custom_id="diamond_vault")
    async def vault_btn(self, interaction, button):
        await self._pick_tier(interaction, "diamond_vault")

    @discord.ui.button(label="🚀 Federal Reserve", style=discord.ButtonStyle.danger,  row=1, custom_id="federal_reserve")
    async def federal_btn(self, interaction, button):
        await self._pick_tier(interaction, "federal_reserve")

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=2)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction):
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        await interaction.response.edit_message(
            embed=_make_gang_embed(g, gid, interaction.guild),
            view=GangMenuView(self.user_id)
        )


# ── Heist Role Select ─────────────────────────────────────────────
def _heist_role_embed(tier_key: str, g: dict, chosen_role: str = None) -> discord.Embed:
    t     = HEIST_TIERS[tier_key]
    embed = discord.Embed(
        title=f"{t['emoji']} {t['name']} — Pick Your Role",
        description="Choose how you contribute to the heist. Each role stacks with others.",
        color=discord.Color.orange()
    )
    # Show active bonuses so far (assume leader picks solo here; members join later)
    hacker_reduce = HEIST_ROLES["hacker"]["fail_reduce"]
    has_guard     = chosen_role == "guard"
    has_getaway   = chosen_role == "getaway"
    effective_fail = max(0.05, t["fail_chance"] - (hacker_reduce if chosen_role == "hacker" else 0.0))
    loot_min, loot_max = t["loot"]
    bag_mult = 1.20 if chosen_role == "bag_man" else 1.0
    cd_mult  = 0.75 if has_getaway else 1.0
    for key, r in HEIST_ROLES.items():
        active = "✅ **Selected**" if chosen_role == key else ""
        embed.add_field(
            name=f"{r['emoji']} {r['name']}  {active}",
            value=r["desc"],
            inline=True
        )
    embed.add_field(name="​", value="​", inline=True)  # spacer
    embed.add_field(
        name="📊 Active Preview",
        value=(
            f"❌ Fail chance: **{int(effective_fail*100)}%**\n"
            f"💰 Your loot share: **×{bag_mult:.2f}**\n"
            f"⏱️ Cooldown: **{fmt_cd(int(t['cd'] * cd_mult))}**\n"
            f"🛡️ Guard active: **{'Yes' if has_guard else 'No'}**"
        ),
        inline=False
    )
    warn = []
    if chosen_role != "hacker":  warn.append("⚠️ No Hacker — full fail chance")
    if chosen_role != "guard":   warn.append("⚠️ No Guard — full loss on fail")
    if warn:
        embed.add_field(name="⚠️ Warnings", value="\n".join(warn), inline=False)
    embed.set_footer(text="Pick a role then hit 🚀 Launch Heist")
    return embed


class HeistRoleView(View):
    def __init__(self, user_id: str, tier_key: str, chosen_role: str = None):
        super().__init__(timeout=90)
        self.user_id     = user_id
        self.tier_key    = tier_key
        self.chosen_role = chosen_role
        for btn in [self.hacker_btn, self.guard_btn, self.bagman_btn, self.getaway_btn]:
            if btn.custom_id == chosen_role:
                btn.style    = discord.ButtonStyle.success
            else:
                btn.style    = discord.ButtonStyle.blurple
        self.launch_btn.disabled = chosen_role is None

    def _ok(self, i): return str(i.user.id) == self.user_id

    async def _pick_role(self, interaction: discord.Interaction, role_key: str):
        if not self._ok(interaction):
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        await interaction.response.edit_message(
            embed=_heist_role_embed(self.tier_key, g, role_key),
            view=HeistRoleView(self.user_id, self.tier_key, role_key)
        )

    @discord.ui.button(label="💻 Hacker",          style=discord.ButtonStyle.blurple, row=0, custom_id="hacker")
    async def hacker_btn(self, interaction, button):
        await self._pick_role(interaction, "hacker")

    @discord.ui.button(label="🔫 Guard",           style=discord.ButtonStyle.blurple, row=0, custom_id="guard")
    async def guard_btn(self, interaction, button):
        await self._pick_role(interaction, "guard")

    @discord.ui.button(label="💰 Bag Man",         style=discord.ButtonStyle.blurple, row=1, custom_id="bag_man")
    async def bagman_btn(self, interaction, button):
        await self._pick_role(interaction, "bag_man")

    @discord.ui.button(label="🚗 Getaway Driver",  style=discord.ButtonStyle.blurple, row=1, custom_id="getaway")
    async def getaway_btn(self, interaction, button):
        await self._pick_role(interaction, "getaway")

    @discord.ui.button(label="🚀 Launch Heist",    style=discord.ButtonStyle.success, row=2, custom_id="launch")
    async def launch_btn(self, interaction, button):
        if not self._ok(interaction):
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if not g:
            return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        embed = await _execute_gang_heist(self.user_id, self.tier_key, self.chosen_role, g, gid)
        await interaction.response.edit_message(embed=embed, view=GangPanelBackView(self.user_id))

    @discord.ui.button(label="🔙 Back",            style=discord.ButtonStyle.gray,    row=2)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction):
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        await interaction.response.edit_message(
            embed=_heist_tier_embed(g, gid),
            view=HeistTierSelectView(self.user_id)
        )


# ── Main Gang Menu ────────────────────────────────────────────────

class GangMenuView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, i): return str(i.user.id) == self.user_id

    # ── Row 0: G-Bank · Territory · War ──────────────────────────
    @discord.ui.button(label="🏦 G-Bank", style=discord.ButtonStyle.green, row=0)
    async def gbank_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        p = get_player(self.user_id)
        embed = discord.Embed(
            title=f"🏦 G-Bank — {g.get('emoji','')} {g['name']}",
            description=f"```\nTreasury    ${g.get('bank',0):>12,}\nYour Wallet ${p['money']:>12,}\n```",
            color=_gang_color_obj(g)
        )
        embed.add_field(name="📥 Deposit",  value="Free for all members",   inline=True)
        embed.add_field(name="📤 Withdraw", value="Leader / Officer only",  inline=True)
        embed.set_footer(text="Funds heists, territory chests & war operations")
        await interaction.response.edit_message(embed=embed, view=GangBankView(self.user_id))

    @discord.ui.button(label="🗺️ Territory", style=discord.ButtonStyle.blurple, row=0)
    async def territory_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        owned = [(tid, territories[tid]) for tid in TERRITORIES if territories.get(tid, {}).get("owner_gid") == gid]
        embed = discord.Embed(title=f"🗺️ Territory — {g.get('emoji','')} {g['name']}", color=_gang_color_obj(g))
        if owned:
            tid, td = owned[0]; tinfo = TERRITORIES[tid]
            r_emoji = TERRITORY_RARITY_EMOJI.get(tinfo.get("rarity", "common"), "⬜")
            expires = td.get("expires_at", 0)
            embed.description = (
                f"{tinfo['emoji']} **{tinfo['name']}**  {r_emoji} {tinfo.get('rarity','').replace('_',' ').title()}\n"
                f"⏳ Expires <t:{int(expires)}:R>\n"
                "⚡ Perks: " + " | ".join(tinfo.get("perks", []))
            )
        else:
            embed.description = (
                "Your gang holds **no territory**.\n\n"
                f"🎁 Open a chest for **${CHEST_COST:,}** from the gang treasury!\n"
                "⬜ Common · 🟦 Rare · 🟣 Epic · 🟡 Legendary · 🔴 Extra Legendary"
            )
        await interaction.response.edit_message(embed=embed, view=TerritoryMenuView(self.user_id))

    @discord.ui.button(label="⚔️ War", style=discord.ButtonStyle.danger, row=0)
    async def war_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("at_war_with") and time.time() > g.get("war_ends_at", 0):
            winner_gid, stake = await _resolve_war(gid)
            wname = gangs.get(winner_gid, {}).get("name", "?") if winner_gid else "?"
            return await interaction.response.send_message(f"⚔️ War resolved! **{wname}** won and claimed **${stake:,}**.", ephemeral=True)
        await interaction.response.edit_message(embed=_gang_war_embed(g, gid), view=GangWarView(self.user_id, gid))

    # ── Row 1: Heist · Requests · Resources ──────────────────────
    @discord.ui.button(label="🎯 Heist", style=discord.ButtonStyle.green, row=1)
    async def heist_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can launch heists.", ephemeral=True)
        await interaction.response.edit_message(
            embed=_heist_tier_embed(g, gid),
            view=HeistTierSelectView(uid)
        )

    @discord.ui.button(label="📩 Requests", style=discord.ButtonStyle.blurple, row=1)
    async def requests_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can manage requests.", ephemeral=True)
        reqs = g.get("join_requests", [])
        if not reqs:
            return await interaction.response.send_message("📩 No pending join requests.", ephemeral=True)
        view = GangRequestsView(uid, gid, reqs)
        await interaction.response.edit_message(embed=view._embed(), view=view)

    @discord.ui.button(label="💰 Resources", style=discord.ButtonStyle.gray, row=1)
    async def resources_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        await interaction.response.edit_message(embed=_gang_resources_embed(g, gid), view=GangPanelBackView(self.user_id))

    # ── Row 2: Privacy · Invite · Back ───────────────────────────
    @discord.ui.button(label="🔒 Privacy", style=discord.ButtonStyle.gray, row=2)
    async def privacy_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid: return await interaction.response.send_message("❌ Only the leader can change privacy.", ephemeral=True)
        g["privacy"] = "private" if g.get("privacy", "public") == "public" else "public"
        save_gangs()
        icon = "🔓 Public" if g["privacy"] == "public" else "🔒 Private"
        await interaction.response.send_message(f"✅ Gang is now **{icon}**.", ephemeral=True)
        await interaction.message.edit(embed=_make_gang_embed(g, gid, interaction.guild), view=self)

    @discord.ui.button(label="👥 Invite", style=discord.ButtonStyle.blurple, row=2)
    async def invite_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can send invites.", ephemeral=True)
        embed = discord.Embed(
            title=f"👥 Invite Player — {g.get('emoji','')} {g['name']}",
            description="Select a Discord member to send a gang invite.",
            color=_gang_color_obj(g)
        )
        embed.set_footer(text="Player must not already be in a gang  •  Invite expires in 1 hour")
        await interaction.response.edit_message(embed=embed, view=GangInviteView(self.user_id))

    @discord.ui.button(label="🎯 Bounties", style=discord.ButtonStyle.red, row=2)
    async def bounty_board_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        embed = _bounty_embed_list()
        embed.set_footer(text="Use /bounty set @player amount to place a bounty  •  Steal from them to claim it!")
        await interaction.response.edit_message(embed=embed, view=GangBountyBoardView(self.user_id))

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=3)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        p = get_player(self.user_id, interaction.user.display_name)
        await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


class GangBountyBoardView(View):
    """Bounty board shown from inside the gang panel."""
    def __init__(self, user_id: str):
        super().__init__(timeout=60); self.user_id = user_id

    def _ok(self, i): return str(i.user.id) == self.user_id

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.blurple, row=0)
    async def refresh_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        embed = _bounty_embed_list()
        embed.set_footer(text="Use /bounty set @player amount to place a bounty  •  Steal from them to claim it!")
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="🔙 Back to Gang", style=discord.ButtonStyle.gray, row=0)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Not your panel.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if g:
            await interaction.response.edit_message(embed=_make_gang_embed(g, gid, interaction.guild), view=GangMenuView(self.user_id))
        else:
            p = get_player(self.user_id, interaction.user.display_name)
            await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


class TerritoryMenuView(View):
    def __init__(self, user_id: str):
        super().__init__(timeout=90); self.user_id = user_id

    def _ok(self, i): return str(i.user.id) == self.user_id

    # ── Row 0: Open Chest · Collect Income ───────────────────────
    @discord.ui.button(label="🎁 Open Chest", style=discord.ButtonStyle.green, row=0)
    async def chest_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can open chests.", ephemeral=True)
        owned = [tid for tid in TERRITORIES if territories.get(tid, {}).get("owner_gid") == gid]
        if owned:
            tinfo = TERRITORIES.get(owned[0], {}); td = territories.get(owned[0], {})
            expires = td.get("expires_at", 0)
            return await interaction.response.send_message(
                f"❌ Already own **{tinfo.get('emoji','')} {tinfo.get('name', owned[0])}**.\n"
                f"Expires <t:{int(expires)}:R>. Wait for it to expire or lose it in battle.", ephemeral=True)
        if g.get("bank", 0) < CHEST_COST:
            return await interaction.response.send_message(
                f"❌ Gang treasury needs **${CHEST_COST:,}**. Current: **${g.get('bank',0):,}**.", ephemeral=True)
        pool = [(tid, tinfo) for tid, tinfo in TERRITORIES.items()
                if territories.get(tid, {}).get("owner_gid") is None]
        if not pool:
            return await interaction.response.send_message("❌ All territories are currently owned by other gangs. Try again later.", ephemeral=True)
        weights_list = [TERRITORY_RARITY_WEIGHTS[t[1].get("rarity","common")] for t in pool]
        chosen_tid, chosen_tinfo = random.choices(pool, weights=weights_list, k=1)[0]
        duration = random.randint(*CHEST_TERRITORY_DURATION)
        g["bank"] -= CHEST_COST
        territories[chosen_tid]["owner_gid"] = gid
        territories[chosen_tid]["expires_at"] = time.time() + duration
        territories[chosen_tid]["last_income"] = 0
        rarity   = chosen_tinfo.get("rarity", "common")
        r_emoji  = TERRITORY_RARITY_EMOJI.get(rarity, "⬜")
        heat_add = TERRITORY_HEAT.get(rarity, 1)
        for mid in g.get("members", []):
            mp = players.get(mid)
            if mp: mp["heat"] = min(10, mp.get("heat", 0) + heat_add)
        save_territory(chosen_tid); save_gangs(); save_data()
        embed = discord.Embed(
            title="🎁 Chest Opened!",
            description=(
                f"🎊 You unlocked: **{chosen_tinfo['emoji']} {chosen_tinfo['name']}**  {r_emoji} **{rarity.capitalize()}**!\n\n"
                f"⚡ Perks: " + " | ".join(chosen_tinfo.get("perks", [])) + "\n"
                f"💰 Income: **${chosen_tinfo['income'][0]:,}–${chosen_tinfo['income'][1]:,} / 6h**\n"
                f"⏳ Expires <t:{int(territories[chosen_tid]['expires_at'])}:R>\n"
                f"🌡️ All members +{heat_add} heat from claiming this territory.\n\n"
                f"*Other gangs can attack and steal it — defend it well!*"
            ),
            color={"common": discord.Color.light_gray(), "rare": discord.Color.blue(), "epic": discord.Color.purple(), "legendary": discord.Color.gold(), "extra_legendary": discord.Color.red()}.get(rarity, discord.Color.light_gray())
        )
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="💰 Collect Income", style=discord.ButtonStyle.green, row=0)
    async def collect_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        uid = self.user_id; gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
        if g.get("leader") != uid and uid not in g.get("officers", []):
            return await interaction.response.send_message("❌ Only leader/officers can collect income.", ephemeral=True)
        owned = [(tid, territories[tid]) for tid in TERRITORIES if territories.get(tid, {}).get("owner_gid") == gid]
        if not owned:
            return await interaction.response.send_message("❌ Your gang doesn't control any territory.", ephemeral=True)
        tid, td = owned[0]; tinfo = TERRITORIES[tid]; now = time.time()
        last_inc = td.get("last_income", 0); cd = tinfo["income_cd"]
        if now - last_inc < cd:
            remaining = int(cd - (now - last_inc))
            return await interaction.response.send_message(
                f"⏳ Income not ready. Available in **{remaining // 3600}h {(remaining % 3600) // 60}m**.", ephemeral=True)
        loot = random.randint(*tinfo["income"])
        g["bank"] += loot; territories[tid]["last_income"] = now
        save_territory(tid); save_gangs()
        embed = discord.Embed(
            title=f"💰 Income Collected — {tinfo['emoji']} {tinfo['name']}",
            description=f"**+${loot:,}** deposited into gang treasury.\nTreasury: **${g['bank']:,}**",
            color=discord.Color.gold()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── Row 1: Territory Map · Back ───────────────────────────────
    @discord.ui.button(label="🗺️ Map", style=discord.ButtonStyle.blurple, row=1)
    async def map_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        await interaction.response.send_message(embed=_all_territories_embed(), ephemeral=True)

    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.gray, row=1)
    async def back_btn(self, interaction, button):
        if not self._ok(interaction): return await interaction.response.send_message("❌ Open your own `/play`.", ephemeral=True)
        gid, g = _find_player_gang(self.user_id)
        if g:
            await interaction.response.edit_message(embed=_make_gang_embed(g, gid, interaction.guild), view=GangMenuView(self.user_id))
        else:
            p = get_player(self.user_id, interaction.user.display_name)
            await interaction.response.edit_message(embed=_main_menu_embed(interaction.user, p), view=MainMenuView(self.user_id))


class _GangDepositModal(Modal, title="🏦 Deposit to Gang Treasury"):
    amount = TextInput(label="Amount", placeholder="Number or 'all'")
    def __init__(self, uid, gid): super().__init__(); self.uid = uid; self.gid = gid

    async def on_submit(self, interaction: discord.Interaction):
        p = get_player(self.uid); g = gangs.get(self.gid)
        if not g: return await interaction.response.send_message("❌ Gang not found.", ephemeral=True)
        raw = self.amount.value.strip().lower()
        amt = p["money"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if amt <= 0 or amt > p["money"]: return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        p["money"] -= amt; g["bank"] = g.get("bank", 0) + amt; add_gang_xp(g, 10)
        save_data(); save_gangs()
        await interaction.response.send_message(f"✅ Deposited **${amt:,}**. Treasury: **${g['bank']:,}**", ephemeral=True)


class _GangWithdrawModal(Modal, title="💸 Withdraw from Gang Treasury"):
    amount = TextInput(label="Amount", placeholder="Number or 'all'")
    def __init__(self, uid, gid): super().__init__(); self.uid = uid; self.gid = gid

    async def on_submit(self, interaction: discord.Interaction):
        p = get_player(self.uid); g = gangs.get(self.gid)
        if not g: return await interaction.response.send_message("❌ Gang not found.", ephemeral=True)
        bank = g.get("bank", 0); raw = self.amount.value.strip().lower()
        amt  = bank if raw == "all" else (int(raw) if raw.isdigit() else -1)
        if amt <= 0 or amt > bank: return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
        g["bank"] -= amt; p["money"] += amt; save_data(); save_gangs()
        await interaction.response.send_message(f"✅ Withdrew **${amt:,}**. Treasury: **${g['bank']:,}**", ephemeral=True)


# ── Gang slash command group ──────────────────────────────────────
gang_group = app_commands.Group(name="gang", description="Gang system — create, manage and battle gangs")

@gang_group.command(name="create", description=f"Create a new gang (costs ${GANG_CREATE_COST:,}, requires Level {GANG_CREATE_LEVEL_REQ})")
async def gang_create(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    if p.get("gang_id"):             return await interaction.response.send_message("❌ Already in a gang. `/gang leave` first.", ephemeral=True)
    if p["money"] < GANG_CREATE_COST: return await interaction.response.send_message(f"❌ Need **${GANG_CREATE_COST:,}** to create a gang.", ephemeral=True)
    if p["level"] < GANG_CREATE_LEVEL_REQ: return await interaction.response.send_message(f"❌ Need **Level {GANG_CREATE_LEVEL_REQ}**.", ephemeral=True)
    await interaction.response.send_modal(GangCreateModal(uid))

@gang_group.command(name="invite", description="Invite a player to your gang")
async def gang_invite(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only leader/officers can invite.", ephemeral=True)
    uid2 = str(member.id)
    if uid2 == uid: return await interaction.response.send_message("❌ Can't invite yourself.", ephemeral=True)
    p2 = get_player(uid2, member.display_name)
    if p2.get("gang_id"): return await interaction.response.send_message(f"❌ **{member.display_name}** is already in a gang.", ephemeral=True)
    if len(g.get("members", [])) >= _gang_max_members(g):
        return await interaction.response.send_message(f"❌ Gang is full ({_gang_max_members(g)} max members).", ephemeral=True)
    p2["gang_invite"] = {"gang_id": gid, "invited_by": uid, "expires": time.time() + GANG_INVITE_TTL}
    save_data()
    embed = discord.Embed(title=f"📨 Gang Invite — {g.get('emoji','')} {g['name']}", color=_gang_color_obj(g),
        description=f"<@{uid}> invited you to join their gang!\nUse `/gang accept` to join or `/gang decline` to refuse.")
    embed.set_footer(text="Invite expires in 1 hour")
    try:
        await member.send(embed=embed)
        await interaction.response.send_message(f"✅ Invite sent to **{member.display_name}**!", ephemeral=True)
    except Exception:
        await interaction.response.send_message(f"✅ Invite sent *(DMs off — they can still `/gang accept`)*", ephemeral=True)

@gang_group.command(name="accept", description="Accept a pending gang invite")
async def gang_accept(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    if p.get("gang_id"): return await interaction.response.send_message("❌ Already in a gang.", ephemeral=True)
    invite = p.get("gang_invite")
    if not invite: return await interaction.response.send_message("❌ No pending invite.", ephemeral=True)
    if time.time() > invite.get("expires", 0):
        p.pop("gang_invite", None); save_data()
        return await interaction.response.send_message("❌ Invite expired.", ephemeral=True)
    gid = invite["gang_id"]; g = gangs.get(gid)
    if not g:
        p.pop("gang_invite", None); save_data()
        return await interaction.response.send_message("❌ That gang no longer exists.", ephemeral=True)
    if len(g.get("members", [])) >= _gang_max_members(g):
        return await interaction.response.send_message("❌ Gang is now full.", ephemeral=True)
    g.setdefault("members", []).append(uid); p["gang_id"] = gid; p.pop("gang_invite", None)
    add_gang_xp(g, 50); save_data(); save_gangs()
    await interaction.response.send_message(f"✅ Joined **{g.get('emoji','')} {g['name']}**!", ephemeral=True)

@gang_group.command(name="decline", description="Decline a pending gang invite")
async def gang_decline(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    if not p.get("gang_invite"): return await interaction.response.send_message("❌ No pending invite.", ephemeral=True)
    p.pop("gang_invite", None); save_data()
    await interaction.response.send_message("✅ Invite declined.", ephemeral=True)

@gang_group.command(name="leave", description="Leave your current gang")
async def gang_leave(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") == uid:
        return await interaction.response.send_message("❌ Leaders can't leave — `/gang transfer @user` first, or `/gang disband`.", ephemeral=True)
    g["members"].remove(uid)
    if uid in g.get("officers", []): g["officers"].remove(uid)
    p["gang_id"] = None; save_data(); save_gangs()
    await interaction.response.send_message(f"✅ Left **{g['name']}**.", ephemeral=True)

@gang_group.command(name="kick", description="Kick a member from your gang")
async def gang_kick(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only leader/officers can kick.", ephemeral=True)
    uid2 = str(member.id)
    if uid2 == g.get("leader"):                               return await interaction.response.send_message("❌ Can't kick the leader.", ephemeral=True)
    if uid2 not in g.get("members", []):                      return await interaction.response.send_message("❌ Not in your gang.", ephemeral=True)
    if uid in g.get("officers", []) and uid2 in g.get("officers", []): return await interaction.response.send_message("❌ Officers can't kick other officers.", ephemeral=True)
    g["members"].remove(uid2)
    if uid2 in g.get("officers", []): g["officers"].remove(uid2)
    p2 = players.get(uid2, {}); p2["gang_id"] = None
    save_data(); save_gangs()
    await interaction.response.send_message(f"✅ **{member.display_name}** was kicked from the gang.", ephemeral=True)

@gang_group.command(name="promote", description="Promote a member to Officer (leader only)")
async def gang_promote(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid: return await interaction.response.send_message("❌ Only the leader can promote.", ephemeral=True)
    uid2 = str(member.id)
    if uid2 not in g.get("members", []):    return await interaction.response.send_message("❌ Not in your gang.", ephemeral=True)
    if uid2 in g.get("officers", []):       return await interaction.response.send_message("❌ Already an officer.", ephemeral=True)
    if len(g.get("officers", [])) >= MAX_OFFICERS: return await interaction.response.send_message(f"❌ Max {MAX_OFFICERS} officers.", ephemeral=True)
    g.setdefault("officers", []).append(uid2); save_gangs()
    await interaction.response.send_message(f"✅ **{member.display_name}** promoted to ⭐ Officer!", ephemeral=True)

@gang_group.command(name="demote", description="Demote an Officer back to Member (leader only)")
async def gang_demote(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid: return await interaction.response.send_message("❌ Only the leader can demote.", ephemeral=True)
    uid2 = str(member.id)
    if uid2 not in g.get("officers", []): return await interaction.response.send_message("❌ Not an officer.", ephemeral=True)
    g["officers"].remove(uid2); save_gangs()
    await interaction.response.send_message(f"✅ **{member.display_name}** demoted to Member.", ephemeral=True)

@gang_group.command(name="transfer", description="Transfer gang leadership to another member")
async def gang_transfer(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid: return await interaction.response.send_message("❌ Only the leader can transfer leadership.", ephemeral=True)
    uid2 = str(member.id)
    if uid2 not in g.get("members", []): return await interaction.response.send_message("❌ Not in your gang.", ephemeral=True)
    g["leader"] = uid2
    if uid2 in g.get("officers", []): g["officers"].remove(uid2)
    if uid not in g.get("officers", []): g.setdefault("officers", []).append(uid)
    save_gangs()
    await interaction.response.send_message(f"✅ Leadership transferred to **{member.display_name}**. You are now an Officer.", ephemeral=True)

@gang_group.command(name="disband", description="Permanently disband your gang (leader only)")
async def gang_disband(interaction: discord.Interaction):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid: return await interaction.response.send_message("❌ Only the leader can disband.", ephemeral=True)
    for mid in g.get("members", []):
        mp = players.get(mid, {}); mp["gang_id"] = None
    refund = g.get("bank", 0) // 2
    if refund: players.get(uid, {})["money"] = players.get(uid, {}).get("money", 0) + refund
    gangs.pop(gid, None); delete_gang(gid); save_data()
    msg = f"💀 **{g['name']}** has been disbanded."
    if refund: msg += f" **${refund:,}** (50% treasury) returned to you."
    await interaction.response.send_message(msg, ephemeral=True)

@gang_group.command(name="info", description="View your gang or search by name")
async def gang_info(interaction: discord.Interaction, name: str = None):
    uid, _ = await _ensure_user(interaction)
    if name:
        found = [(gid, g) for gid, g in gangs.items() if g.get("name","").lower() == name.lower()]
        if not found: return await interaction.response.send_message(f"❌ No gang named **{name}**.", ephemeral=True)
        gid, g = found[0]
    else:
        gid, g = _find_player_gang(uid)
        if not g: return await interaction.response.send_message("❌ Not in a gang. Use `/gang create` or get invited.", ephemeral=True)
    await interaction.response.send_message(embed=_make_gang_embed(g, gid, interaction.guild), ephemeral=True)

@gang_group.command(name="deposit", description="Deposit money into the gang treasury")
async def gang_deposit_cmd(interaction: discord.Interaction, amount: str):
    uid, p = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    raw = amount.strip().lower(); amt = p["money"] if raw == "all" else (int(raw) if raw.isdigit() else -1)
    if amt <= 0:         return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
    if amt > p["money"]: return await interaction.response.send_message(f"❌ Only **${p['money']:,}** in wallet.", ephemeral=True)
    p["money"] -= amt; g["bank"] = g.get("bank", 0) + amt; add_gang_xp(g, 10)
    save_data(); save_gangs()
    await interaction.response.send_message(f"✅ Deposited **${amt:,}**. 💰 Treasury: **${g['bank']:,}**", ephemeral=True)

@gang_group.command(name="withdraw", description="Withdraw from gang treasury (leader/officer only)")
async def gang_withdraw_cmd(interaction: discord.Interaction, amount: str):
    uid, p = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only leader/officers can withdraw.", ephemeral=True)
    bank = g.get("bank", 0); raw = amount.strip().lower()
    amt  = bank if raw == "all" else (int(raw) if raw.isdigit() else -1)
    if amt <= 0 or amt > bank: return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
    g["bank"] -= amt; p["money"] += amt; save_data(); save_gangs()
    await interaction.response.send_message(f"✅ Withdrew **${amt:,}**. 💰 Treasury: **${g['bank']:,}**", ephemeral=True)

@gang_group.command(name="heist", description="Launch a gang heist — choose tier & role (leader/officer only)")
async def gang_heist_cmd(interaction: discord.Interaction):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only leader/officers can launch heists.", ephemeral=True)
    await interaction.response.send_message(
        embed=_heist_tier_embed(g, gid),
        view=HeistTierSelectView(uid),
        ephemeral=True
    )

@gang_group.command(name="war", description="Declare war on another player's gang (leader only, Gang Lv5)")
async def gang_war_cmd(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid: return await interaction.response.send_message("❌ Only the leader can declare war.", ephemeral=True)
    if not any("War" in p_ for p_ in _gang_perks(g)):
        return await interaction.response.send_message("❌ Gang War unlocks at **Gang Level 5**.", ephemeral=True)
    if g.get("at_war_with"): return await interaction.response.send_message("❌ Already at war!", ephemeral=True)
    uid2 = str(member.id); egid, eg = _find_player_gang(uid2)
    if not eg: return await interaction.response.send_message(f"❌ **{member.display_name}** is not in a gang.", ephemeral=True)
    if egid == gid: return await interaction.response.send_message("❌ Can't war your own gang.", ephemeral=True)
    if eg.get("at_war_with"): return await interaction.response.send_message("❌ That gang is already at war.", ephemeral=True)
    wend = time.time() + GANG_WAR_DURATION
    for gg, eid in [(g, egid), (eg, gid)]:
        gg["at_war_with"] = eid; gg["war_ends_at"] = wend; gg["war_wins"] = 0; gg["war_losses"] = 0
    save_gangs()
    embed = discord.Embed(
        title="⚔️ WAR DECLARED!",
        description=(f"**{g.get('emoji','')} {g['name']}** vs **{eg.get('emoji','')} {eg['name']}**\n"
                     f"War lasts **24 hours** — most wins claims **{int(GANG_WAR_STAKE*100)}%** of enemy treasury!"),
        color=discord.Color.dark_red()
    )
    embed.set_footer(text="Members: use /gang attack to fight! 1 attack per hour.")
    await interaction.response.send_message(embed=embed)

@gang_group.command(name="attack", description="Attack the enemy gang during war (1 per hour per member)")
async def gang_attack_cmd(interaction: discord.Interaction):
    uid, p = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if not g.get("at_war_with"): return await interaction.response.send_message("❌ Not at war.", ephemeral=True)
    rem = cd_remaining(p, f"gang_atk_{gid}", GANG_ATTACK_COOLDOWN)
    if rem > 0: return await interaction.response.send_message(f"⏳ Attack cooldown: `{fmt_cd(rem)}`", ephemeral=True)
    if time.time() > g.get("war_ends_at", 0):
        winner_gid, stake = await _resolve_war(gid)
        wname = gangs.get(winner_gid, {}).get("name", "?") if winner_gid else "?"
        return await interaction.response.send_message(f"⚔️ War ended! **{wname}** won and claimed **${stake:,}**.", ephemeral=True)
    egid = g["at_war_with"]; eg = gangs.get(egid, {})
    win  = random.random() < 0.52; loot = random.randint(50, 200)
    if win:
        g["war_wins"]    = g.get("war_wins", 0) + 1
        eg["war_losses"] = eg.get("war_losses", 0) + 1
        p["money"] += loot; add_gang_xp(g, 25)
        embed = discord.Embed(title="⚔️ Attack — VICTORY!", color=discord.Color.green())
        embed.add_field(name="💰 Loot",        value=f"+**${loot:,}**",                        inline=True)
        embed.add_field(name="📊 Gang Score",  value=f"**{g['war_wins']}W — {g['war_losses']}L**", inline=True)
    else:
        g["war_losses"]  = g.get("war_losses", 0) + 1
        eg["war_wins"]   = eg.get("war_wins", 0) + 1
        add_gang_xp(eg, 15)
        embed = discord.Embed(title="⚔️ Attack — DEFEATED!", color=discord.Color.red())
        embed.add_field(name="📊 Gang Score",  value=f"**{g['war_wins']}W — {g['war_losses']}L**", inline=True)
    embed.set_footer(text="Next attack in 1 hour")
    p["cooldowns"][f"gang_atk_{gid}"] = time.time(); save_data(); save_gangs()
    await interaction.response.send_message(embed=embed, ephemeral=True)

@gang_group.command(name="leaderboard", description="Top gangs ranked by treasury + XP")
async def gang_leaderboard_cmd(interaction: discord.Interaction):
    if not gangs: return await interaction.response.send_message("❌ No gangs exist yet.", ephemeral=True)
    top     = sorted(gangs.items(), key=lambda x: x[1].get("bank",0) + x[1].get("xp",0)*5, reverse=True)[:10]
    medals  = ["🥇","🥈","🥉"] + ["🔹"] * 7
    lines   = [
        f"{medals[i]} {g.get('emoji','🏴')} **{g['name']}** `{g.get('tag','')}` — "
        f"Lv.**{_gang_level(g)}** | 👥 {len(g.get('members',[]))} | 💰 ${g.get('bank',0):,}"
        for i, (_, g) in enumerate(top)
    ]
    embed = discord.Embed(title="🏆 Gang Leaderboard", description="\n".join(lines), color=discord.Color.gold())
    await interaction.response.send_message(embed=embed, ephemeral=True)

@gang_group.command(name="join", description="Join a public gang, or request to join a private one")
async def gang_join_cmd(interaction: discord.Interaction, name: str):
    uid, p = await _ensure_user(interaction)
    if p.get("gang_id"): return await interaction.response.send_message("❌ Already in a gang. `/gang leave` first.", ephemeral=True)
    found = [(gid, g) for gid, g in gangs.items() if g.get("name","").lower() == name.lower()]
    if not found: return await interaction.response.send_message(f"❌ No gang named **{name}**.", ephemeral=True)
    gid, g = found[0]
    if len(g.get("members", [])) >= _gang_max_members(g):
        return await interaction.response.send_message(f"❌ **{g['name']}** is full ({_gang_max_members(g)} members max).", ephemeral=True)
    if g.get("privacy", "public") == "public":
        g.setdefault("members", []).append(uid); p["gang_id"] = gid
        add_gang_xp(g, 50); save_data(); save_gangs()
        await interaction.response.send_message(f"✅ Joined **{g.get('emoji','')} {g['name']}**!", ephemeral=True)
    else:
        reqs = g.setdefault("join_requests", [])
        if uid in reqs: return await interaction.response.send_message("⏳ You already have a pending request for this gang.", ephemeral=True)
        reqs.append(uid); save_gangs()
        await interaction.response.send_message(
            f"📋 Join request sent to **{g.get('emoji','')} {g['name']}**!\n"
            f"*This is a private gang — a leader/officer will review your request.*", ephemeral=True)

@gang_group.command(name="approve", description="Approve a player's join request (leader/officer only)")
async def gang_approve_cmd(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only leader/officers can approve requests.", ephemeral=True)
    uid2 = str(member.id)
    reqs = g.get("join_requests", [])
    if uid2 not in reqs: return await interaction.response.send_message(f"❌ **{member.display_name}** has no pending request.", ephemeral=True)
    if len(g.get("members", [])) >= _gang_max_members(g):
        return await interaction.response.send_message(f"❌ Gang is full ({_gang_max_members(g)} members max).", ephemeral=True)
    p2 = get_player(uid2, member.display_name)
    if p2.get("gang_id"):
        reqs.remove(uid2); save_gangs()
        return await interaction.response.send_message(f"❌ **{member.display_name}** already joined another gang.", ephemeral=True)
    reqs.remove(uid2); g.setdefault("members", []).append(uid2); p2["gang_id"] = gid
    add_gang_xp(g, 50); save_data(); save_gangs()
    try: await member.send(f"✅ Your request to join **{g.get('emoji','')} {g['name']}** was approved!")
    except Exception: pass
    await interaction.response.send_message(f"✅ **{member.display_name}** approved and added to the gang!", ephemeral=True)

@gang_group.command(name="deny", description="Deny a player's join request (leader/officer only)")
async def gang_deny_cmd(interaction: discord.Interaction, member: discord.Member):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only leader/officers can deny requests.", ephemeral=True)
    uid2 = str(member.id); reqs = g.get("join_requests", [])
    if uid2 not in reqs: return await interaction.response.send_message(f"❌ **{member.display_name}** has no pending request.", ephemeral=True)
    reqs.remove(uid2); save_gangs()
    try: await member.send(f"❌ Your request to join **{g.get('emoji','')} {g['name']}** was denied.")
    except Exception: pass
    await interaction.response.send_message(f"✅ **{member.display_name}**'s request denied.", ephemeral=True)

@gang_group.command(name="privacy", description="Toggle gang between public and private (leader only)")
async def gang_privacy_cmd(interaction: discord.Interaction):
    uid, _ = await _ensure_user(interaction)
    gid, g = _find_player_gang(uid)
    if not g: return await interaction.response.send_message("❌ Not in a gang.", ephemeral=True)
    if g.get("leader") != uid: return await interaction.response.send_message("❌ Only the leader can change privacy.", ephemeral=True)
    g["privacy"] = "private" if g.get("privacy", "public") == "public" else "public"
    save_gangs()
    icon = "🔓 Public" if g["privacy"] == "public" else "🔒 Private"
    desc = ("Anyone can `/gang join` freely." if g["privacy"] == "public"
            else "Players must request to join — you approve/deny with `/gang approve` or `/gang deny`.")
    embed = discord.Embed(title=f"Gang Privacy Updated — {icon}", description=desc, color=_gang_color_obj(g))
    await interaction.response.send_message(embed=embed, ephemeral=True)

bot.tree.add_command(gang_group)

# ================================================================
# TERRITORY COMMANDS
# ================================================================
territory_group = app_commands.Group(name="territory", description="Territory control system")

@territory_group.command(name="list", description="View the territory map")
async def territory_list(interaction: discord.Interaction):
    await interaction.response.send_message(embed=_all_territories_embed(), ephemeral=True)

@territory_group.command(name="info", description="Detailed info on a territory")
@app_commands.describe(territory="Territory ID: downtown / slums / port / tech_zone")
async def territory_info(interaction: discord.Interaction, territory: str):
    tid = territory.lower()
    if tid not in TERRITORIES:
        return await interaction.response.send_message(
            f"❌ Unknown territory. Choose from: {', '.join(TERRITORIES.keys())}", ephemeral=True)
    await interaction.response.send_message(
        embed=_territory_embed(tid, territories.get(tid, {}), interaction.guild), ephemeral=True)

@territory_group.command(name="attack", description="Attack a territory to seize control")
@app_commands.describe(territory="Territory to attack: downtown / slums / port / tech_zone")
async def territory_attack(interaction: discord.Interaction, territory: str):
    uid  = str(interaction.user.id)
    tid  = territory.lower()
    now  = time.time()
    p    = get_player(uid, interaction.user.display_name)
    gid, g = _find_player_gang(uid)
    if not g:
        return await interaction.response.send_message("❌ You must be in a gang to attack territories.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only gang leaders or officers can launch attacks.", ephemeral=True)
    if tid not in TERRITORIES:
        return await interaction.response.send_message(
            f"❌ Unknown territory. Choose from: {', '.join(TERRITORIES.keys())}", ephemeral=True)
    td = territories.get(tid, {})
    if td.get("owner_gid") == gid:
        return await interaction.response.send_message("❌ Your gang already owns this territory.", ephemeral=True)
    if td.get("battle"):
        return await interaction.response.send_message("❌ A battle is already in progress here.", ephemeral=True)
    if _gang_territory_count(gid) >= MAX_TERRITORIES:
        return await interaction.response.send_message(
            f"❌ Your gang already controls the maximum of **{MAX_TERRITORIES}** territories.", ephemeral=True)
    last_atk = td.get("last_attacked", 0)
    if now - last_atk < TERRITORY_ATTACK_CD:
        remaining = int(TERRITORY_ATTACK_CD - (now - last_atk))
        return await interaction.response.send_message(
            f"❌ This territory was recently attacked. Wait **{remaining // 60}m {remaining % 60}s**.", ephemeral=True)
    tinfo = TERRITORIES[tid]
    base_def = tinfo["base_def"]
    def_bonus = 50 if td.get("owner_gid") else 0
    td["battle"] = {
        "attacker_gid": gid,
        "atk_pts":      0,
        "def_pts":      base_def + def_bonus,
        "ends_at":      now + BATTLE_DURATION,
        "contributors": [],
    }
    territories[tid] = td
    save_territory(tid)
    owner_gid = td.get("owner_gid")
    og = gangs.get(owner_gid, {}) if owner_gid else None
    def_name = f"{og.get('emoji','')} {og['name']}" if og else "the unclaimed territory"
    embed = discord.Embed(
        title=f"⚔️ Battle Started — {tinfo['emoji']} {tinfo['name']}",
        description=(
            f"**{g.get('emoji','')} {g['name']}** is attacking {def_name}!\n\n"
            f"🛡️ Defenders start with **{base_def + def_bonus} pts**.\n"
            f"Your gang members can `/territory contribute` to add attack points.\n"
            f"Battle ends <t:{int(now + BATTLE_DURATION)}:R>."
        ),
        color=_gang_color_obj(g)
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@territory_group.command(name="contribute", description="Contribute points to your gang's active territory battle")
@app_commands.describe(territory="Territory being battled: downtown / slums / port / tech_zone")
async def territory_contribute(interaction: discord.Interaction, territory: str):
    uid = str(interaction.user.id)
    tid = territory.lower()
    now = time.time()
    gid, g = _find_player_gang(uid)
    if not g:
        return await interaction.response.send_message("❌ You must be in a gang.", ephemeral=True)
    if tid not in TERRITORIES:
        return await interaction.response.send_message(
            f"❌ Unknown territory. Choose from: {', '.join(TERRITORIES.keys())}", ephemeral=True)
    td = territories.get(tid, {})
    b  = td.get("battle")
    if not b:
        return await interaction.response.send_message("❌ No active battle on this territory.", ephemeral=True)
    is_attacker  = b["attacker_gid"] == gid
    is_defender  = td.get("owner_gid") == gid
    if not is_attacker and not is_defender:
        return await interaction.response.send_message("❌ Your gang is not involved in this battle.", ephemeral=True)
    if now > b["ends_at"]:
        winner_gid, attacker_won = await _resolve_battle(tid)
        wg = gangs.get(winner_gid, {}) if winner_gid else {}
        result_str = f"⏰ Battle has ended! **{wg.get('emoji','')}{wg.get('name','Defenders')}** won {tinfo_['name'] if (tinfo_ := TERRITORIES.get(tid)) else tid}."
        return await interaction.response.send_message(result_str, ephemeral=True)
    contrib_key = f"terr_contrib_{tid}"
    cd = g.get("cooldowns", {}).get(uid, {}).get(contrib_key, 0)
    if now - cd < CONTRIBUTE_CD:
        remaining = int(CONTRIBUTE_CD - (now - cd))
        return await interaction.response.send_message(
            f"❌ You already contributed recently. Wait **{remaining // 60}m {remaining % 60}s**.", ephemeral=True)
    p = get_player(uid)
    pts = random.randint(20, 60)
    if is_attacker:
        b["atk_pts"] += pts
    else:
        b["def_pts"] += pts
    if uid not in b.get("contributors", []):
        b.setdefault("contributors", []).append(uid)
    g.setdefault("cooldowns", {}).setdefault(uid, {})[contrib_key] = now
    territories[tid]["battle"] = b
    save_territory(tid); save_gangs()
    tinfo = TERRITORIES.get(tid, {})
    side  = "⚔️ Attack" if is_attacker else "🛡️ Defense"
    embed = discord.Embed(
        title=f"💪 Contribution — {tinfo.get('emoji','')} {tinfo.get('name', tid)}",
        description=f"You added **+{pts} pts** to {side}!\n\n"
                    f"⚔️ Attackers: **{b['atk_pts']}** pts\n"
                    f"🛡️ Defenders: **{b['def_pts']}** pts\n"
                    f"Battle ends <t:{int(b['ends_at'])}:R>",
        color=discord.Color.green() if is_attacker else discord.Color.blue()
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@territory_group.command(name="collect", description="Collect passive income from a territory your gang owns")
@app_commands.describe(territory="Territory to collect from: downtown / slums / port / tech_zone")
async def territory_collect(interaction: discord.Interaction, territory: str):
    uid = str(interaction.user.id)
    tid = territory.lower()
    now = time.time()
    gid, g = _find_player_gang(uid)
    if not g:
        return await interaction.response.send_message("❌ You must be in a gang.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only the leader or officers can collect income.", ephemeral=True)
    if tid not in TERRITORIES:
        return await interaction.response.send_message(
            f"❌ Unknown territory. Choose from: {', '.join(TERRITORIES.keys())}", ephemeral=True)
    td = territories.get(tid, {})
    if td.get("owner_gid") != gid:
        return await interaction.response.send_message("❌ Your gang does not control this territory.", ephemeral=True)
    tinfo    = TERRITORIES[tid]
    last_inc = td.get("last_income", 0)
    cd       = tinfo["income_cd"]
    if now - last_inc < cd:
        remaining = int(cd - (now - last_inc))
        return await interaction.response.send_message(
            f"❌ Income not ready yet. Available in **{remaining // 3600}h {(remaining % 3600) // 60}m**.", ephemeral=True)
    loot = random.randint(*tinfo["income"])
    g["bank"] = g.get("bank", 0) + loot
    territories[tid]["last_income"] = now
    save_territory(tid); save_gangs()
    embed = discord.Embed(
        title=f"💰 Income Collected — {tinfo['emoji']} {tinfo['name']}",
        description=f"**${loot:,}** deposited into the gang treasury.\n"
                    f"Gang Bank: **${g['bank']:,}**",
        color=discord.Color.gold()
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@territory_group.command(name="leaderboard", description="Top gangs ranked by territories controlled")
async def territory_leaderboard(interaction: discord.Interaction):
    counts: dict[str, list] = {}
    for tid, td in territories.items():
        owner = td.get("owner_gid")
        if owner:
            counts.setdefault(owner, []).append(tid)
    ranked = sorted(counts.items(), key=lambda x: len(x[1]), reverse=True)
    embed  = discord.Embed(
        title="🗺️ Territory Leaderboard",
        description="Gangs ranked by territories controlled.",
        color=discord.Color.dark_gold()
    )
    medals = ["🥇", "🥈", "🥉"]
    if not ranked:
        embed.description = "No territories have been claimed yet."
    for i, (gid, tids) in enumerate(ranked[:10]):
        g     = gangs.get(gid, {})
        medal = medals[i] if i < 3 else f"#{i+1}"
        names = " ".join(TERRITORIES[t]["emoji"] + " " + TERRITORIES[t]["name"] for t in tids)
        embed.add_field(
            name=f"{medal} {g.get('emoji','')} {g.get('name','Unknown')}",
            value=f"**{len(tids)}** territories: {names}",
            inline=False
        )
    await interaction.response.send_message(embed=embed, ephemeral=True)

bot.tree.add_command(territory_group)

# ── Roulette command group ────────────────────────────────────────
roulette_group = app_commands.Group(name="roulette", description="Live multiplayer roulette")

@roulette_group.command(name="bet", description="Place a bet on the current roulette round")
@app_commands.describe(
    type="Bet type: red / black / green / even / odd / low / high",
    amount="Amount to bet (min $100)"
)
async def roulette_bet(interaction: discord.Interaction, type: str, amount: str):
    rs  = roulette_state
    uid = str(interaction.user.id)

    type = type.lower().strip()
    is_num_bet = _rou_is_number_bet(type)
    if type not in ROULETTE_KEYWORD_BETS and not is_num_bet:
        return await interaction.response.send_message(
            "❌ Invalid bet type.\n"
            "**Keywords:** `red · black · green · even · odd · low · high`\n"
            "**Number:** `0`–`36` (straight-up, pays **32×**)", ephemeral=True)

    try:
        amt = int(amount)
    except ValueError:
        return await interaction.response.send_message("❌ Enter a valid number for the amount.", ephemeral=True)

    err = await _rou_place_bet(interaction, type, amt)
    if err:
        return await interaction.response.send_message(err, ephemeral=True)

    is_live     = rs["active"] and rs["phase"] == "betting"
    bucket      = rs["bets"] if is_live else rs["pending_bets"]
    player_bets = bucket.get(uid, [])
    nxt         = rs["next_round_at"]
    nxt_str     = f"<t:{nxt}:R>"
    status      = "this round ✅" if is_live else f"next round ({nxt_str}) ⏳"

    if is_num_bet:
        payout_str = f"{ROULETTE_NUMBER_PAYOUT}×"
        icon  = _rou_emoji(_rou_color(int(type)))
        label = f"#{type}"
    else:
        payout_str = f"{ROULETTE_PAYOUTS[type]}×"
        icon  = _rou_emoji(type) if type in ("red", "black", "green") else "🎯"
        label = type.upper()

    p        = get_player(uid)
    total_in = sum(b["amount"] for b in player_bets)
    await interaction.response.send_message(
        f"{icon} **${amt:,}** on **{label}** (payout: {payout_str}) — queued for **{status}**\n"
        f"Bets queued: **{len(player_bets)}/{ROULETTE_MAX_BETS}** — staked: **${total_in:,}**\n"
        f"💵 Wallet: **${p['money']:,}**",
        ephemeral=True
    )

@roulette_group.command(name="start", description="Manually trigger a roulette round (admin only)")
async def roulette_start(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Requires Manage Server permission.", ephemeral=True)
    if roulette_state["active"]:
        return await interaction.response.send_message("❌ A round is already in progress.", ephemeral=True)
    await interaction.response.send_message("🎡 Starting a roulette round now!", ephemeral=True)
    asyncio.create_task(run_roulette_round())

@roulette_group.command(name="setchannel", description="Set this channel as the auto-roulette channel (admin only)")
async def roulette_setchannel(interaction: discord.Interaction):
    global _roulette_channel_id
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Requires Manage Server permission.", ephemeral=True)
    _roulette_channel_id = interaction.channel_id
    await interaction.response.send_message(
        f"✅ Roulette rounds will now auto-start in <#{interaction.channel_id}> every **{ROULETTE_ROUND_GAP}s**.",
        ephemeral=True
    )

bot.tree.add_command(roulette_group)

@bot.tree.command(name="chest", description="Open a territory chest for your gang (costs gang treasury)")
async def chest_slash(interaction: discord.Interaction):
    uid = str(interaction.user.id)
    gid, g = _find_player_gang(uid)
    if not g:
        return await interaction.response.send_message("❌ You must be in a gang to open a chest.", ephemeral=True)
    if g.get("leader") != uid and uid not in g.get("officers", []):
        return await interaction.response.send_message("❌ Only the gang leader or officers can open chests.", ephemeral=True)
    owned = [tid for tid in TERRITORIES if territories.get(tid, {}).get("owner_gid") == gid]
    if owned:
        tinfo = TERRITORIES.get(owned[0], {}); td = territories.get(owned[0], {}); expires = td.get("expires_at", 0)
        return await interaction.response.send_message(
            f"❌ Already own **{tinfo.get('emoji','')} {tinfo.get('name', owned[0])}**.\n"
            f"Expires <t:{int(expires)}:R>. Wait for it to expire or lose it in battle.", ephemeral=True)
    if g.get("bank", 0) < CHEST_COST:
        return await interaction.response.send_message(
            f"❌ Gang treasury needs **${CHEST_COST:,}**. Current: **${g.get('bank',0):,}**.", ephemeral=True)
    pool = [(tid, tinfo) for tid, tinfo in TERRITORIES.items()
            if territories.get(tid, {}).get("owner_gid") is None]
    if not pool:
        return await interaction.response.send_message("❌ All territories are currently owned. Try again later.", ephemeral=True)
    weights_list = [TERRITORY_RARITY_WEIGHTS[t[1].get("rarity","common")] for t in pool]
    chosen_tid, chosen_tinfo = random.choices(pool, weights=weights_list, k=1)[0]
    duration = random.randint(*CHEST_TERRITORY_DURATION)
    g["bank"] -= CHEST_COST
    territories[chosen_tid]["owner_gid"] = gid
    territories[chosen_tid]["expires_at"] = time.time() + duration
    territories[chosen_tid]["last_income"] = 0
    rarity   = chosen_tinfo.get("rarity", "common")
    r_emoji  = TERRITORY_RARITY_EMOJI.get(rarity, "⬜")
    heat_add = TERRITORY_HEAT.get(rarity, 1)
    for mid in g.get("members", []):
        mp = players.get(mid)
        if mp: mp["heat"] = min(10, mp.get("heat", 0) + heat_add)
    save_territory(chosen_tid); save_gangs(); save_data()
    embed = discord.Embed(
        title="🎁 Chest Opened!",
        description=(
            f"🎊 You unlocked: **{chosen_tinfo['emoji']} {chosen_tinfo['name']}**  {r_emoji} **{rarity.capitalize()}**!\n\n"
            f"⚡ Perks: " + " | ".join(chosen_tinfo.get("perks", [])) + "\n"
            f"💰 Income: **${chosen_tinfo['income'][0]:,}–${chosen_tinfo['income'][1]:,} / 6h**\n"
            f"⏳ Expires <t:{int(territories[chosen_tid]['expires_at'])}:R>\n"
            f"🌡️ All members +{heat_add} heat from claiming this territory.\n\n"
            f"*Other gangs can attack and steal it — defend it well!*"
        ),
        color={"common": discord.Color.light_gray(), "rare": discord.Color.blue(), "epic": discord.Color.purple(), "legendary": discord.Color.gold(), "extra_legendary": discord.Color.red()}.get(rarity, discord.Color.light_gray())
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ================================================================
# SERVER EVENT COMMANDS
# ================================================================
@bot.tree.command(name="seteventchannel", description="[Admin] Set the channel where server events are announced")
@app_commands.checks.has_permissions(administrator=True)
async def seteventchannel_slash(interaction: discord.Interaction, channel: discord.TextChannel):
    _save_event_channel(channel.id)
    await interaction.response.send_message(
        f"✅ Server events will be posted in {channel.mention}!", ephemeral=True)

@bot.tree.command(name="event", description="[Admin] Manually fire a server event right now")
@app_commands.checks.has_permissions(administrator=True)
async def event_slash(interaction: discord.Interaction):
    global _next_event_at
    channel_id = _event_channel_id or _load_event_channel()
    if not channel_id:
        return await interaction.response.send_message(
            "❌ No event channel set. Use `/seteventchannel` first.", ephemeral=True)
    await interaction.response.send_message("🌍 Firing a server event...", ephemeral=True)
    _next_event_at = 0  # allow task to fire immediately
    await _fire_server_event()

@bot.tree.command(name="currentevent", description="Check if a server event is currently active")
async def currentevent_slash(interaction: discord.Interaction):
    ev = active_server_event
    if not ev:
        return await interaction.response.send_message("🌍 No server event is currently active.", ephemeral=True)
    etype  = ev.get("type", "")
    ev_cfg = SERVER_EVENTS.get(etype, {})
    dur    = ev_cfg.get("duration", 0)
    embed  = discord.Embed(title=f"🌍 Active Event: {ev_cfg.get('name','?')}", color=ev_cfg.get("color", discord.Color.blurple()))
    embed.description = ev_cfg.get("desc", "")
    if dur > 0:
        left = event_time_left()
        embed.add_field(name="⏱️ Time Remaining", value=f"`{fmt_cd(left)}`", inline=True)
    else:
        embed.add_field(name="Status", value="One-shot event (already applied)", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ================================================================
# SETUP COMMAND — creates #lifebot intro channel
# ================================================================
@bot.tree.command(name="setup", description="[Admin] Create a #lifebot channel with a full introduction embed")
@app_commands.checks.has_permissions(manage_channels=True)
async def setup_slash(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild

    # Reuse existing channel if already there
    existing = discord.utils.get(guild.text_channels, name="lifebot")
    if existing:
        ch = existing
    else:
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(send_messages=False, read_messages=True)
        }
        ch = await guild.create_text_channel("lifebot", overwrites=overwrites, topic="All LifeBot commands and info")

    embed = discord.Embed(
        title="🤖 Welcome to LifeBot!",
        description=(
            "LifeBot is a full-featured economy & gaming bot.\n"
            "Everything runs on **slash commands** — type `/` to get started!\n"
        ),
        color=discord.Color.dark_green()
    )
    embed.add_field(
        name="💼 Economy",
        value="`/daily` `/work` `/crime` `/bank` `/shop` `/leaderboard`",
        inline=False
    )
    embed.add_field(
        name="🎮 Games",
        value="`/blackjack` `/slots` `/rps` `/mines` `/chicken` `/scratch` `/play`",
        inline=False
    )
    embed.add_field(
        name="🥷 PvP",
        value="`/steal` `/duel` `/scan` `/revenge`",
        inline=False
    )
    embed.add_field(
        name="🏴 Gangs & Territory",
        value="`/gang` `/territory` `/chest`",
        inline=False
    )
    embed.add_field(
        name="📊 Profile & Progression",
        value="`/profile` `/prestige` `/achievements` `/challenges` `/train` `/career`",
        inline=False
    )
    embed.add_field(
        name="❓ Full Command List",
        value="Use `/help` for detailed info on every command.",
        inline=False
    )
    embed.set_footer(text=f"Starting balance: ${STARTING_MONEY:,}  •  Good luck!")

    msg = await ch.send(embed=embed)
    await msg.pin()
    await interaction.followup.send(f"✅ Intro posted and pinned in {ch.mention}!", ephemeral=True)


# ================================================================
# BOUNTY COMMANDS
# ================================================================
bounty_group = app_commands.Group(name="bounty", description="Place and track bounties on players")

@bounty_group.command(name="set", description="Place a bounty on a player (min $500) — auto-claimed when they get stolen from")
@app_commands.describe(member="The player to put a bounty on", amount="Amount to put up (min $500)")
async def bounty_set(interaction: discord.Interaction, member: discord.Member, amount: int):
    uid  = str(interaction.user.id)
    uid2 = str(member.id)
    if uid == uid2:
        return await interaction.response.send_message("❌ Can't put a bounty on yourself.", ephemeral=True)
    if member.bot:
        return await interaction.response.send_message("❌ Can't bounty a bot.", ephemeral=True)
    if amount < BOUNTY_MIN:
        return await interaction.response.send_message(f"❌ Minimum bounty is **${BOUNTY_MIN:,}**.", ephemeral=True)
    p = get_player(uid, interaction.user.display_name)
    if amount > p["money"]:
        return await interaction.response.send_message(f"❌ You only have **${p['money']:,}**.", ephemeral=True)

    # Prune expired first
    _prune_bounty(uid2)

    b = bounties.get(uid2)
    if b:
        # Check if this user already has an entry — allow stacking
        for entry in b["entries"]:
            if entry["by"] == uid:
                entry["amount"] += amount
                b["total"]      += amount
                p["money"]      -= amount
                save_data(); save_bounties()
                return await interaction.response.send_message(
                    f"✅ Added **${amount:,}** to your existing bounty on **{member.display_name}**!\nNew total: **${b['total']:,}**", ephemeral=True)
        # New contributor to existing bounty
        b["entries"].append({"by": uid, "amount": amount, "name": interaction.user.display_name})
        b["total"] += amount
    else:
        bounties[uid2] = {
            "total":      amount,
            "target_name": member.display_name,
            "placed_at":  time.time(),
            "entries":    [{"by": uid, "amount": amount, "name": interaction.user.display_name}],
        }

    p["money"] -= amount
    save_data(); save_bounties()
    total = bounties[uid2]["total"]
    embed = discord.Embed(title="🎯 Bounty Placed!", color=discord.Color.red())
    embed.add_field(name="🎯 Target",    value=member.display_name,    inline=True)
    embed.add_field(name="💰 Your Bid",  value=f"**${amount:,}**",     inline=True)
    embed.add_field(name="💰 Total Pot", value=f"**${total:,}**",      inline=True)
    embed.add_field(name="⏳ Expires",   value="In 24 hours (50% refund if unclaimed)", inline=False)
    embed.set_footer(text="Anyone who successfully steals from this player will claim the bounty!")
    await interaction.response.send_message(embed=embed, ephemeral=True)
    try:
        await interaction.channel.send(
            f"🎯 A **${total:,}** bounty has been placed on **{member.display_name}**! Steal from them to claim it!", delete_after=120)
    except Exception: pass


@bounty_group.command(name="remove", description="Remove your bounty contribution (get 50% back)")
@app_commands.describe(member="The player whose bounty you want to remove your contribution from")
async def bounty_remove(interaction: discord.Interaction, member: discord.Member):
    uid  = str(interaction.user.id)
    uid2 = str(member.id)
    _prune_bounty(uid2)
    b = bounties.get(uid2)
    if not b:
        return await interaction.response.send_message("❌ No active bounty on that player.", ephemeral=True)
    entry = next((e for e in b["entries"] if e["by"] == uid), None)
    if not entry:
        return await interaction.response.send_message("❌ You haven't contributed to this bounty.", ephemeral=True)

    refund = int(entry["amount"] * BOUNTY_REFUND)
    b["entries"].remove(entry)
    b["total"] -= entry["amount"]
    p = get_player(uid, interaction.user.display_name)
    p["money"] += refund

    if b["total"] <= 0 or not b["entries"]:
        bounties.pop(uid2, None)
        delete_bounty_db(uid2)
    save_data(); save_bounties()
    await interaction.response.send_message(
        f"✅ Removed your bounty contribution. **${refund:,}** refunded (50%).", ephemeral=True)


@bounty_group.command(name="list", description="Show all active bounties on the server")
async def bounty_list(interaction: discord.Interaction):
    await interaction.response.send_message(embed=_bounty_embed_list(), ephemeral=True)


@bounty_group.command(name="check", description="Check if a specific player has a bounty on them")
@app_commands.describe(member="The player to check")
async def bounty_check(interaction: discord.Interaction, member: discord.Member):
    uid2  = str(member.id)
    total = _bounty_total(uid2)
    if total == 0:
        return await interaction.response.send_message(
            f"🎯 **{member.display_name}** has no active bounty.", ephemeral=True)
    b    = bounties[uid2]
    left = max(0, int(b.get("placed_at", 0) + BOUNTY_TTL - time.time()))
    n    = len(b.get("entries", []))
    embed = discord.Embed(title=f"🎯 Bounty on {member.display_name}", color=discord.Color.red())
    embed.add_field(name="💰 Total Pot",    value=f"**${total:,}**",              inline=True)
    embed.add_field(name="👥 Contributors", value=str(n),                          inline=True)
    embed.add_field(name="⏳ Expires",      value=f"`{fmt_cd(left)}`",             inline=True)
    embed.set_footer(text="Steal from them to claim the bounty!")
    await interaction.response.send_message(embed=embed, ephemeral=True)


bot.tree.add_command(bounty_group)


# ================================================================
# LIVE ROULETTE HELPERS
# ================================================================
def _rou_color(n: int) -> str:
    if n == 0:                    return "green"
    if n in ROULETTE_RED_NUMS:    return "red"
    return "black"

def _rou_emoji(color: str) -> str:
    return {"red": "🔴", "black": "⚫", "green": "🟢"}[color]

def _rou_is_number_bet(bet_type: str) -> bool:
    return bet_type.isdigit() and 0 <= int(bet_type) <= 36

async def _rou_place_bet(interaction: discord.Interaction, t: str, amt: int) -> str:
    """Shared bet logic. Returns an error string or empty string on success."""
    global _roulette_channel_id
    rs  = roulette_state
    uid = str(interaction.user.id)

    p = get_player(uid, interaction.user.display_name)
    if is_jailed(p) > 0:
        return "⛓️ You can't bet while jailed!"
    if amt < ROULETTE_MIN_BET:
        return f"❌ Minimum bet is **${ROULETTE_MIN_BET:,}**."
    if amt > ROULETTE_MAX_BET:
        return f"❌ Maximum bet is **${ROULETTE_MAX_BET:,}**."
    if amt > p["money"]:
        return f"❌ Not enough cash! You have **${p['money']:,}**."

    is_live = rs["active"] and rs["phase"] == "betting"
    bucket  = rs["bets"] if is_live else rs["pending_bets"]

    player_bets = bucket.get(uid, [])
    if len(player_bets) >= ROULETTE_MAX_BETS:
        return f"❌ Max **{ROULETTE_MAX_BETS} bets** per round reached."

    p["money"] -= amt
    player_bets.append({"type": t, "amount": amt})
    bucket[uid] = player_bets

    # Ensure countdown is always a valid future time
    if rs["next_round_at"] <= time.time():
        rs["next_round_at"] = int(time.time()) + ROULETTE_ROUND_GAP

    save_data()
    return ""

def _rou_bet_wins(bet_type: str, number: int, color: str) -> bool:
    if _rou_is_number_bet(bet_type): return int(bet_type) == number
    if bet_type == "red":   return color == "red"
    if bet_type == "black": return color == "black"
    if bet_type == "green": return color == "green"
    if bet_type == "even":  return number != 0 and number % 2 == 0
    if bet_type == "odd":   return number % 2 == 1
    if bet_type == "low":   return 1 <= number <= 18
    if bet_type == "high":  return 19 <= number <= 36
    return False

def _rou_embed(time_left: int = 0, phase: str = "betting") -> discord.Embed:
    rs     = roulette_state
    pot    = sum(b["amount"] for ub in rs["bets"].values() for b in ub)
    pcount = len(rs["bets"])

    if phase == "betting":
        color = discord.Color.gold()
        desc  = f"**Place your bets!** ⏳ **{time_left}s** remaining\n`/roulette bet <type> <amount>`"
    else:
        color = discord.Color.orange()
        desc  = "🔒 Bets closed — spinning soon..."

    embed = discord.Embed(
        title=f"🎡 Roulette — Round #{rs['round_id']}",
        description=desc, color=color
    )
    embed.add_field(name="👥 Players",    value=str(pcount),      inline=True)
    embed.add_field(name="💰 Total Pot",  value=f"**${pot:,}**",  inline=True)

    # Bet type breakdown
    by_type = {}
    for ub in rs["bets"].values():
        for b in ub:
            by_type[b["type"]] = by_type.get(b["type"], 0) + b["amount"]
    if by_type:
        lines = []
        for t, amt in sorted(by_type.items(), key=lambda x: -x[1]):
            if _rou_is_number_bet(t):
                n = int(t)
                icon = _rou_emoji(_rou_color(n))
                lines.append(f"{icon} **#{t}** (32×): ${amt:,}")
            else:
                icon = _rou_emoji(t) if t in ("red", "black", "green") else "🎯"
                lines.append(f"{icon} **{t.title()}**: ${amt:,}")
        embed.add_field(name="📊 Bets", value="\n".join(lines), inline=False)

    embed.set_footer(text="Red/Black/Even/Odd/Low/High = 1.9×  •  Green = 14×  •  Number = 32×  •  Min $100  •  Max 3 bets/player")
    return embed


def _rou_spin_embed(round_id: int) -> discord.Embed:
    """One frame of the spinning animation — random numbers with colors."""
    nums = random.sample(range(0, 37), 9)
    row  = "  ".join(f"{_rou_emoji(_rou_color(n))}`{n:02d}`" for n in nums)
    return discord.Embed(
        title="🎡 Spinning...",
        description=f"**The wheel is rolling!**\n\n{row}\n\n*Round #{round_id}*",
        color=discord.Color.orange()
    )

async def _rou_push(embed, view=None):
    """Push an embed to all users who have the roulette panel open."""
    for uid, itr in list(roulette_webhooks.items()):
        try:
            await itr.edit_original_response(embed=embed, view=view or RouletteFromCasinoView(uid))
        except Exception:
            roulette_webhooks.pop(uid, None)

async def run_roulette_round():
    rs = roulette_state
    rs["active"]        = True
    rs["round_id"]     += 1
    rs["phase"]         = "betting"
    rs["_start_time"]   = time.time()
    rs["last_result"]   = None

    rs["bets"]         = {uid: list(bets) for uid, bets in rs["pending_bets"].items()}
    rs["pending_bets"] = {}

    # Betting window — push live countdown every 2s
    elapsed = 0
    while elapsed < ROULETTE_BET_WINDOW:
        left = max(0, ROULETTE_BET_WINDOW - elapsed)
        pot    = sum(b["amount"] for ub in rs["bets"].values() for b in ub)
        pcount = len(rs["bets"])
        bet_embed = discord.Embed(
            title=f"🎡 Roulette — Round #{rs['round_id']} OPEN",
            description=(
                f"⏳ **{left}s** left to place your bet!\n\n"
                f"**Bet types:** `red · black · green · even · odd · low · high · 0–36`\n"
                f"**Payouts:** Red/Black/Even/Odd/Low/High **1.9×**  •  Green **14×**  •  Number **32×**"
            ),
            color=discord.Color.gold()
        )
        bet_embed.add_field(name="👥 Players", value=str(pcount), inline=True)
        bet_embed.add_field(name="💰 Pot",     value=f"**${pot:,}**", inline=True)
        bet_embed.set_footer(text="Min $100 · Max $50,000 · Max 3 bets per round")
        await _rou_push(bet_embed)
        await asyncio.sleep(2)
        elapsed += 2

    # Lock bets
    rs["phase"] = "locked"
    lock_embed = discord.Embed(
        title="🔒 Bets Closed!",
        description=f"**{len(rs['bets'])} player(s)** locked in. Spinning now...",
        color=discord.Color.orange()
    )
    await _rou_push(lock_embed)
    await asyncio.sleep(1)

    # Spinning animation — 4 frames × 1.5s
    for _ in range(4):
        await _rou_push(_rou_spin_embed(rs["round_id"]))
        await asyncio.sleep(1.5)

    # Winning number
    winning_num   = random.randint(0, 36)
    winning_color = _rou_color(winning_num)
    winning_emoji = _rou_emoji(winning_color)

    # Payouts
    player_results = {}
    for uid, uid_bets in rs["bets"].items():
        p          = get_player(uid)
        total_in   = sum(b["amount"] for b in uid_bets)
        total_back = 0
        for b in uid_bets:
            if _rou_bet_wins(b["type"], winning_num, winning_color):
                mult = ROULETTE_NUMBER_PAYOUT if _rou_is_number_bet(b["type"]) else ROULETTE_PAYOUTS[b["type"]]
                total_back += int(b["amount"] * mult)
        won    = total_back > 0
        profit = total_back - total_in if won else -total_in
        if won:
            p["money"] += total_back
        player_results[uid] = {
            "won": won, "profit": profit,
            "returned": total_back, "lost": total_in,
            "number": winning_num, "color": winning_color,
            "emoji": winning_emoji, "round": rs["round_id"],
            "wallet": p["money"],
        }

    save_data()
    rs["last_result"]   = player_results
    rs["active"]        = False
    rs["phase"]         = "idle"
    rs["next_round_at"] = int(time.time()) + ROULETTE_ROUND_GAP

    # Push individual results to each player who has the panel open
    result_color_map = {"red": discord.Color.red(), "black": discord.Color.dark_gray(), "green": discord.Color.green()}
    for uid, itr in list(roulette_webhooks.items()):
        res = player_results.get(uid)
        if res:
            if res["won"]:
                r_embed = discord.Embed(
                    title=f"🏆 Round #{res['round']} — You Won!",
                    description=f"Result: {res['emoji']} **{res['number']}** — {res['color'].upper()}",
                    color=discord.Color.green()
                )
                r_embed.add_field(name="💰 Returned", value=f"**${res['returned']:,}**", inline=True)
                r_embed.add_field(name="📈 Profit",   value=f"**+${res['profit']:,}**",  inline=True)
                r_embed.add_field(name="💵 Wallet",   value=f"**${res['wallet']:,}**",   inline=True)
            else:
                r_embed = discord.Embed(
                    title=f"💸 Round #{res['round']} — You Lost",
                    description=f"Result: {res['emoji']} **{res['number']}** — {res['color'].upper()}",
                    color=discord.Color.red()
                )
                r_embed.add_field(name="💸 Lost",   value=f"**-${res['lost']:,}**",   inline=True)
                r_embed.add_field(name="💵 Wallet", value=f"**${res['wallet']:,}**",  inline=True)
            r_embed.set_footer(text=f"Next round in {ROULETTE_ROUND_GAP}s")
        else:
            # Spectator — show the winning number
            r_embed = discord.Embed(
                title=f"🎡 Round #{rs['round_id']} — Result",
                description=f"Result: {winning_emoji} **{winning_num}** — {winning_color.upper()}",
                color=result_color_map.get(winning_color, discord.Color.blurple())
            )
            r_embed.set_footer(text=f"Next round in {ROULETTE_ROUND_GAP}s")
        try:
            await itr.edit_original_response(embed=r_embed, view=RouletteFromCasinoView(uid))
        except Exception:
            pass

    roulette_webhooks.clear()


# ================================================================
# SERVER EVENTS — VIEW & TASK
# ================================================================
class MoneyDropView(View):
    def __init__(self, amount: int):
        super().__init__(timeout=120)
        self.amount  = amount
        self.claimed = False

    @discord.ui.button(label="💰 Grab It!", style=discord.ButtonStyle.green)
    async def grab_btn(self, interaction: discord.Interaction, button: Button):
        if self.claimed:
            return await interaction.response.send_message("❌ Too late — already claimed!", ephemeral=True)
        self.claimed = True
        self.stop()
        button.disabled = True
        uid = str(interaction.user.id)
        p   = get_player(uid, interaction.user.display_name)
        p["money"] += self.amount
        save_data()
        embed = discord.Embed(
            title="💰 Money Drop — CLAIMED!",
            description=f"**{interaction.user.display_name}** grabbed **${self.amount:,}**! 🎉",
            color=discord.Color.gold()
        )
        embed.set_footer(text="Better luck next time for everyone else!")
        await interaction.response.edit_message(embed=embed, view=self)


async def _fire_server_event():
    global active_server_event
    channel_id = _event_channel_id or _load_event_channel()
    if not channel_id:
        return

    channel = bot.get_channel(channel_id)
    if not channel:
        return

    etype  = random.choice(list(SERVER_EVENTS.keys()))
    ev_cfg = SERVER_EVENTS[etype]
    now    = time.time()

    # ── Apply instant effects ────────────────────────────────────────
    if etype == "heat_wave":
        for p in players.values():
            p["heat"] = min(10, p.get("heat", 0) + 2)
        save_data()

    elif etype == "cash_rain":
        active_uids = [uid for uid, p in players.items()
                       if time.time() - p.get("last_daily", 0) < 86400 * 7]
        if not active_uids:
            active_uids = list(players.keys())[:20]
        prize = random.randint(150, 400)
        for uid in active_uids:
            players[uid]["money"] += prize
        save_data()

    # ── Store active event state ─────────────────────────────────────
    active_server_event.clear()
    active_server_event.update({
        "type":    etype,
        "ends_at": now + ev_cfg["duration"] if ev_cfg["duration"] > 0 else 0,
    })

    # ── Build and post embed ─────────────────────────────────────────
    money_amt = 0
    view      = None

    if etype == "money_drop":
        money_amt = random.randint(300, 800)
        active_server_event["money_amount"] = money_amt
        view  = MoneyDropView(money_amt)
        embed = _event_embed(etype, money_amt)

    elif etype == "cash_rain":
        embed = discord.Embed(
            title=f"🌍 SERVER EVENT: {ev_cfg['name']}",
            color=ev_cfg["color"],
            description=f"{ev_cfg['desc']}\n\n**${prize:,}** has been added to every active player's wallet!"
        )
        embed.set_footer(text="Check your wallet — the money is already there!")

    else:
        embed = _event_embed(etype)

    try:
        msg = await channel.send(embed=embed, view=view)
        active_server_event["msg_id"]     = msg.id
        active_server_event["channel_id"] = channel_id
    except Exception as e:
        log.error(f"[ServerEvent] Failed to post event: {e}")


# ── Event interval: random 2–4 hours ────────────────────────────────
_next_event_at: float = time.time() + random.randint(7200, 14400)

@tasks.loop(minutes=1)
async def server_event_task():
    global _next_event_at, active_server_event
    now = time.time()

    # Clear expired timed events
    if active_server_event:
        dur = SERVER_EVENTS.get(active_server_event.get("type", ""), {}).get("duration", 0)
        if dur > 0 and now > active_server_event.get("ends_at", 0):
            active_server_event.clear()

    # Prune expired bounties (refund contributors)
    for uid in list(bounties.keys()):
        _prune_bounty(uid)

    # Fire next event when the timer expires
    if now >= _next_event_at:
        _next_event_at = now + random.randint(7200, 14400)  # reset 2–4h from now
        await _fire_server_event()


# ================================================================
# LOTTERY DRAW TASK
# ================================================================
async def _run_lottery_draw():
    """Pick a winner and distribute the jackpot."""
    tickets = lottery_state.get("tickets", {})
    pot     = lottery_state.get("pot", 0) + LOTTERY_SEED
    if not tickets:
        # No players — reset with seed carried over
        lottery_state["draw_at"] = _next_sunday_ts()
        save_lottery()
        return
    pool   = [uid for uid, cnt in tickets.items() for _ in range(cnt)]
    winner = random.choice(pool)
    wp     = get_player(winner)
    amount = int(pot * LOTTERY_WINNER_CUT)
    wp["money"] += amount
    # Gang bonus
    gang_share = pot - amount
    gid, g = _find_player_gang(winner)
    if g:
        g["bank"] = g.get("bank", 0) + gang_share
        save_gangs()
    lottery_state["last_winner"] = {
        "uid":    winner,
        "name":   wp.get("name", f"Player {winner[:6]}"),
        "amount": amount,
    }
    lottery_state["pot"]     = 0
    lottery_state["tickets"] = {}
    lottery_state["draw_at"] = _next_sunday_ts()
    save_data(); save_lottery()
    log.info(f"Lottery drawn: winner {winner} won ${amount:,}")

@tasks.loop(hours=1)
async def lottery_draw_task():
    if time.time() >= lottery_state.get("draw_at", 0):
        await _run_lottery_draw()


# ================================================================
# BACKGROUND TASKS
# ================================================================
@tasks.loop(minutes=5)
async def territory_battle_tick():
    now = time.time()
    for tid, td in list(territories.items()):
        b = td.get("battle")
        if b and now > b["ends_at"]:
            await _resolve_battle(tid)

@tasks.loop(minutes=10)
async def territory_expiry_tick():
    now = time.time()
    for tid, td in list(territories.items()):
        if td.get("owner_gid") and td.get("expires_at", 0) and now > td["expires_at"]:
            territories[tid]["owner_gid"]  = None
            territories[tid]["expires_at"] = 0
            territories[tid]["battle"]     = None
            territories[tid]["last_income"] = 0
            save_territory(tid)
            log.info(f"Territory {tid} expired and is now unclaimed.")

@tasks.loop(minutes=20)
async def heat_decay_task():
    changed = False
    for p in players.values():
        if p.get("heat", 0) > 0:
            p["heat"] = max(0, p["heat"] - 1); changed = True
    if changed: save_data()

@tasks.loop(hours=24)
async def economy_daily_tick():
    for uid, p in players.items():
        eligible = bank_interest_eligible(p)
        if eligible > 0:
            rate = BANK_INTEREST_RATE + _career_bank_interest_bonus(p)
            if "bank_10" in _territory_perks_for_player(uid):
                rate += 0.10
            interest = min(int(eligible * rate), BANK_INTEREST_CAP)
            p["bank"] += interest
        worth = p.get("money", 0) + p.get("bank", 0)
        if worth > TAX_THRESHOLD:
            tax = min(int(worth * TAX_RATE), TAX_CAP)
            if p["money"] >= tax:
                p["money"] -= tax
            else:
                leftover = tax - p["money"]; p["money"] = 0
                p["bank"] = max(0, p["bank"] - leftover)
    save_data()

@tasks.loop(seconds=ROULETTE_ROUND_GAP)
async def roulette_loop():
    if roulette_state["active"]:
        return
    roulette_state["next_round_at"] = int(time.time()) + ROULETTE_ROUND_GAP
    asyncio.create_task(run_roulette_round())

# ================================================================
# EVENTS
# ================================================================
@tasks.loop(seconds=30)
async def refresh_menus():
    dead = []
    for uid, msg in list(menu_messages.items()):
        try:
            p      = get_player(uid)
            member = msg.guild.get_member(int(uid)) if msg.guild else bot.get_user(int(uid))
            if member:
                await msg.edit(embed=_main_menu_embed(member, p))
        except discord.NotFound:
            dead.append(uid)
        except Exception:
            pass
    for uid in dead:
        menu_messages.pop(uid, None)

@bot.event
async def on_ready():
    log.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    await bot.change_presence(activity=discord.Game(name="🎰 /play to start"))
    await bot.tree.sync()
    log.info("Global slash commands synced.")
    refresh_menus.start()
    heat_decay_task.start()
    economy_daily_tick.start()
    territory_battle_tick.start()
    territory_expiry_tick.start()
    roulette_state["next_round_at"] = int(time.time()) + ROULETTE_ROUND_GAP
    roulette_loop.start()
    _load_event_channel()
    server_event_task.start()
    lottery_draw_task.start()

@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def sync_cmd(ctx):
    bot.tree.copy_global_to(guild=ctx.guild)
    synced = await bot.tree.sync(guild=ctx.guild)
    await ctx.send(f"✅ Synced {len(synced)} slash commands to this server instantly.", delete_after=10)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot: return
    uid = str(message.author.id)
    if uid in players: players[uid]["name"] = message.author.display_name
    await bot.process_commands(message)

@bot.tree.error
async def on_tree_error(interaction: discord.Interaction, error):
    if isinstance(error, discord.app_commands.CommandInvokeError):
        inner = error.original
        if isinstance(inner, discord.NotFound) and inner.code == 10062:
            return  # Expired interaction (stale after restart) — silently ignore
    msg = str(error)
    log.error(f"Slash error in /{interaction.command.name if interaction.command else '?'}: {msg}")
    try:
        await interaction.response.send_message("❌ Something went wrong.", ephemeral=True)
    except Exception:
        pass


if not TOKEN:
    log.error("DISCORD_TOKEN not found in .env!")
else:
    bot.run(TOKEN)
