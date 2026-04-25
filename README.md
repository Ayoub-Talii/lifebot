# LifeBot 🤖

A feature-rich Discord economy & crime bot built with discord.py and MongoDB.

## Features

### 💰 Economy
- Jobs, salaries, bank, wallet, interest system
- Training system to boost stats
- Career Paths (Corporate / Hustler / Shadow) unlocked at Level 10
- Properties — buy apartments, storefronts, warehouses, penthouses for passive income
- Prestige system

### 🔫 Crime & Streets
- Crime commands with heat system and jail mechanic
- Steal from other players (masks, gloves, tracker items)
- Scan targets with a scanner item
- Revenge system

### 🏴 Gang System
- Create and manage gangs
- Territory control across 20 zones (5 rarity tiers)
- Gang bank, gang XP, gang levels
- Gang Heist — 4 tiers (Corner Store → Federal Reserve) with role selection
- Gang War system
- Bounty board

### 🎰 Casino
- Blackjack, Slots, Coin Flip
- Roulette (live shared rounds)
- Mines (pick tiles, dodge bombs, cash out anytime)
- Chicken Cross (cross lanes for multipliers)
- Scratch Cards
- Multiplayer rooms

### 🎯 Progression
- XP & leveling system
- 20+ Achievements across 5 categories
- Weekly Challenges (5 missions, refreshed every Monday)
- Leaderboard

### 🎉 Server Events
- Automated events every 2–4 hours (Money Drop, Heat Wave, XP Boost, Immunity Window)
- Configurable event channel

### 🎟️ Lottery
- Weekly jackpot drawing every Sunday
- 80% to winner, 20% to gang bank

## Setup

### Requirements
- Python 3.10+
- MongoDB Atlas cluster
- Discord bot token

### Install
```bash
pip install -r requirements.txt
```

### Environment Variables
Create a `.env` file:
```
DISCORD_TOKEN=your_discord_token
MONGO_URI=your_mongodb_uri
```

### Run
```bash
python bot.py
```

## Deployment
Hosted on [Railway](https://railway.app) with auto-restart on failure.

## Tech Stack
- [discord.py](https://github.com/Rapptz/discord.py) 2.7+
- [PyMongo](https://pymongo.readthedocs.io/)
- MongoDB Atlas
- Railway
