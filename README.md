# 🐴 Mambo the Omniscient

A comprehensive Discord bot for managing Uma Musume: Pretty Derby club data, fan tracking, and leaderboards.

![Discord](https://img.shields.io/badge/Discord-Bot-5865F2?style=flat&logo=discord&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)

[![Discord Server](https://img.shields.io/discord/1015976866856812594?color=5865F2&logo=discord&logoColor=white&label=Join%20our%20Discord)](https://discord.com/invite/touchclub)
[![Top.gg](https://img.shields.io/badge/Top.gg-Vote-FF3366?logo=discord&logoColor=white)](https://top.gg/bot/1312444816071720980/vote)

## ✨ Features

### 📊 Club Management
- **Leaderboard System** - View and compare member fan counts across clubs
- **Stats Tracking** - Track daily and cumulative fan gains for each member
- **Multi-Club Support** - Manage multiple clubs with different settings
- **Yui Logic** - Smart target calculation for late joiners

### 🔍 Search & Discovery
- **Club Search** - Search for any club using Trainer ID via uma.moe API
- **Member Stats** - View detailed statistics for individual members
- **Global Leaderboard** - See top performers across all tracked clubs

### 🔔 Notifications
- **Schedule System** - Auto-fetch and display game events from TazunaDiscordBot
- **Rank Updates** - Automatic club rank updates twice daily
- **Change History** - Log all data changes to a designated channel

### 🛠️ Admin Tools
- **God Mode Panel** - Quick access to admin functions
- **Club Setup** - Easy wizard for adding new clubs
- **Channel Management** - Control which channels can use bot commands
- **Cache Management** - Clear and refresh data caches

### 🔗 Profile Linking
- **Profile Verification** - Link Discord accounts to in-game profiles via OCR
- **Verified Member Display** - Show verification status on stats

## 🚀 Quick Start

### Prerequisites

- Python 3.9 or higher
- Discord Bot Token
- Google Cloud Service Account with Sheets API enabled
- (Optional) Supabase account for database backup

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/senchouXflare/Mambo-the-Omniscient.git
   cd Mambo-the-Omniscient
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Add Google credentials**
   - Place your `credentials.json` file in the root directory

5. **Run the bot**
   ```bash
   python bot.py
   ```

## ⚙️ Configuration

### Environment Variables

Copy `.env.example` to `.env` and fill in your values:

| Variable | Description |
|----------|-------------|
| `DISCORD_TOKEN` | Your Discord bot token |
| `GOOGLE_SHEET_ID` | Google Sheets document ID |
| `LOGGING_CHANNEL_ID` | Channel for logging changes |
| `REQUEST_CHANNEL_ID` | Channel for club requests |
| `ADMIN_ROLE_IDS` | Comma-separated admin role IDs |
| `GOD_MODE_USER_IDS` | Comma-separated god mode user IDs |

### Google Sheets Setup

Your Google Sheet should have:
- `Clubs_Config` sheet with club configurations
- Individual data sheets for each club (e.g., `ClubName_Data`)
- Member sheets for each club (e.g., `ClubName_Members`)

## 📋 Commands

### User Commands
| Command | Description |
|---------|-------------|
| `/leaderboard [club]` | View club leaderboard |
| `/stats [club] [member]` | View member statistics |
| `/club_list` | List all tracked clubs |
| `/search_club [trainer_id]` | Search for club by Trainer ID |
| `/schedule` | View upcoming game events |
| `/help` | Show help information |

### Admin Commands
| Command | Description |
|---------|-------------|
| `/club_setup` | Add a new club |
| `/god_mode` | Open admin panel |
| `/clear_cache` | Clear data caches |
| `/sync` | Manual data sync |

## 📁 Project Structure

```
Mambo-the-Omniscient/
├── bot.py                 # Main bot file
├── data_updater.py        # Google Sheets data handling
├── auto_sync_helpers.py   # uma.moe API integration
├── god_mode_panel.py      # Admin panel UI
├── supabase_manager.py    # Supabase database manager
├── sync_command.py        # Sync command implementation
├── requirements.txt       # Python dependencies
├── .env.example           # Environment template
├── .gitignore             # Git ignore rules
└── assets/                # Static assets
```

## 🔒 Security Notes

- Never commit `.env` or `credentials.json` to version control
- All sensitive IDs are loaded from environment variables
- The `.gitignore` is configured to exclude sensitive files

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [uma.moe](https://uma.moe) - For the club data API
- [TazunaDiscordBot](https://github.com/JustWastingTime/TazunaDiscordBot) - For schedule data
- [Chronogenesis](https://chronogenesis.net) - For additional club data

## 💖 Support

If you find this bot helpful, consider:
- ⭐ Starring this repository
- ☕ [Buying me a coffee](https://ko-fi.com/senchouxflare_7b7m)
- 🗳️ [Voting on Top.gg](https://top.gg/bot/1312444816071720980/vote)

---

Made with ❤️ for the Uma Musume community
