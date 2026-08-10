# 🟦 Bale (بله) Platform Adapter for Hermes Agent

<p align="center">
  <strong>Add the Iranian Bale messenger as a platform to your Hermes Agent</strong>
</p>

---

## 📖 About

This plugin adds [Bale (بله)](https://bale.ai/) — the Iranian messaging platform — as a fully supported gateway platform in [Hermes Agent](https://github.com/NousResearch/hermes-agent).

Bale's Bot API is **Telegram-compatible**, using `https://tapi.bale.ai/bot<TOKEN>/` as the base URL. This adapter implements:

- ✅ Long-polling message reception (`getUpdates`)
- ✅ Text message sending (`sendMessage`)
- ✅ Typing indicators (`sendChatAction`)
- ✅ Per-user access control (allowlist / allow-all)
- ✅ Cron job delivery (`standalone_sender_fn`)
- ✅ Interactive setup wizard (`hermes gateway setup`)
- ✅ Auto-reconnect with exponential backoff
- ✅ Session source routing (DM + group support)

---

## 🚀 Installation

### Option 1: Manual install

```bash
# 1. Clone this repo
git clone https://github.com/YOUR_USERNAME/bale-hermes.git

# 2. Copy the plugin into your Hermes plugins directory
mkdir -p ~/.hermes/plugins/platforms/bale
cp bale-hermes/adapter.py      ~/.hermes/plugins/platforms/bale/
cp bale-hermes/__init__.py     ~/.hermes/plugins/platforms/bale/
cp bale-hermes/plugin.yaml     ~/.hermes/plugins/platforms/bale/

# 3. Install dependency
pip install aiohttp

# 4. Restart Hermes gateway
hermes gateway restart
```

### Option 2: From source tree (developers)

```bash
# If you have the hermes-agent source:
cp -r bale-hermes/ /path/to/hermes-agent/plugins/platforms/bale/
```

---

## ⚙️ Configuration

### Step 1: Create a Bale Bot

1. Open **Bale** on your phone
2. Search for **@Bot_Father**
3. Send `/newbot`
4. Follow the prompts to name your bot
5. Copy the **bot token** (looks like `123456789:ABCdefGhi...`)

### Step 2: Configure Hermes

**Option A — Interactive setup:**

```bash
hermes gateway setup
# Select "Bale" from the platform list
# Paste your bot token when prompted
```

**Option B — Manual (edit `~/.hermes/.env`):**

```bash
# Required
BALE_BOT_TOKEN=your_bot_token_here

# Optional: restrict access
BALE_ALLOWED_USERS=49036693,12345678
# OR allow everyone:
BALE_ALLOW_ALL_USERS=true

# Optional: default chat for cron/notification delivery
BALE_HOME_CHANNEL=49036693
```

### Step 3: Restart the gateway

```bash
hermes gateway restart
```

---

## 🧪 Verify

After restart, check that Bale is connected:

```bash
hermes gateway status
```

Then send a message to your bot in Bale. On first contact, you'll get a **pairing code**. Approve it:

```bash
hermes pairing approve bale <CODE>
```

Send another message — the bot should respond! 🎉

---

## 📁 Repository Structure

```
bale-hermes/
├── adapter.py       # Main adapter (BaleAdapter + register hooks)
├── __init__.py      # Plugin entry point
├── plugin.yaml      # Plugin metadata and env var definitions
├── LICENSE          # MIT License
├── README.md        # You are here
└── .gitignore
```

---

## 🔧 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BALE_BOT_TOKEN` | ✅ | Bot token from @Bot_Father |
| `BALE_ALLOWED_USERS` | ❌ | Comma-separated chat IDs allowed to use the bot |
| `BALE_ALLOW_ALL_USERS` | ❌ | Set to `true` to allow all users |
| `BALE_HOME_CHANNEL` | ❌ | Default chat ID for cron/notification delivery |

---

## 🏗️ How It Works

```
┌─────────────┐     getUpdates (long-poll)     ┌──────────────────┐
│  Bale API   │ ◄───────────────────────────── │  BaleAdapter     │
│  (tapi.     │ ─────────────────────────────► │  (async polling) │
│   bale.ai)  │     sendMessage / typing       │                  │
└─────────────┘                                └──────┬───────────┘
                                                      │
                                                      ▼
                                              ┌──────────────────┐
                                              │  Hermes Gateway  │
                                              │  → AI Agent      │
                                              │  → Tools         │
                                              │  → Skills        │
                                              └──────────────────┘
```

The adapter uses `aiohttp` for fully async I/O, matching Hermes' async event loop. Messages are received via long-polling (30s timeout), parsed into `MessageEvent` objects, and dispatched through `BasePlatformAdapter.handle_message()`.

---

## 📋 Requirements

- [Hermes Agent](https://github.com/NousResearch/hermes-agent) (v1.0+)
- Python 3.11+
- `aiohttp` (`pip install aiohttp`)
- A Bale bot token (free from @Bot_Father in Bale)

---

## 📝 License

MIT License — see [LICENSE](LICENSE).

---

## 👤 Author

**Erfan Rahmat Zadeh**

---

## 🤝 Contributing

PRs welcome! If you find a bug or want to add a feature (e.g., photo/video/sticker support, inline keyboards), feel free to open an issue or submit a pull request.

---

<div align="center">
  Made with ❤️ for the Iranian developer community
</div>
