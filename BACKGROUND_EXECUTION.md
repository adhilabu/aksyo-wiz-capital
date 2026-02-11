# Background Execution Guide

## Telegram Listener - Background Mode

The Telegram listener now intelligently handles both **first-time authentication** and **background execution**.

## How It Works

### First Run (No Session File)

When you run the script for the first time:

```bash
./run_telegram_listener.sh
```

**What happens:**
1. ✅ Detects no session file exists
2. ✅ Runs in **FOREGROUND** mode
3. ⌨️ You enter Telegram auth code (sent to your phone)
4. ✅ Session saved to `telegram_session.session`
5. ❓ Asks if you want to continue in background
6. ✅ If yes, starts in background with `nohup`

### Subsequent Runs (Session Exists)

After the first successful authentication:

```bash
./run_telegram_listener.sh
```

**What happens:**
1. ✅ Detects session file exists
2. ✅ Automatically starts in **BACKGROUND** mode
3. 📝 Logs to `logs/telegram_listener.log`
4. 🆔 Saves PID to `telegram_listener.pid`

## Process Management

### Start in Background
```bash
./run_telegram_listener.sh
```

### View Logs (Live)
```bash
tail -f logs/telegram_listener.log
```

### Check Status
```bash
ps -p $(cat telegram_listener.pid)
```

### Stop Listener
```bash
./stop_telegram_listener.sh
# OR manually:
kill $(cat telegram_listener.pid)
```

## Protection Features

### Duplicate Prevention
- ✅ Checks if already running before starting
- ✅ Prevents multiple instances
- ✅ Cleans up stale PID files

### Session Management
- ✅ Saves session after first auth
- ✅ No repeated auth codes needed
- ✅ Persistent authentication

### Error Handling
- ✅ FloodWaitError auto-retry
- ✅ Connection error recovery
- ✅ Message parsing failures logged

## Files Created

| File | Purpose |
|------|---------|
| `telegram_session.session` | Telegram auth session (don't delete!) |
| `telegram_listener.pid` | Process ID for background instance |
| `logs/telegram_listener.log` | All listener logs |

## Example Flow

### First Time Setup:
```bash
$ ./run_telegram_listener.sh
⚠️  First-time setup detected (no session file found)
Running in FOREGROUND mode for authentication...

Please enter your phone (or bot token): +919562025919
Please enter the code you received: 12345
✅ Authentication complete! Session file saved.

Do you want to run in BACKGROUND mode now? (y/n): y
Starting in background mode...
✅ Telegram listener started in background (PID: 12345)
📝 Logs: logs/telegram_listener.log
```

### All Subsequent Runs:
```bash
$ ./run_telegram_listener.sh
Session file exists. Starting in BACKGROUND mode...

✅ Telegram listener started in background
   PID: 67890
   Logs: logs/telegram_listener.log

Useful commands:
  📝 View logs:  tail -f logs/telegram_listener.log
  🛑 Stop:       kill $(cat telegram_listener.pid)
  ℹ️  Status:     ps -p $(cat telegram_listener.pid)
```

## Security Notes

⚠️ **Important:**
- **Keep `telegram_session.session` secure** - it contains your auth token
- Don't commit session file to git (already in `.gitignore`)
- If compromised, delete it and re-authenticate

## Troubleshooting

### "Already running" error
```bash
# Stop the existing process
./stop_telegram_listener.sh
# Then start fresh
./run_telegram_listener.sh
```

### Session expired
```bash
# Delete session and re-authenticate
rm telegram_session.session
./run_telegram_listener.sh
```

### Not receiving messages
```bash
# Check logs
tail -f logs/telegram_listener.log
# Verify channel name is correct in .env
```

## Summary

✅ **Intelligent mode detection** - foreground for first auth, background thereafter  
✅ **Automatic process management** - PID tracking and duplicate prevention  
✅ **Easy log viewing** - all output to `logs/telegram_listener.log`  
✅ **Graceful shutdown** - stop script provided  
✅ **Session persistence** - authenticate once, run forever  

The listener is production-ready for 24/7 background operation!
