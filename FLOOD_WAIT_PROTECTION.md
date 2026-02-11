# FloodWaitError Protection

## What is FloodWaitError?

`FloodWaitError` is Telegram's way of telling your script to slow down. This happens when you make too many API requests in a short time period.

## Why It Matters

- **Userbots (like yours)** are expected to behave like humans
- Telegram monitors for unusual activity patterns
- **Ignoring FloodWaitError can lead to account bans**

## How We Handle It

The Telegram listener now automatically handles `FloodWaitError`:

```python
except FloodWaitError as e:
    wait_seconds = e.seconds
    logger.warning(f"⚠️  FloodWaitError: Must wait {wait_seconds} seconds...")
    await asyncio.sleep(wait_seconds)
    logger.info(f"✅ Rate limit wait complete. Resuming...")
```

### What Happens:
1. **Telegram rate limits detected** → Exception raised
2. **Script automatically pauses** → Sleeps for required time
3. **Resumes normal operation** → After wait period completes
4. **No manual intervention needed** → Fully automated

## Your Current Usage: ✅ SAFE

You are **only listening** to `@capitalcom_international` for trading signals. This is passive behavior and very unlikely to trigger rate limits.

### Safe Activities:
- ✅ Listening to channels
- ✅ Reading new messages as they arrive
- ✅ Parsing signal text
- ✅ Storing data in database

### Risky Activities (Avoid):
- ❌ Sending 50+ messages per minute
- ❌ Mass forwarding to other channels
- ❌ Downloading thousands of historical messages without delays
- ❌ Adding/removing users rapidly

## If You See FloodWaitError

**Don't panic!** The script will:
1. Log a warning with wait time
2. Automatically pause
3. Resume when safe

Example log output:
```
⚠️  FloodWaitError: Telegram requires a wait of 60 seconds.
   This is normal if processing many messages. Pausing...
✅ Rate limit wait complete (60s). Resuming normal operation.
```

## Best Practices

1. **Let it run** - Don't restart the script if you see FloodWaitError
2. **Monitor logs** - Check for frequent FloodWaitErrors (sign to optimize)
3. **Stay passive** - Only listen, don't mass-send or forward
4. **Use rate limits** - If adding features, add delays between actions

## Summary

✅ **FloodWaitError handling added** to `telegram_listener.py`  
✅ **Automatic compliance** with Telegram's rate limits  
✅ **Account protection** from potential bans  
✅ **No manual intervention** required  

Your script is now production-ready and safe!
