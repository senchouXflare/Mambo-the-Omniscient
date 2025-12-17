"""
One-time script to sync existing channels to web dashboard.
Run this once to populate the channels on the web.
"""
import json
import os
import asyncio
import aiohttp

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
ALLOWED_CHANNELS_CONFIG_FILE = os.path.join(SCRIPT_DIR, "allowed_channels_config.json")
OCR_SERVICE_URL = "http://2.56.246.119:30404"

def load_channels():
    """Load channels from config file"""
    if os.path.exists(ALLOWED_CHANNELS_CONFIG_FILE):
        try:
            with open(ALLOWED_CHANNELS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('channels', [])
        except Exception as e:
            print(f"Error loading channels: {e}")
    return []

async def sync_channels():
    """Sync channels to web dashboard"""
    channels = load_channels()
    print(f"Found {len(channels)} channels")
    
    if not channels:
        print("No channels found in config file.")
        print(f"Looking for: {ALLOWED_CHANNELS_CONFIG_FILE}")
        return
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{OCR_SERVICE_URL}/api/channels",
                json={"channels": channels},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                result = await resp.json()
                if result.get('success'):
                    print(f"✅ Successfully synced {len(channels)} channels to web dashboard!")
                else:
                    print(f"❌ Error: {result}")
    except Exception as e:
        print(f"❌ Error syncing: {e}")

if __name__ == "__main__":
    asyncio.run(sync_channels())
