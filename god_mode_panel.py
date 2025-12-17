"""
God Mode Control Panel
Persistent message with buttons for bot owner commands
"""

import discord
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput
import json
import os
import sys
import time
import subprocess
import datetime
import pytz


# Control Panel Channel - loaded from environment variables
GOD_MODE_PANEL_CHANNEL_ID = int(os.getenv('GOD_MODE_PANEL_CHANNEL_ID', '0'))
GOD_MODE_USER_ID = int(os.getenv('GOD_MODE_USER_ID', '0'))


class GodModeControlPanel(View):
    """Persistent control panel with God Mode buttons"""
    
    def __init__(self):
        super().__init__(timeout=None)  # Persistent view
    
    def is_god_mode(self, interaction: discord.Interaction) -> bool:
        """Check if user is God mode"""
        return interaction.user.id == GOD_MODE_USER_ID
    
    # Row 1: Cache Management
    @discord.ui.button(
        label="📊 Cache Stats",
        style=discord.ButtonStyle.secondary,
        custom_id="gm_cache_stats",
        row=0
    )
    async def cache_stats(self, interaction: discord.Interaction, button: Button):
        """Show cache statistics"""
        if not self.is_god_mode(interaction):
            await interaction.response.send_message("❌ Unauthorized", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            from bot import smart_cache, client
            stats = smart_cache.get_stats()
            
            embed = discord.Embed(
                title="📊 Cache Statistics",
                color=discord.Color.blue()
            )
            
            embed.add_field(
                name="Smart Cache (In-Memory)",
                value=(
                    f"**Entries:** {stats['total_entries']}\n"
                    f"**Size:** {stats['total_size_mb']} MB"
                ),
                inline=False
            )
            
            embed.add_field(
                name="Bot Cache",
                value=(
                    f"**Clubs:** {len(client.config_cache)}\n"
                    f"**Members:** {sum(len(m) for m in client.member_cache.values())}"
                ),
                inline=False
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    
    @discord.ui.button(
        label="🗑️ Clear Cache",
        style=discord.ButtonStyle.danger,
        custom_id="gm_clear_cache",
        row=0
    )
    async def clear_cache(self, interaction: discord.Interaction, button: Button):
        """Clear all cache"""
        if not self.is_god_mode(interaction):
            await interaction.response.send_message("❌ Unauthorized", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            from bot import smart_cache
            before_stats = smart_cache.get_stats()
            total_entries = before_stats['total_entries']
            total_size_mb = before_stats['total_size_mb']
            
            smart_cache.invalidate()
            
            await interaction.followup.send(
                f"✅ **Cache cleared!**\n\n"
                f"**Cleared:**\n"
                f"• {total_entries} entries\n"
                f"• {total_size_mb} MB",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    
    # Row 2: Bot Control
    @discord.ui.button(
        label="🔄 Restart Bot",
        style=discord.ButtonStyle.danger,
        custom_id="gm_restart",
        row=1
    )
    async def restart_bot(self, interaction: discord.Interaction, button: Button):
        """Restart the bot"""
        if not self.is_god_mode(interaction):
            await interaction.response.send_message("❌ Unauthorized", ephemeral=True)
            return
        
        await interaction.response.send_message("🔄 Restarting bot...", ephemeral=False)
        message = await interaction.original_response()
        
        print(f"--- RESTARTING (from control panel by {interaction.user.id}) ---")
        
        # Save restart info
        try:
            from bot import RESTART_FILE_PATH
            restart_data = {
                "channel_id": interaction.channel_id,
                "message_id": message.id,
                "start_time": time.time()
            }
            with open(RESTART_FILE_PATH, "w") as f:
                json.dump(restart_data, f)
        except:
            pass
        
        # Restart
        try:
            os.execv(sys.executable, ['python'] + sys.argv)
        except Exception as e:
            print(f"Restart failed: {e}")
            await message.edit(content="❌ Restart failed!")
    
    # Row 2: Task Triggers
    @discord.ui.button(
        label="🔄 Refresh Cache",
        style=discord.ButtonStyle.primary,
        custom_id="gm_refresh_cache",
        row=1
    )
    async def refresh_cache(self, interaction: discord.Interaction, button: Button):
        """Refresh bot cache from Google Sheets"""
        if not self.is_god_mode(interaction):
            await interaction.response.send_message("❌ Unauthorized", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            from bot import client
            
            # Reset cooldown to force refresh
            client.last_cache_update_time = 0
            
            start_time = time.time()
            await client.update_caches()
            elapsed = time.time() - start_time
            
            await interaction.followup.send(
                f"✅ **Cache refreshed!**\n\n"
                f"**Loaded:**\n"
                f"• {len(client.config_cache)} clubs\n"
                f"• {sum(len(m) for m in client.member_cache.values())} members\n"
                f"⏱️ Time: {elapsed:.1f}s",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    
    @discord.ui.button(
        label="📡 Sync Data",
        style=discord.ButtonStyle.success,
        custom_id="gm_sync_data",
        row=1
    )
    async def sync_data(self, interaction: discord.Interaction, button: Button):
        """Manually trigger full data sync: ranks + member data from uma.moe API"""
        if not self.is_god_mode(interaction):
            await interaction.response.send_message("❌ Unauthorized", ephemeral=True)
            return
        
        await interaction.response.send_message(
            "📡 **Starting full data sync...**\n"
            "• Fetching ranks from uma.moe API\n"
            "• Syncing member daily data to Google Sheets\n"
            "• Applying Yui logic for late joiners\n\n"
            "This may take a while...", 
            ephemeral=True
        )
        
        try:
            from bot import update_club_data_task
            
            # Run the merged task (updates ranks + syncs member data)
            await update_club_data_task.coro()
            
            await interaction.edit_original_response(
                content=(
                    "✅ **Full data sync complete!**\n\n"
                    "• Ranks updated in Clubs_Config\n"
                    "• Member data synced to each club's sheet\n"
                    "• Check console for details"
                )
            )
        except Exception as e:
            await interaction.edit_original_response(content=f"❌ Error: {e}")
    
    @discord.ui.button(
        label="📤 Sync to Supabase",
        style=discord.ButtonStyle.primary,
        custom_id="gm_sync_supabase",
        row=1
    )
    async def sync_supabase(self, interaction: discord.Interaction, button: Button):
        """Manually trigger sync to Supabase"""
        if not self.is_god_mode(interaction):
            await interaction.response.send_message("❌ Unauthorized", ephemeral=True)
            return
        
        await interaction.response.send_message("📤 **Starting Supabase sync...**\nThis may take a while.", ephemeral=True)
        
        try:
            from bot import auto_sync_to_supabase
            
            # Run the task manually
            await auto_sync_to_supabase.coro()
            
            await interaction.edit_original_response(
                content="✅ **Supabase sync complete!**\nCheck console for details."
            )
        except Exception as e:
            await interaction.edit_original_response(content=f"❌ Error: {e}")
    
    # Row 3: Channel Management
    @discord.ui.button(
        label="🚫 Clear Channels",
        style=discord.ButtonStyle.danger,
        custom_id="gm_clear_channels",
        row=2
    )
    async def clear_channels(self, interaction: discord.Interaction, button: Button):
        """Clear all channel restrictions"""
        if not self.is_god_mode(interaction):
            await interaction.response.send_message("❌ Unauthorized", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            from bot import ALLOWED_CHANNELS_CONFIG_FILE, config
            
            if os.path.exists(ALLOWED_CHANNELS_CONFIG_FILE):
                os.remove(ALLOWED_CHANNELS_CONFIG_FILE)
            
            config.ALLOWED_CHANNEL_IDS = []
            
            await interaction.followup.send(
                "✅ **All channel restrictions cleared**\n"
                "Bot can now be used in all channels",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)


def create_control_panel_embed():
    """Create embed for control panel"""
    embed = discord.Embed(
        title="⚡ GOD MODE CONTROL PANEL",
        description=(
            "**System Controls**\n"
            "Essential bot management functions\n\n"
            f"🔐 **Access:** <@{GOD_MODE_USER_ID}> only"
        ),
        color=0xFF0000,
        timestamp=datetime.datetime.now()
    )
    
    embed.add_field(
        name="📊 Row 1: Cache Management",
        value="• **Cache Stats** - View cache info\n• **Clear Cache** - Clear all cached data",
        inline=False
    )
    
    embed.add_field(
        name="🔄 Row 2: Task Triggers",
        value=(
            "• **Restart Bot** - Restart the bot\n"
            "• **Refresh Cache** - Reload from Google Sheets\n"
            "• **Sync Data** - Full sync: ranks + member data from uma.moe\n"
            "• **Sync to Supabase** - Sync data to Supabase"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🔐 Row 3: Security",
        value="• **Clear Channels** - Remove all channel restrictions",
        inline=False
    )
    
    embed.set_footer(text="Control Panel v2.3 - With Full Data Sync")
    
    return embed


async def update_god_mode_panel(client):
    """Update or create God Mode control panel"""
    try:
        channel = client.get_channel(GOD_MODE_PANEL_CHANNEL_ID)
        if not channel:
            print(f"⚠️ Cannot find God Mode panel channel: {GOD_MODE_PANEL_CHANNEL_ID}")
            return
        
        embed = create_control_panel_embed()
        view = GodModeControlPanel()
        
        # Try to load existing message ID
        config_file = os.path.join(os.path.dirname(__file__), "god_mode_panel_config.json")
        message_id = None
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    data = json.load(f)
                    message_id = data.get('message_id')
            except:
                pass
        
        # Try to edit existing message
        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed, view=view)
                print("✅ Updated God Mode control panel")
                return
            except:
                pass
        
        # Create new message
        message = await channel.send(embed=embed, view=view)
        
        # Save message ID
        with open(config_file, 'w') as f:
            json.dump({
                'message_id': message.id,
                'channel_id': channel.id,
                'created_at': datetime.datetime.now().isoformat()
            }, f)
        
        print(f"✅ Created God Mode control panel (Message ID: {message.id})")
        
    except Exception as e:
        print(f"❌ Error updating God Mode panel: {e}")
        import traceback
        traceback.print_exc()
