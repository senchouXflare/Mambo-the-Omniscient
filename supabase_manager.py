"""
Supabase Database Manager
Replaces GoogleSheetsManager for better performance and reliability
"""

from supabase import create_client, Client
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime, date
import os

# ============================================================================
# SUPABASE CONFIGURATION (from environment variables)
# ============================================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")  # Service role key

class SupabaseManager:
    """Manages Supabase PostgreSQL database connection"""
    
    def __init__(self):
        """Initialize Supabase client"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connected to Supabase")
    
    # ========================================================================
    # CLUB CONFIGURATION METHODS
    # ========================================================================
    
    def get_all_clubs(self) -> List[Dict]:
        """Get all clubs configuration"""
        response = self.client.table('clubs_config').select('*').execute()
        return response.data
    
    def get_club_by_name(self, club_name: str) -> Optional[Dict]:
        """Get specific club configuration"""
        response = self.client.table('clubs_config')\
            .select('*')\
            .eq('club_name', club_name)\
            .single()\
            .execute()
        return response.data if response.data else None
    
    def get_clubs_by_server(self, server_id: str) -> List[Dict]:
        """Get all clubs for a specific Discord server"""
        response = self.client.table('clubs_config')\
            .select('*')\
            .eq('server_id', str(server_id))\
            .execute()
        return response.data
    
    def create_club(self, club_data: Dict) -> Dict:
        """Create a new club"""
        response = self.client.table('clubs_config')\
            .insert(club_data)\
            .execute()
        return response.data[0]
    
    def update_club(self, club_name: str, updates: Dict) -> Dict:
        """Update club configuration"""
        response = self.client.table('clubs_config')\
            .update(updates)\
            .eq('club_name', club_name)\
            .execute()
        return response.data[0] if response.data else None
    
    # ========================================================================
    # CLUB MEMBERS METHODS
    # ========================================================================
    
    def get_club_members(self, club_name: str) -> List[str]:
        """Get member list for a club"""
        response = self.client.table('club_members')\
            .select('member_name')\
            .eq('club_name', club_name)\
            .execute()
        return [r['member_name'] for r in response.data]
    
    def add_member(self, club_name: str, member_name: str):
        """Add a member to a club"""
        self.client.table('club_members')\
            .insert({'club_name': club_name, 'member_name': member_name})\
            .execute()
    
    def remove_member(self, club_name: str, member_name: str):
        """Remove a member from a club"""
        self.client.table('club_members')\
            .delete()\
            .eq('club_name', club_name)\
            .eq('member_name', member_name)\
            .execute()
    
    # ========================================================================
    # MEMBER STATS METHODS (Replaces Google Sheets data)
    # ========================================================================
    
    def get_member_stats(self, club_name: str, member_name: str = None, limit: int = 100) -> pd.DataFrame:
        """Get stats data (replaces fetching from Google Sheets)"""
        query = self.client.table('member_stats')\
            .select('*')\
            .eq('club_name', club_name)\
            .order('date', desc=True)\
            .limit(limit)
        
        if member_name:
            query = query.eq('member_name', member_name)
        
        response = query.execute()
        return pd.DataFrame(response.data)
    
    def get_latest_stats(self, club_name: str) -> pd.DataFrame:
        """Get most recent stats for all members (for leaderboard)"""
        # Get latest date first
        latest = self.client.table('member_stats')\
            .select('date')\
            .eq('club_name', club_name)\
            .order('date', desc=True)\
            .limit(1)\
            .execute()
        
        if not latest.data:
            return pd.DataFrame()
        
        latest_date = latest.data[0]['date']
        
        # Get all stats for that date
        response = self.client.table('member_stats')\
            .select('*')\
            .eq('club_name', club_name)\
            .eq('date', latest_date)\
            .order('rank')\
            .execute()
        
        return pd.DataFrame(response.data)
    
    def insert_stats(self, stats_data: List[Dict]):
        """Bulk upsert stats data - updates existing records, inserts new ones"""
        try:
            # Use upsert with explicit conflict resolution on composite key
            self.client.table('member_stats')\
                .upsert(
                    stats_data,
                    on_conflict='club_name,member_name,date'
                )\
                .execute()
        except Exception as e:
            print(f"❌ Failed to upsert stats: {e}")
            raise
    
    # ========================================================================
    # ROLE MANAGEMENT METHODS
    # ========================================================================
    
    def get_leaders(self, club_name: str) -> List[int]:
        """Get list of leader user IDs"""
        club = self.get_club_by_name(club_name)
        return club.get('leaders', []) if club else []
    
    def get_officers(self, club_name: str) -> List[int]:
        """Get list of officer user IDs"""
        club = self.get_club_by_name(club_name)
        return club.get('officers', []) if club else []
    
    def assign_leader(self, club_name: str, user_id: int):
        """Add user to leaders list"""
        leaders = self.get_leaders(club_name)
        if user_id not in leaders:
            leaders.append(user_id)
            self.update_club(club_name, {'leaders': leaders})
    
    def remove_leader(self, club_name: str, user_id: int):
        """Remove user from leaders list"""
        leaders = self.get_leaders(club_name)
        if user_id in leaders:
            leaders.remove(user_id)
            self.update_club(club_name, {'leaders': leaders})
    
    def assign_officer(self, club_name: str, user_id: int):
        """Add user to officers list"""
        officers = self.get_officers(club_name)
        if user_id not in officers:
            officers.append(user_id)
            self.update_club(club_name, {'officers': officers})
    
    def remove_officer(self, club_name: str, user_id: int):
        """Remove user from officers list"""
        officers = self.get_officers(club_name)
        if user_id in officers:
            officers.remove(user_id)
            self.update_club(club_name, {'officers': officers})
    
    # ========================================================================
    # QUOTA MANAGEMENT
    # ========================================================================
    
    def update_quota(self, club_name: str, quota: int):
        """Update club daily quota"""
        self.update_club(club_name, {'target_per_day': quota})
    
    def update_webhook(self, club_name: str, webhook_url: str):
        """Update club webhook URL"""
        self.update_club(club_name, {'webhook_url': webhook_url})
    
    def update_url(self, club_name: str, club_url: str):
        """Update club profile URL"""
        self.update_club(club_name, {'club_url': club_url})
