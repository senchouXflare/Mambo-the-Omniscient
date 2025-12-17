"""
Intelligent Hybrid Database Wrapper
Tries Google Sheets first, auto-switches to Supabase on timeout/errors
"""

import time
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import pandas as pd

class HybridDatabaseManager:
    """
    Smart database manager with automatic failover
    
    Strategy:
    - Primary: Google Sheets (for compatibility)
    - Fallback: Supabase (on timeout/errors)
    - Auto-retry: Google Sheets every 5 minutes
    """
    
    def __init__(self, gs_manager, supabase_db):
        self.gs_manager = gs_manager
        self.supabase_db = supabase_db
        
        # Failover state
        self.sheets_available = True
        self.last_sheets_failure = None
        self.retry_interval = 300  # 5 minutes
        
        # Timeout settings
        self.sheets_timeout = 3.0  # 3 seconds max for Sheets queries
        self.startup_timeout = 5.0  # 5 seconds max for startup connection
        
        print("🔄 Hybrid Database Manager initialized")
        print(f"   Primary: Google Sheets (timeout: {self.sheets_timeout}s)")
        print(f"   Fallback: Supabase")
    
    def _should_retry_sheets(self) -> bool:
        """Check if we should retry Google Sheets connection"""
        if self.sheets_available:
            return False
        
        if self.last_sheets_failure is None:
            return True
        
        elapsed = (datetime.now() - self.last_sheets_failure).total_seconds()
        return elapsed >= self.retry_interval
    
    def _mark_sheets_failure(self):
        """Mark Google Sheets as unavailable"""
        self.sheets_available = False
        self.last_sheets_failure = datetime.now()
        print(f"⚠️  Google Sheets marked unavailable (will retry in {self.retry_interval}s)")
    
    def _mark_sheets_success(self):
        """Mark Google Sheets as available"""
        if not self.sheets_available:
            print("✅ Google Sheets connection restored")
        self.sheets_available = True
        self.last_sheets_failure = None
    
    async def get_data_with_timeout(self, func, *args, timeout=None, **kwargs):
        """
        Execute function with timeout
        Auto-switches to Supabase on timeout/error
        """
        if timeout is None:
            timeout = self.sheets_timeout
        
        try:
            # Try with timeout
            result = await asyncio.wait_for(
                asyncio.to_thread(func, *args, **kwargs),
                timeout=timeout
            )
            self._mark_sheets_success()
            return result, 'sheets'
            
        except asyncio.TimeoutError:
            print(f"⏱️  Timeout after {timeout}s - switching to Supabase")
            self._mark_sheets_failure()
            return None, 'timeout'
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Check for rate limit errors
            if 'quota' in error_msg or 'rate limit' in error_msg or '429' in error_msg:
                print(f"🚫 Rate limit hit - switching to Supabase")
                self._mark_sheets_failure()
                return None, 'rate_limit'
            
            # Check for auth errors
            elif 'auth' in error_msg or '401' in error_msg or '403' in error_msg:
                print(f"🔒 Auth error - switching to Supabase")
                self._mark_sheets_failure()
                return None, 'auth_error'
            
            # Other errors
            else:
                print(f"❌ Error: {str(e)[:100]} - switching to Supabase")
                self._mark_sheets_failure()
                return None, 'error'
    
    def get_club_config(self, club_name: str) -> Optional[Dict]:
        """Get club config - always from Supabase (fast)"""
        try:
            return self.supabase_db.get_club_by_name(club_name)
        except:
            # Fallback to cache if Supabase fails
            return None
    
    def get_all_clubs(self) -> List[Dict]:
        """Get all clubs - always from Supabase (fast)"""
        try:
            return self.supabase_db.get_all_clubs()
        except:
            return []
    
    def get_club_members(self, club_name: str) -> List[str]:
        """Get members - always from Supabase (fast)"""
        try:
            return self.supabase_db.get_club_members(club_name)
        except:
            return []
    
    async def get_stats_data(self, club_name: str, use_supabase: bool = False) -> pd.DataFrame:
        """
        Get stats data with intelligent failover
        
        Strategy:
        1. Try Google Sheets first (unless forced to Supabase)
        2. Switch to Supabase on timeout/error
        3. Return best available data
        """
        # Check if we should retry Sheets
        if not use_supabase and (self.sheets_available or self._should_retry_sheets()):
            # Try Google Sheets
            try:
                def fetch_from_sheets():
                    # Your existing Google Sheets fetch logic here
                    # This is a placeholder - replace with actual sheet read
                    ws = self.gs_manager.sh.worksheet(f"{club_name}_Data")
                    return pd.DataFrame(ws.get_all_records())
                
                result, source = await self.get_data_with_timeout(fetch_from_sheets)
                
                if result is not None:
                    return result  # Success from Sheets
                
            except Exception as e:
                print(f"Sheets error: {e}")
        
        # Fallback to Supabase
        try:
            print(f"📊 Fetching from Supabase: {club_name}")
            stats_df = self.supabase_db.get_latest_stats(club_name)
            return stats_df
        except Exception as e:
            print(f"Supabase error: {e}")
            return pd.DataFrame()


# Global instance
hybrid_db = None

def initialize_hybrid_db(gs_manager, supabase_db):
    """Initialize global hybrid database manager"""
    global hybrid_db
    hybrid_db = HybridDatabaseManager(gs_manager, supabase_db)
    return hybrid_db
