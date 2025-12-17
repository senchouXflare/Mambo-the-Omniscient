"""
Auto-Sync Helpers Module
Provides helper functions for fetching club data from uma.moe API
Used by bot.py for /search_club command and manual club sync
"""

import aiohttp
import asyncio
from typing import Dict, Optional, List


async def fetch_circle_data(circle_id: str, timeout: int = 15) -> Optional[Dict]:
    """
    Fetch circle (club) data from uma.moe API using circle_id
    
    Args:
        circle_id: The circle/club ID to fetch
        timeout: Request timeout in seconds
    
    Returns:
        Dict with circle data or None if not found
        
    Example response structure:
    {
        "circle_id": "12345",
        "name": "Club Name",
        "monthly_point": 1234567890,
        "monthly_rank": 50,
        "prev_monthly_point": 1000000000,
        "prev_monthly_rank": 60,
        "leader_id": "67890",
        "leader_name": "LeaderName",
        "join_style": 0,  # 0=Open, 1=Approval Required, 2=Closed
        "policy": 0,      # Club policy type
        "created_date": "2024-01-15",
        "members": [
            {"id": "12345", "name": "MemberName", ...},
            ...
        ]
    }
    """
    # Try different viewer_ids to get circle data
    # Using circle_id as viewer_id often works
    url = f"https://uma.moe/api/v4/circles?circle_id={circle_id}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # API returns list of circles, find matching one
                    if isinstance(data, list):
                        for circle in data:
                            if str(circle.get('circle_id')) == str(circle_id):
                                return circle
                        # If not found by ID, return first circle (user's club)
                        return data[0] if data else None
                    elif isinstance(data, dict):
                        return data
                    
                    return None
                elif response.status == 404:
                    return None
                else:
                    print(f"API Error: Status {response.status}")
                    return None
                    
    except aiohttp.ClientTimeout:
        print(f"Timeout fetching circle data for {circle_id}")
        return None
    except aiohttp.ClientError as e:
        print(f"Network error fetching circle data: {e}")
        return None
    except Exception as e:
        print(f"Error fetching circle data: {e}")
        return None


async def sync_club_from_api(
    club_name: str,
    circle_id: str,
    gs_manager,
    config_cache: dict
) -> Dict:
    """
    Sync club data from uma.moe API to Google Sheets
    
    This function fetches latest member list from API and updates
    the club's Members sheet in Google Sheets.
    
    Args:
        club_name: Name of the club to sync
        circle_id: The circle/club ID on uma.moe
        gs_manager: GoogleSheetsManager instance
        config_cache: Bot's config cache with club configurations
    
    Returns:
        Dict with sync results:
        {
            'success': bool,
            'message': str,
            'members_added': int,
            'members_removed': int
        }
    """
    try:
        # Get club config
        club_config = config_cache.get(club_name)
        if not club_config:
            return {
                'success': False,
                'message': f'Club {club_name} not found in config',
                'members_added': 0,
                'members_removed': 0
            }
        
        members_sheet = club_config.get('Members_Sheet_Name')
        if not members_sheet:
            return {
                'success': False,
                'message': 'Members sheet not configured',
                'members_added': 0,
                'members_removed': 0
            }
        
        # Fetch data from API
        api_data = await fetch_circle_data(circle_id)
        if not api_data:
            return {
                'success': False,
                'message': 'Failed to fetch data from API',
                'members_added': 0,
                'members_removed': 0
            }
        
        # Get members from API
        api_members = api_data.get('members', [])
        if not api_members:
            return {
                'success': False,
                'message': 'No members found in API response',
                'members_added': 0,
                'members_removed': 0
            }
        
        # Read current members from sheet
        members_ws = gs_manager.sh.worksheet(members_sheet)
        current_data = members_ws.get_all_values()
        
        # Build current member set (ID -> Name)
        current_members = {}
        if len(current_data) > 1:
            for row in current_data[1:]:  # Skip header
                if len(row) >= 2 and row[0].strip():
                    current_members[row[0].strip()] = row[1].strip() if len(row) > 1 else ""
        
        # Build API member set
        api_member_set = {}
        for member in api_members:
            member_id = str(member.get('id', ''))
            member_name = member.get('name', '')
            if member_id:
                api_member_set[member_id] = member_name
        
        # Find differences
        new_members = set(api_member_set.keys()) - set(current_members.keys())
        removed_members = set(current_members.keys()) - set(api_member_set.keys())
        
        members_added = 0
        members_removed = 0
        
        # Add new members
        if new_members:
            next_row = len(current_data) + 1
            for member_id in new_members:
                member_name = api_member_set[member_id]
                members_ws.update(f'A{next_row}:B{next_row}', [[member_id, member_name]])
                next_row += 1
                members_added += 1
        
        # Note: We don't auto-remove members, just report them
        members_removed = len(removed_members)
        
        return {
            'success': True,
            'message': f'Synced {members_added} new members, {members_removed} may have left',
            'members_added': members_added,
            'members_removed': members_removed
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f'Sync error: {str(e)}',
            'members_added': 0,
            'members_removed': 0
        }


# For backwards compatibility - sync function that can be called synchronously
def sync_club_from_api_sync(*args, **kwargs):
    """Synchronous wrapper for sync_club_from_api"""
    return asyncio.run(sync_club_from_api(*args, **kwargs))
