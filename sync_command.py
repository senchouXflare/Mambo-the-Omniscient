"""
Auto-Sync Module: Google Sheets to Supabase
Syncs all club stats data from Google Sheets to Supabase database
"""

import gspread
from datetime import datetime, timedelta
from typing import Dict
from supabase_manager import SupabaseManager


def sync_single_club(club_name: str, sh: gspread.Spreadsheet, db: SupabaseManager) -> Dict:
    """
    Sync a single club's data from Google Sheets to Supabase
    
    Args:
        club_name: Name of the club to sync
        sh: Google Sheets spreadsheet object
        db: Supabase database manager
    
    Returns:
        Dictionary with sync results: {
            'club_name': str,
            'status': 'success' | 'error',
            'rows_synced': int,
            'error': str (if error)
        }
    """
    try:
        # Get club config
        club = db.get_club_by_name(club_name)
        if not club:
            return {
                'club_name': club_name,
                'status': 'error',
                'rows_synced': 0,
                'error': 'Club not found in database'
            }
        
        data_sheet = club.get('data_sheet_name')
        if not data_sheet:
            return {
                'club_name': club_name,
                'status': 'error',
                'rows_synced': 0,
                'error': 'No data sheet configured'
            }
        
        # Read from Google Sheets
        ws = sh.worksheet(data_sheet)
        all_data = ws.get_all_values()
        
        if len(all_data) <= 1:
            return {
                'club_name': club_name,
                'status': 'success',
                'rows_synced': 0,
                'error': None
            }
        
        headers = all_data[0]
        
        # Find column indices
        try:
            name_idx = headers.index('Name')
            day_idx = headers.index('Day')
            fans_idx = headers.index('Total Fans')
            daily_idx = headers.index('Daily')
        except ValueError as e:
            return {
                'club_name': club_name,
                'status': 'error',
                'rows_synced': 0,
                'error': f'Missing column: {e}'
            }
        
        # Calculate date range
        today = datetime.now().date()
        max_day = max([int(row[day_idx]) for row in all_data[1:] if row[day_idx]])
        start_date = today - timedelta(days=max_day - 1)
        
        # Parse all rows
        stats_batch = []
        for row in all_data[1:]:
            try:
                member_name = row[name_idx].strip()
                day_num = int(row[day_idx])
                fans_count = int(row[fans_idx].replace(',', ''))
                fans_gain = int(float(row[daily_idx]))
                
                actual_date = start_date + timedelta(days=day_num - 1)
                
                stats_batch.append({
                    'club_name': club_name,
                    'member_name': member_name,
                    'date': actual_date.strftime('%Y-%m-%d'),
                    'fans_count': fans_count,
                    'fans_gain': fans_gain,
                    'rank': None
                })
            except:
                continue
        
        # Insert in batches
        if stats_batch:
            batch_size = 100
            for j in range(0, len(stats_batch), batch_size):
                batch = stats_batch[j:j+batch_size]
                db.insert_stats(batch)
            
            return {
                'club_name': club_name,
                'status': 'success',
                'rows_synced': len(stats_batch),
                'error': None
            }
        else:
            return {
                'club_name': club_name,
                'status': 'success',
                'rows_synced': 0,
                'error': None
            }
            
    except Exception as e:
        return {
            'club_name': club_name,
            'status': 'error',
            'rows_synced': 0,
            'error': str(e)
        }


def sync_all_clubs(sh: gspread.Spreadsheet, db: SupabaseManager) -> Dict:
    """
    Sync all clubs from Google Sheets to Supabase
    
    Args:
        sh: Google Sheets spreadsheet object
        db: Supabase database manager
    
    Returns:
        Dictionary with overall sync results: {
            'clubs_synced': int,
            'total_rows': int,
            'errors': list of error messages,
            'club_results': list of individual club results
        }
    """
    try:
        # Get all clubs
        clubs = db.get_all_clubs()
        
        results = {
            'clubs_synced': 0,
            'total_rows': 0,
            'errors': [],
            'club_results': []
        }
        
        for club in clubs:
            club_name = club['club_name']
            result = sync_single_club(club_name, sh, db)
            results['club_results'].append(result)
            
            if result['status'] == 'success':
                results['clubs_synced'] += 1
                results['total_rows'] += result['rows_synced']
            else:
                results['errors'].append(f"{club_name}: {result['error']}")
        
        return results
        
    except Exception as e:
        return {
            'clubs_synced': 0,
            'total_rows': 0,
            'errors': [f'Fatal error: {str(e)}'],
            'club_results': []
        }
