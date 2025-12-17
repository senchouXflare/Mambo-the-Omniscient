# data_updater.py - Simplified version for Fan-Count bot
# NOTE: This module is imported by bot.py but most functions are not actively used
# Keeping minimal functions for potential future use or indirect dependencies

import os
import json
import time

# --- SCRIPT & CACHE PATHS ---
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "data_cache")
CONFIG_CACHE_FILE = os.path.join(CACHE_DIR, "config_cache.json")
MEMBER_CACHE_FILE = os.path.join(CACHE_DIR, "member_cache.json")
DATA_CACHE_DIR = os.path.join(CACHE_DIR, "data") 

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DATA_CACHE_DIR, exist_ok=True)


def is_retryable_error(e: Exception) -> bool:
    """
    Kiểm tra xem một lỗi có phải là lỗi mạng (có thể retry) không.
    """
    error_str = str(e).lower() 
    return "remotedisconnected" in error_str or \
           "connection aborted" in error_str or \
           "service unavailable" in error_str or \
           "429" in error_str


def clean_fan_total(value) -> int:
    """Clean and convert fan total value to integer"""
    val_str = str(value).strip().lower()
    if val_str == 'nan' or val_str == 'none' or val_str == '':
        return 0 
    try:
        cleaned_val = val_str.replace(',', '')
        return int(float(cleaned_val)) 
    except ValueError:
        return 0


# NOTE: The following functions were removed as they are not used by bot.py:
# - get_club_config() - bot.py uses its own cache system
# - load_member_map() - bot.py uses ClubManagementBot.member_cache
# - build_master_member_map_REVISED() - not used
# - run_smart_update_logic() - not used
# - recalculate_targets_for_member() - not used (bot reads pre-calculated data)
# - _write_data_cache() - not used by bot.py
# - run_update_logic() - not used by bot.py
# - run_member_replacement() - not used by bot.py
# - archive_club_data() - not used by bot.py
# - list_all_archives() - not used by bot.py
# - create_archive_sheet_name() - not used by bot.py
# - archive_exists() - not used by bot.py
#
# If any of these functions are needed in the future, refer to:
# c:\Users\GIGABYTE\Retard Project\Chronoscrape\Test\data_updater.py
