# 📋 Update Log - Bot.py

> **Cập nhật:** 04/12/2024

---

## � Smart Cache System
- Cache data với disk persistence, TTL 24 giờ
- Tự động load cache từ disk khi khởi động
- Invalidate cache khi data được update

## 🔗 Database Integration
- Kết nối **Supabase** làm backup database
- Hybrid wrapper: Google Sheets → Supabase fallback
- Retry logic với exponential backoff cho API calls

## 🌍 Global Leaderboard
- Xem ranking tất cả members từ mọi clubs
- Filter theo daily average (min/max)
- Pagination với 6 buttons: First, Prev, Next, Last, Filter, Clear Filter
- **Nút Return** để quay về club leaderboard

## 📋 Club List Command
- `/club_list` hiển thị **TẤT CẢ clubs** trong hệ thống
- Filter theo type (Competitive/Casual)
- Sort theo Name, Type, hoặc Quota
- Link support server với embedded markdown

## 🆘 Help Command
- Thêm section **"Need help? Join our support server"** với link
- Áp dụng cho cả User Commands và Manager Commands

## 🏆 Club Setup Modal
- Modal riêng cho **Competitive** (có quota) và **Casual** (không quota)
- Validate Club URL format
- Error handling với hướng dẫn tìm Club ID

## 📊 Leaderboard Display
- Đổi tên cột **'Carry'** → **'Surplus'**
- Format khác nhau cho Casual vs Competitive clubs
- Căn chỉnh cột với monospace font

## 🔐 Permission System
- `is_admin_or_has_role()` - Admin/Role check
- `is_leader_or_admin()` - Leader + Server Admin + God Mode
- `is_god_mode_only()` - Chỉ God Mode users
- Administrators có quyền dùng channel commands

## � Multi-Channel Support
- Hỗ trợ nhiều allowed channels per server
- Migration tự động từ single-channel config cũ
- Log history các thay đổi channel

## 📝 Command Logging
- Log **TẤT CẢ** commands bao gồm failed attempts
- Gửi log đến logging channel với thông tin chi tiết

## ⏰ Scheduled Tasks
- `auto_sync_to_supabase()` chạy lúc 0h UTC mỗi ngày
- `warm_cache` task để pre-load cache
- `cleanup_expired_requests` dọn pending requests

---

## 🐛 Bugs Fixed
- **Stale Data** - Cache không refresh đúng cách
- **Discord Heartbeat Blocking** - Bot bị block >200s
- **Google Sheets Quota 429** - Rate limit errors
- **Multiple Error Messages** - Duplicate permission denied messages
- **Command Registration** - Duplicate command errors

---

## ❌ Removed
- `/add_channel` command (replaced by `/set_channel`)
- `/list_channels` command (replaced by permanent message)
- Duplicate command definitions
