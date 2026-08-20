#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║          🎬  NEXUS DOWNLOADER BOT  — v7.0                      ║
║      python-telegram-bot v21+ | yt-dlp | Fully Async           ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL (Termux / Pydroid 3 / Render / justrunmy.app / wispbyte / VPS):
  pkg update && pkg upgrade -y
  pkg install -y python ffmpeg nodejs
  pip install "python-telegram-bot[job-queue]" yt-dlp aiofiles curl_cffi aiohttp

UPDATE yt-dlp before running (image/slideshow support needs a recent build):
  pip install -U yt-dlp

REQUIRED ENV VARS:
  BOT_TOKEN   -> your token from @BotFather
  ADMIN_ID    -> your numeric Telegram user ID

OPTIONAL — Fix YouTube/Instagram "login required":
  Export cookies.txt from your browser (logged-in) and place it next to
  this script as "cookies.txt".  The bot will use it automatically.
  Recommended extension: "Get cookies.txt LOCALLY" (Chrome/Firefox).

NEW IN v6.0:
  ✅ Admin "User List" now shows username, numeric ID, message count,
       last-seen date/time, and the title of their most recent download
       (data stored in nexus_user_info.json)
  ✅ Multi-language support — /language command + 🌐 Language menu button.
       21 languages in the picker (English, Русский, Українська, Español,
       O'zbek, Português, Deutsch, Italiano, Français, Türkçe, עברית,
       العربية, فارسی, 中文, Bahasa Indonesia, Svenska, Melayu, Nederlands,
       हिंदी, 한국인, Tiếng Việt). Core menus/messages are translated;
       anything not yet translated for a language falls back to English
       automatically, so nothing ever breaks.
  ✅ Image / slideshow downloader — detects TikTok photo posts (and any
       other image-only post yt-dlp can read) and sends them to Telegram
       as a photo album instead of trying to treat them as video.
  ✅ SECURITY FIX: BOT_TOKEN / ADMIN_ID now read from environment
       variables instead of being hardcoded in the script.

KEPT FROM v5.1 / v4.x (nothing removed):
  ✅ Admin menu buttons no longer trigger YouTube search
  ✅ Admin text handler only intercepts when in broadcast/ban mode
  ✅ YouTube & Instagram login-wall fix — auto-uses cookies.txt if present
  ✅ YouTube fallback: android_embedded + web_embedded extractor
  ✅ Instagram fallback: mobile user-agent on login errors
  ✅ Admin Dashboard — Statistics, User List, Clear Cache,
       Broadcast, Ban User, Unban User
  ✅ Ban/Unban — banned users get a polite rejection message
  ✅ Settings — 📋 Results per search: 10 / 20 / 50 / 100
  ✅ Facebook fix — mibextid strip + mbasic fallback
  ✅ TikTok fix — api22 + curl_cffi Chrome TLS fingerprint
  ✅ YouTube Playlist support, progress counter, stop button
  ✅ Retry button on failed downloads
  ✅ /stats — personal download counter
  ✅ Pre-download size check, per-user cooldown
  ✅ Compatible with Termux, Pydroid 3, Render, justrunmy.app,
       wispbyte, and all hosting platforms / VPS
"""

import asyncio
import io
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import traceback
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
import aiohttp
import yt_dlp
from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    InputFile, InputMediaPhoto, Message, ReplyKeyboardMarkup, Update,
)
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, NetworkError, TelegramError, TimedOut
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

# ──────────────────────────────────────────────────────────────────
#  ★  CONFIGURATION — EDIT VIA ENVIRONMENT VARIABLES  ★
# ──────────────────────────────────────────────────────────────────
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
ADMIN_ID:  int = int(os.environ.get("ADMIN_ID", "0"))

# NEW in v6.8: OPTIONAL — point at a self-hosted Telegram Bot API server
# (https://github.com/tdlib/telegram-bot-api) to raise the send limit from
# Telegram's default 50MB up to 2000MB, avoiding the fallback-link path
# entirely for large files. Leave unset to keep using the standard cloud
# API (api.telegram.org) exactly as before — nothing changes if you don't
# set this. If it's set but unreachable at startup, the bot automatically
# falls back to the cloud API instead of failing to start.
#   LOCAL_BOT_API_URL=http://localhost:8081
LOCAL_BOT_API_URL: str = os.environ.get("LOCAL_BOT_API_URL", "").rstrip("/")

if not BOT_TOKEN:
    raise SystemExit(
        "BOT_TOKEN is not set. Add it in your host's Environment/.env "
        "settings (key: BOT_TOKEN, value: your token from @BotFather)."
    )
if not ADMIN_ID:
    logging.getLogger(__name__).warning(
        "ADMIN_ID is not set — admin notifications will be skipped. "
        "Set ADMIN_ID in your .env to your numeric Telegram user ID."
    )

MAX_CONCURRENT_PER_USER: int  = 2
MAX_FILE_SIZE_MB: int         = 2000
TELEGRAM_SEND_LIMIT_MB: int   = 50     # hard cap for bot.send_video/send_audio (Bot API default)
REHOST_RETRIES: int           = 2      # retries per host before moving to the next one
REHOST_TIMEOUT_SECS: int      = 300
# NEW in v6.3: multiple free re-host targets tried in order. Free anonymous
# hosts like these are flaky by nature (rate limits, downtime, carrier-IP
# blocks) — having more than one means a single host being down doesn't
# take out the whole fallback-link feature.
REHOST_HOSTS: list = ["0x0", "tmpfiles"]
PROGRESS_THROTTLE_SECS: int   = 3
SEARCH_RESULTS_COUNT: int     = 10   # default; overridden per-user in Settings
SEARCH_PAGE_SIZE: int         = 5
PLAYLIST_MAX_ITEMS: int       = 25
DOWNLOAD_COOLDOWN_SECS: float = 3.0
MAX_ALBUM_IMAGES: int         = 30   # safety cap on how many images we'll pull from one post
DOWNLOAD_DIR: Path            = Path(tempfile.gettempdir()) / "nexus_bot_dl"
USER_DB_FILE: Path            = Path("nexus_users.json")
STATS_FILE: Path              = Path("nexus_stats.json")
USER_INFO_FILE: Path          = Path("nexus_user_info.json")
COOKIES_FILE: Path            = Path("cookies.txt")   # optional — place next to script

SEARCH_COUNT_OPTIONS: list = [10, 20, 50, 100]

# ──────────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("nexus_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("NexusBot")
logging.getLogger("httpx").setLevel(logging.WARNING)

# ──────────────────────────────────────────────────────────────────
#  EMOJI CONSTANTS
# ──────────────────────────────────────────────────────────────────
E = {
    "film":     "🎬", "music":    "🎵", "dl":       "⬇️",
    "check":    "✅", "cross":    "❌", "warn":     "⚠️",
    "search":   "🔍", "gear":     "⚙️", "admin":    "👑",
    "user":     "👤", "queue":    "📋", "speed":    "⚡",
    "size":     "💾", "time":     "⏱️", "sub":      "💬",
    "thumb":    "🖼️", "caption":  "📝", "cancel":   "🚫",
    "link":     "🔗", "fire":     "🔥", "rocket":   "🚀",
    "wave":     "👋", "info":     "ℹ️", "globe":    "🌐",
    "bell":     "🔔", "on":       "🟢", "off":      "🔴",
    "next":     "▶️", "prev":     "◀️", "playlist": "📀",
    "retry":    "🔁", "stats":    "📊", "star":     "⭐",
    "video":    "🎥", "moon":     "🌙", "trash":    "🗑️",
}
FILLED  = "🟩"
EMPTY   = "⬜"
BAR_LEN = 10

# ──────────────────────────────────────────────────────────────────
#  QUALITY PRESETS
# ──────────────────────────────────────────────────────────────────
# NEW in v7.0: simplified to match a proven-working reference implementation
# (bestvideo[height<=X]+bestaudio/best[height<=X]) rather than the more
# elaborate ext-filtered, multi-tier fallback chain used before — the extra
# [ext=mp4]/[ext=m4a] restrictions could themselves reject perfectly good
# higher-resolution webm-only tracks and force a fallback all the way down
# to a lowest-common-denominator result.
QUALITY_PRESETS: Dict[str, dict] = {
    "1080p": {"label": "🎥 1080p (Full HD)",  "fmt": "bestvideo[height<=1080]+bestaudio/best[height<=1080]"},
    "720p":  {"label": "📺 720p (HD)",         "fmt": "bestvideo[height<=720]+bestaudio/best[height<=720]"},
    "480p":  {"label": "📱 480p (SD)",         "fmt": "bestvideo[height<=480]+bestaudio/best[height<=480]"},
    "360p":  {"label": "🔲 360p (Low)",        "fmt": "bestvideo[height<=360]+bestaudio/best[height<=360]"},
    "best":  {"label": "⚡ Best Available",    "fmt": "best/bestvideo+bestaudio"},
    "audio": {"label": "🎵 Audio Only (MP3)",  "fmt": "bestaudio/best"},
}

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

TIKTOK_ARGS = {
    "tiktok": {
        "api_hostname": ["api22-normal-c-useast2a.tiktokv.com"],
        "app_version":  ["39.4.6"],
    }
}

# NEW in v6.1: many hosts (e.g. Replit) don't have a JS runtime (deno/node)
# installed, which YouTube's "web" client needs to decrypt signature-protected
# formats. android/ios clients don't need JS for that, so try them first —
# this avoids the "stuck re-extracting forever" symptom on JS-less hosts.
# "web" is kept last so higher-res formats are still tried when JS IS available.
# REVERTED in v7.0: forcing a specific YouTube player_client order (tried in
# v6.1 to dodge a JS-runtime hang, then flipped in v6.4 chasing a different
# bug) turned out to be actively harmful — it was silently collapsing the
# format list down to a single quality tier regardless of which preset was
# requested. Confirmed by side-by-side testing: a minimal reference bot that
# sets NO extractor_args at all reliably produced different file sizes per
# quality (2.2MB@360p vs 5.4MB@1080p) on a video where this bot returned the
# exact same 360p/2.2MB file for every quality button. Letting yt-dlp use its
# own default client-selection logic is what actually works — so we stop
# overriding it. TikTok's args are unaffected and still applied below.
EXTRACTOR_ARGS = {**TIKTOK_ARGS}

# NEW in v6.1: hard ceiling on how long a single download attempt can run
# before we give up and show a retry button, instead of silently hanging.
# Raise this if you host somewhere slow, or for very long videos.
DOWNLOAD_TIMEOUT_SECS: int = 900   # 15 minutes
HEARTBEAT_SECS: int         = 20   # how often to reassure the user it's alive

FACEBOOK_JUNK_PARAMS = {
    "mibextid", "si", "ref", "fref", "sfnsn", "extid",
    "__cft__", "__tn__", "hrc", "_rdr", "cached_data",
}

# ──────────────────────────────────────────────────────────────────
#  MULTI-LANGUAGE SUPPORT
# ──────────────────────────────────────────────────────────────────
# (code, flag + native name shown in the picker)
LANGUAGES: List[tuple] = [
    ("en", "🇬🇧 English"),
    ("ru", "🇷🇺 Русский"),
    ("uk", "🇺🇦 Українська"),
    ("es", "🇪🇸 Español"),
    ("uz", "🇺🇿 O'zbek"),
    ("pt", "🇧🇷 Português"),
    ("de", "🇩🇪 Deutsch"),
    ("it", "🇮🇹 Italiano"),
    ("fr", "🇫🇷 Français"),
    ("tr", "🇹🇷 Türkçe"),
    ("he", "🇮🇱 עברית"),
    ("ar", "🇸🇦 العربية"),
    ("fa", "🇮🇷 فارسی"),
    ("zh", "🇨🇳 中國人"),
    ("id", "🇮🇩 Bahasa Indonesia"),
    ("sv", "🇸🇪 Svenska"),
    ("ms", "🇲🇾 Melayu"),
    ("nl", "🇳🇱 Nederlands"),
    ("hi", "🇮🇳 हिंदी"),
    ("ko", "🇰🇷 한국인"),
    ("vi", "🇻🇳 Tiếng Việt"),
]
LANGUAGE_LABELS: Dict[str, str] = dict(LANGUAGES)

# Core, high-visibility strings are translated. Anything missing for a
# given language automatically falls back to English via t() below, so
# adding more languages/keys later never breaks the bot.
TRANSLATIONS: Dict[str, Dict[str, str]] = {
    "en": {
        "menu_download": "⬇️ Download Link", "menu_search": "🔍 Search YouTube",
        "menu_settings": "⚙️ Settings", "menu_help": "ℹ️ Help",
        "menu_stats": "📊 My Stats", "menu_admin": "👑 Admin Dashboard",
        "menu_language": "🌐 Language",
        "welcome_new": "Hello, <b>{name}</b>!", "welcome_back": "Welcome back, <b>{name}</b>!",
        "intro": ("<b>NEXUS Downloader Bot v6.0</b>\n\n"
                  "Send a link from YouTube, Shorts, TikTok, Instagram, Pinterest, "
                  "Twitter/X, Facebook & 1000+ sites.\n\n"
                  "📀 YouTube playlists supported — send a playlist link!\n"
                  "🖼️ TikTok photo posts supported — sent as an album!\n\n"
                  "Or type a search query to find YouTube videos."),
        "choose_language": "🌐 <b>Choose your language:</b>",
        "language_set": "✅ Language set to {lang}.",
        "send_url_prompt": "🔗 Send me a URL to download.",
        "send_search_prompt": "🔍 Type your YouTube search query:",
    },
    "ru": {
        "menu_download": "⬇️ Скачать по ссылке", "menu_search": "🔍 Поиск на YouTube",
        "menu_settings": "⚙️ Настройки", "menu_help": "ℹ️ Помощь",
        "menu_stats": "📊 Моя статистика", "menu_admin": "👑 Панель админа",
        "menu_language": "🌐 Язык",
        "welcome_new": "Привет, <b>{name}</b>!", "welcome_back": "С возвращением, <b>{name}</b>!",
        "choose_language": "🌐 <b>Выберите язык:</b>",
        "language_set": "✅ Язык изменён на {lang}.",
        "send_url_prompt": "🔗 Отправьте ссылку для скачивания.",
        "send_search_prompt": "🔍 Введите запрос для поиска на YouTube:",
    },
    "uk": {
        "menu_download": "⬇️ Завантажити за посиланням", "menu_search": "🔍 Пошук на YouTube",
        "menu_settings": "⚙️ Налаштування", "menu_help": "ℹ️ Довідка",
        "menu_stats": "📊 Моя статистика", "menu_admin": "👑 Панель адміністратора",
        "menu_language": "🌐 Мова",
        "welcome_new": "Привіт, <b>{name}</b>!", "welcome_back": "З поверненням, <b>{name}</b>!",
        "choose_language": "🌐 <b>Оберіть мову:</b>",
        "language_set": "✅ Мову змінено на {lang}.",
        "send_url_prompt": "🔗 Надішліть посилання для завантаження.",
        "send_search_prompt": "🔍 Введіть пошуковий запит для YouTube:",
    },
    "es": {
        "menu_download": "⬇️ Descargar enlace", "menu_search": "🔍 Buscar en YouTube",
        "menu_settings": "⚙️ Ajustes", "menu_help": "ℹ️ Ayuda",
        "menu_stats": "📊 Mis estadísticas", "menu_admin": "👑 Panel de administrador",
        "menu_language": "🌐 Idioma",
        "welcome_new": "¡Hola, <b>{name}</b>!", "welcome_back": "¡Bienvenido de nuevo, <b>{name}</b>!",
        "choose_language": "🌐 <b>Elige tu idioma:</b>",
        "language_set": "✅ Idioma cambiado a {lang}.",
        "send_url_prompt": "🔗 Envíame un enlace para descargar.",
        "send_search_prompt": "🔍 Escribe tu búsqueda de YouTube:",
    },
    "uz": {
        "menu_download": "⬇️ Havoladan yuklab olish", "menu_search": "🔍 YouTube qidiruvi",
        "menu_settings": "⚙️ Sozlamalar", "menu_help": "ℹ️ Yordam",
        "menu_stats": "📊 Statistikam", "menu_admin": "👑 Admin panel",
        "menu_language": "🌐 Til",
        "welcome_new": "Salom, <b>{name}</b>!", "welcome_back": "Qaytganingiz bilan, <b>{name}</b>!",
        "choose_language": "🌐 <b>Tilni tanlang:</b>",
        "language_set": "✅ Til {lang} qilib o'zgartirildi.",
        "send_url_prompt": "🔗 Yuklab olish uchun havola yuboring.",
        "send_search_prompt": "🔍 YouTube qidiruv so'zini yozing:",
    },
    "pt": {
        "menu_download": "⬇️ Baixar link", "menu_search": "🔍 Buscar no YouTube",
        "menu_settings": "⚙️ Configurações", "menu_help": "ℹ️ Ajuda",
        "menu_stats": "📊 Minhas estatísticas", "menu_admin": "👑 Painel do admin",
        "menu_language": "🌐 Idioma",
        "welcome_new": "Olá, <b>{name}</b>!", "welcome_back": "Bem-vindo de volta, <b>{name}</b>!",
        "choose_language": "🌐 <b>Escolha seu idioma:</b>",
        "language_set": "✅ Idioma alterado para {lang}.",
        "send_url_prompt": "🔗 Envie um link para baixar.",
        "send_search_prompt": "🔍 Digite sua busca no YouTube:",
    },
    "de": {
        "menu_download": "⬇️ Link herunterladen", "menu_search": "🔍 YouTube durchsuchen",
        "menu_settings": "⚙️ Einstellungen", "menu_help": "ℹ️ Hilfe",
        "menu_stats": "📊 Meine Statistik", "menu_admin": "👑 Admin-Bereich",
        "menu_language": "🌐 Sprache",
        "welcome_new": "Hallo, <b>{name}</b>!", "welcome_back": "Willkommen zurück, <b>{name}</b>!",
        "choose_language": "🌐 <b>Wähle deine Sprache:</b>",
        "language_set": "✅ Sprache geändert zu {lang}.",
        "send_url_prompt": "🔗 Sende mir einen Link zum Herunterladen.",
        "send_search_prompt": "🔍 Gib deinen YouTube-Suchbegriff ein:",
    },
    "it": {
        "menu_download": "⬇️ Scarica link", "menu_search": "🔍 Cerca su YouTube",
        "menu_settings": "⚙️ Impostazioni", "menu_help": "ℹ️ Aiuto",
        "menu_stats": "📊 Le mie statistiche", "menu_admin": "👑 Pannello admin",
        "menu_language": "🌐 Lingua",
        "welcome_new": "Ciao, <b>{name}</b>!", "welcome_back": "Bentornato, <b>{name}</b>!",
        "choose_language": "🌐 <b>Scegli la tua lingua:</b>",
        "language_set": "✅ Lingua impostata su {lang}.",
        "send_url_prompt": "🔗 Inviami un link da scaricare.",
        "send_search_prompt": "🔍 Scrivi la tua ricerca YouTube:",
    },
    "fr": {
        "menu_download": "⬇️ Télécharger un lien", "menu_search": "🔍 Rechercher sur YouTube",
        "menu_settings": "⚙️ Paramètres", "menu_help": "ℹ️ Aide",
        "menu_stats": "📊 Mes statistiques", "menu_admin": "👑 Panneau admin",
        "menu_language": "🌐 Langue",
        "welcome_new": "Bonjour, <b>{name}</b> !", "welcome_back": "Content de te revoir, <b>{name}</b> !",
        "choose_language": "🌐 <b>Choisissez votre langue :</b>",
        "language_set": "✅ Langue changée en {lang}.",
        "send_url_prompt": "🔗 Envoie-moi un lien à télécharger.",
        "send_search_prompt": "🔍 Tape ta recherche YouTube :",
    },
    "tr": {
        "menu_download": "⬇️ Bağlantı İndir", "menu_search": "🔍 YouTube'da Ara",
        "menu_settings": "⚙️ Ayarlar", "menu_help": "ℹ️ Yardım",
        "menu_stats": "📊 İstatistiklerim", "menu_admin": "👑 Yönetici Paneli",
        "menu_language": "🌐 Dil",
        "welcome_new": "Merhaba, <b>{name}</b>!", "welcome_back": "Tekrar hoş geldin, <b>{name}</b>!",
        "choose_language": "🌐 <b>Dilinizi seçin:</b>",
        "language_set": "✅ Dil {lang} olarak ayarlandı.",
        "send_url_prompt": "🔗 İndirmek için bir bağlantı gönder.",
        "send_search_prompt": "🔍 YouTube arama sorgunu yaz:",
    },
    "he": {
        "menu_download": "⬇️ הורדה מקישור", "menu_search": "🔍 חיפוש ביוטיוב",
        "menu_settings": "⚙️ הגדרות", "menu_help": "ℹ️ עזרה",
        "menu_stats": "📊 הסטטיסטיקה שלי", "menu_admin": "👑 פאנל ניהול",
        "menu_language": "🌐 שפה",
        "welcome_new": "שלום, <b>{name}</b>!", "welcome_back": "ברוך שובך, <b>{name}</b>!",
        "choose_language": "🌐 <b>בחר/י שפה:</b>",
        "language_set": "✅ השפה שונתה ל-{lang}.",
        "send_url_prompt": "🔗 שלח/י לי קישור להורדה.",
        "send_search_prompt": "🔍 הקלד/י מונח חיפוש ביוטיוב:",
    },
    "ar": {
        "menu_download": "⬇️ تحميل رابط", "menu_search": "🔍 بحث في يوتيوب",
        "menu_settings": "⚙️ الإعدادات", "menu_help": "ℹ️ مساعدة",
        "menu_stats": "📊 إحصائياتي", "menu_admin": "👑 لوحة الإدارة",
        "menu_language": "🌐 اللغة",
        "welcome_new": "مرحباً، <b>{name}</b>!", "welcome_back": "مرحباً بعودتك، <b>{name}</b>!",
        "choose_language": "🌐 <b>اختر لغتك:</b>",
        "language_set": "✅ تم تغيير اللغة إلى {lang}.",
        "send_url_prompt": "🔗 أرسل لي رابطاً للتحميل.",
        "send_search_prompt": "🔍 اكتب كلمة البحث في يوتيوب:",
    },
    "fa": {
        "menu_download": "⬇️ دانلود از لینک", "menu_search": "🔍 جستجو در یوتیوب",
        "menu_settings": "⚙️ تنظیمات", "menu_help": "ℹ️ راهنما",
        "menu_stats": "📊 آمار من", "menu_admin": "👑 پنل مدیریت",
        "menu_language": "🌐 زبان",
        "welcome_new": "سلام، <b>{name}</b>!", "welcome_back": "خوش برگشتی، <b>{name}</b>!",
        "choose_language": "🌐 <b>زبان خود را انتخاب کنید:</b>",
        "language_set": "✅ زبان به {lang} تغییر کرد.",
        "send_url_prompt": "🔗 لینک را برای دانلود بفرست.",
        "send_search_prompt": "🔍 عبارت جستجوی یوتیوب را بنویس:",
    },
    "zh": {
        "menu_download": "⬇️ 链接下载", "menu_search": "🔍 搜索YouTube",
        "menu_settings": "⚙️ 设置", "menu_help": "ℹ️ 帮助",
        "menu_stats": "📊 我的统计", "menu_admin": "👑 管理面板",
        "menu_language": "🌐 语言",
        "welcome_new": "你好，<b>{name}</b>！", "welcome_back": "欢迎回来，<b>{name}</b>！",
        "choose_language": "🌐 <b>选择你的语言：</b>",
        "language_set": "✅ 语言已设置为 {lang}。",
        "send_url_prompt": "🔗 发送要下载的链接。",
        "send_search_prompt": "🔍 输入YouTube搜索内容：",
    },
    "id": {
        "menu_download": "⬇️ Unduh Tautan", "menu_search": "🔍 Cari di YouTube",
        "menu_settings": "⚙️ Pengaturan", "menu_help": "ℹ️ Bantuan",
        "menu_stats": "📊 Statistik Saya", "menu_admin": "👑 Panel Admin",
        "menu_language": "🌐 Bahasa",
        "welcome_new": "Halo, <b>{name}</b>!", "welcome_back": "Selamat datang kembali, <b>{name}</b>!",
        "choose_language": "🌐 <b>Pilih bahasamu:</b>",
        "language_set": "✅ Bahasa diubah ke {lang}.",
        "send_url_prompt": "🔗 Kirim tautan untuk diunduh.",
        "send_search_prompt": "🔍 Ketik kata pencarian YouTube:",
    },
    "sv": {
        "menu_download": "⬇️ Ladda ner länk", "menu_search": "🔍 Sök på YouTube",
        "menu_settings": "⚙️ Inställningar", "menu_help": "ℹ️ Hjälp",
        "menu_stats": "📊 Min statistik", "menu_admin": "👑 Adminpanel",
        "menu_language": "🌐 Språk",
        "welcome_new": "Hej, <b>{name}</b>!", "welcome_back": "Välkommen tillbaka, <b>{name}</b>!",
        "choose_language": "🌐 <b>Välj ditt språk:</b>",
        "language_set": "✅ Språk ändrat till {lang}.",
        "send_url_prompt": "🔗 Skicka en länk att ladda ner.",
        "send_search_prompt": "🔍 Skriv din YouTube-sökning:",
    },
    "ms": {
        "menu_download": "⬇️ Muat Turun Pautan", "menu_search": "🔍 Cari di YouTube",
        "menu_settings": "⚙️ Tetapan", "menu_help": "ℹ️ Bantuan",
        "menu_stats": "📊 Statistik Saya", "menu_admin": "👑 Panel Admin",
        "menu_language": "🌐 Bahasa",
        "welcome_new": "Hai, <b>{name}</b>!", "welcome_back": "Selamat kembali, <b>{name}</b>!",
        "choose_language": "🌐 <b>Pilih bahasa anda:</b>",
        "language_set": "✅ Bahasa ditukar kepada {lang}.",
        "send_url_prompt": "🔗 Hantar pautan untuk dimuat turun.",
        "send_search_prompt": "🔍 Taip carian YouTube anda:",
    },
    "nl": {
        "menu_download": "⬇️ Link downloaden", "menu_search": "🔍 Zoeken op YouTube",
        "menu_settings": "⚙️ Instellingen", "menu_help": "ℹ️ Help",
        "menu_stats": "📊 Mijn statistieken", "menu_admin": "👑 Adminpaneel",
        "menu_language": "🌐 Taal",
        "welcome_new": "Hallo, <b>{name}</b>!", "welcome_back": "Welkom terug, <b>{name}</b>!",
        "choose_language": "🌐 <b>Kies je taal:</b>",
        "language_set": "✅ Taal gewijzigd naar {lang}.",
        "send_url_prompt": "🔗 Stuur me een link om te downloaden.",
        "send_search_prompt": "🔍 Typ je YouTube-zoekopdracht:",
    },
    "hi": {
        "menu_download": "⬇️ लिंक डाउनलोड करें", "menu_search": "🔍 YouTube खोजें",
        "menu_settings": "⚙️ सेटिंग्स", "menu_help": "ℹ️ मदद",
        "menu_stats": "📊 मेरे आँकड़े", "menu_admin": "👑 एडमिन पैनल",
        "menu_language": "🌐 भाषा",
        "welcome_new": "नमस्ते, <b>{name}</b>!", "welcome_back": "वापसी पर स्वागत है, <b>{name}</b>!",
        "choose_language": "🌐 <b>अपनी भाषा चुनें:</b>",
        "language_set": "✅ भाषा {lang} में बदल दी गई।",
        "send_url_prompt": "🔗 डाउनलोड के लिए एक लिंक भेजें।",
        "send_search_prompt": "🔍 अपना YouTube खोज शब्द लिखें:",
    },
    "ko": {
        "menu_download": "⬇️ 링크 다운로드", "menu_search": "🔍 유튜브 검색",
        "menu_settings": "⚙️ 설정", "menu_help": "ℹ️ 도움말",
        "menu_stats": "📊 내 통계", "menu_admin": "👑 관리자 패널",
        "menu_language": "🌐 언어",
        "welcome_new": "안녕하세요, <b>{name}</b>님!", "welcome_back": "다시 오신 것을 환영합니다, <b>{name}</b>님!",
        "choose_language": "🌐 <b>언어를 선택하세요:</b>",
        "language_set": "✅ 언어가 {lang}(으)로 변경되었습니다.",
        "send_url_prompt": "🔗 다운로드할 링크를 보내주세요.",
        "send_search_prompt": "🔍 유튜브 검색어를 입력하세요:",
    },
    "vi": {
        "menu_download": "⬇️ Tải Liên Kết", "menu_search": "🔍 Tìm trên YouTube",
        "menu_settings": "⚙️ Cài đặt", "menu_help": "ℹ️ Trợ giúp",
        "menu_stats": "📊 Thống Kê Của Tôi", "menu_admin": "👑 Bảng Quản Trị",
        "menu_language": "🌐 Ngôn ngữ",
        "welcome_new": "Xin chào, <b>{name}</b>!", "welcome_back": "Chào mừng trở lại, <b>{name}</b>!",
        "choose_language": "🌐 <b>Chọn ngôn ngữ của bạn:</b>",
        "language_set": "✅ Đã đổi ngôn ngữ sang {lang}.",
        "send_url_prompt": "🔗 Gửi cho tôi một liên kết để tải xuống.",
        "send_search_prompt": "🔍 Nhập từ khóa tìm kiếm YouTube:",
    },
}

MENU_KEYS = ("menu_download", "menu_search", "menu_settings",
             "menu_help", "menu_stats", "menu_admin", "menu_language")

def t(key: str, uid: int, **kwargs) -> str:
    """Translate `key` into the user's chosen language, falling back to English."""
    lang = get_state(uid).language
    s = TRANSLATIONS.get(lang, {}).get(key) or TRANSLATIONS["en"].get(key, key)
    if kwargs:
        try:
            s = s.format(**kwargs)
        except Exception:
            pass
    return s

def resolve_menu_key(text: str) -> Optional[str]:
    """Match a reply-keyboard button press back to its canonical key,
    regardless of which language it was rendered in."""
    for lang_dict in TRANSLATIONS.values():
        for key in MENU_KEYS:
            if lang_dict.get(key) == text:
                return key
    return None

def language_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(LANGUAGES), 2):
        row = [InlineKeyboardButton(label, callback_data=f"lang|{code}")
               for code, label in LANGUAGES[i:i + 2]]
        rows.append(row)
    return InlineKeyboardMarkup(rows)

# ──────────────────────────────────────────────────────────────────
#  URL HELPERS
# ──────────────────────────────────────────────────────────────────
def normalize_url(url: str) -> str:
    url = url.replace("music.youtube.com", "www.youtube.com")
    url = url.replace("m.youtube.com", "www.youtube.com")
    # NEW in v6.5: TikTok slideshow/photo posts use a "/photo/<id>" URL, which
    # some yt-dlp versions' TikTok extractor doesn't match at all (raises
    # "Unsupported URL"), even though the underlying post ID works fine via
    # the "/video/<id>" path pattern the extractor DOES match. Rewriting
    # here is a cheap, safe workaround while yt-dlp catches up.
    if "tiktok.com" in url and "/photo/" in url:
        url = url.replace("/photo/", "/video/")
    m = re.search(r"youtube\.com/shorts/([A-Za-z0-9_-]+)", url)
    if m:
        l = re.search(r"[?&]list=([A-Za-z0-9_-]+)", url)
        out = "https://www.youtube.com/watch?v=" + m.group(1)
        if l: out += "&list=" + l.group(1)
        return out
    m = re.search(r"youtu\.be/([A-Za-z0-9_-]+)", url)
    if m:
        l = re.search(r"[?&]list=([A-Za-z0-9_-]+)", url)
        out = "https://www.youtube.com/watch?v=" + m.group(1)
        if l: out += "&list=" + l.group(1)
        return out
    if "youtube.com/watch" in url:
        v = re.search(r"[?&]v=([A-Za-z0-9_-]+)", url)
        l = re.search(r"[?&]list=([A-Za-z0-9_-]+)", url)
        if v:
            out = "https://www.youtube.com/watch?v=" + v.group(1)
            if l: out += "&list=" + l.group(1)
            return out
    if "youtube.com/playlist" in url:
        l = re.search(r"[?&]list=([A-Za-z0-9_-]+)", url)
        if l:
            return "https://www.youtube.com/playlist?list=" + l.group(1)
    if any(d in url for d in ["facebook.com", "fb.watch"]):
        from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
        parts  = urlsplit(url)
        params = [(k, v) for k, v in parse_qsl(parts.query)
                  if k.lower() not in FACEBOOK_JUNK_PARAMS]
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), ""))
    return url

def is_playlist_url(url: str) -> bool:
    return any(x in url.lower() for x in ["list=", "/playlist", "/sets/", "/album/"])

def strip_playlist(url: str) -> str:
    v = re.search(r"[?&]v=([A-Za-z0-9_-]+)", url)
    if v and "youtube.com" in url:
        return "https://www.youtube.com/watch?v=" + v.group(1)
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    parts  = urlsplit(url)
    params = [(k, val) for k, val in parse_qsl(parts.query)
              if k.lower() not in ("list", "si", "start_radio", "index", "pp")]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment))

def has_single_video(url: str) -> bool:
    return bool(re.search(r"[?&]v=", url))

def _fb_mbasic_url(url: str) -> Optional[str]:
    if "facebook.com" not in url:
        return None
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    parts  = urlsplit(url)
    netloc = "mbasic.facebook.com"
    path   = parts.path
    m = re.search(r"/reel/(\d+)", path)
    if m:
        path = f"/video/{m.group(1)}/"
    params = [(k, v) for k, v in parse_qsl(parts.query)
              if k.lower() not in FACEBOOK_JUNK_PARAMS]
    return urlunsplit(("https", netloc, path, urlencode(params), ""))

def get_best_format(url: str, fmt: str) -> str:
    single_sites = ["pinterest.", "tiktok.", "twitter.", "x.com", "instagram.",
                    "reddit.", "redd.it", "facebook.", "fb.watch", "vm.tiktok"]
    if any(s in url.lower() for s in single_sites):
        if "bestaudio" in fmt and "bestvideo" not in fmt:
            return "bestaudio/best"
        return "best[ext=mp4]/best"
    return fmt

# ──────────────────────────────────────────────────────────────────
#  IMAGE / SLIDESHOW POST HELPERS  (NEW in v6.0)
# ──────────────────────────────────────────────────────────────────
def extract_images(info: Optional[dict]) -> List[str]:
    """Pull image URLs out of a yt-dlp info dict for photo/slideshow posts
    (e.g. TikTok photo mode). Returns [] for normal video content."""
    if not info:
        return []
    urls: List[str] = []

    # Newer yt-dlp exposes TikTok slideshow images directly under "images"
    for im in (info.get("images") or []):
        u = im.get("url") if isinstance(im, dict) else im
        if u:
            urls.append(u)
    if urls:
        return urls[:MAX_ALBUM_IMAGES]

    # Some slideshow posts show up as a "playlist" of single-image entries
    if info.get("_type") == "playlist":
        for entry in (info.get("entries") or []):
            if not entry:
                continue
            for im in (entry.get("images") or []):
                u = im.get("url") if isinstance(im, dict) else im
                if u:
                    urls.append(u)
        if urls:
            return urls[:MAX_ALBUM_IMAGES]

    # Fallback: image-only formats (no audio/video codec, image extension)
    formats = info.get("formats") or []
    for f in formats:
        ext = (f.get("ext") or "").lower()
        is_image_ext = ext in ("jpg", "jpeg", "webp", "png")
        no_av = f.get("vcodec") in (None, "none") and f.get("acodec") in (None, "none")
        if f.get("url") and (is_image_ext or no_av) and not info.get("duration"):
            urls.append(f["url"])
    return urls[:MAX_ALBUM_IMAGES]

def is_image_post(info: Optional[dict]) -> bool:
    if not info:
        return False
    if info.get("duration"):
        return False
    return bool(extract_images(info))

def image_download_keyboard(task_id: str, count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🖼️ Download {count} Image(s)", callback_data=f"img|{task_id}")],
        [InlineKeyboardButton(f"{E['cancel']} Cancel", callback_data=f"cancel|{task_id}")],
    ])

def post_action_keyboard(task_id: str, is_audio: bool) -> InlineKeyboardMarkup:
    """NEW in v6.6: action buttons attached under a sent video/audio message."""
    swap_label = f"{E['video']} Video" if is_audio else f"{E['music']} Audio"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(swap_label,              callback_data=f"pact|swap|{task_id}"),
         InlineKeyboardButton(f"{E['moon']} Grayscale", callback_data=f"pact|gray|{task_id}")],
        [InlineKeyboardButton(f"{E['sub']} Subtitle",   callback_data=f"pact|sub|{task_id}"),
         InlineKeyboardButton(f"{E['thumb']} Thumbnail",callback_data=f"pact|thumb|{task_id}")],
        [InlineKeyboardButton(f"{E['trash']} Remove Buttons", callback_data=f"pact|rmbtn|{task_id}")],
    ])

# ──────────────────────────────────────────────────────────────────
#  PER-USER STATE
# ──────────────────────────────────────────────────────────────────
class UserState:
    def __init__(self, uid: int):
        self.user_id           = uid
        self.subtitles         = False
        self.thumbnail         = True
        self.caption           = True
        self.search_count      = SEARCH_RESULTS_COUNT
        self.language          = "en"
        self.pending_url       = None
        self.pending_info      = None
        self.active_tasks:     List[asyncio.Task] = []
        self.active_procs:     Dict[str, asyncio.Task] = {}
        self.search_results:   List[dict] = []
        self.search_page:      int = 0
        self.search_mode:      str = "single"   # NEW in v6.7: "single" or "playlist"
        self.pl_search_results: List[dict] = []  # NEW in v6.7: playlist/album search hits
        self.pl_search_page:   int = 0
        self.pl_browse_page:   int = 0           # NEW in v6.7: page while browsing a chosen playlist's videos
        self.playlist_entries: List[dict] = []
        self.playlist_title:   str = ""
        self.playlist_url:     str = ""
        self.cancel_flags:     Dict[str, bool] = {}
        self.last_req_time:    float = 0.0

user_states:      Dict[int, UserState] = {}
global_queue:     Optional[asyncio.Queue] = None
registered_users: set = set()
banned_users:     set = set()
stats_data:       dict = {"total_downloads": 0, "user_downloads": {}}
user_info:        dict = {}   # uid(str) -> {username, first_name, msg_count, last_seen, downloads: [...]}

# NEW in v6.6: remembers recently-completed downloads so the action buttons
# attached under the sent media (Video/Audio swap, Grayscale, Subtitle,
# Thumbnail) know what URL/title/quality to work from without re-asking.
completed_media: "OrderedDict[str, dict]" = OrderedDict()
MAX_COMPLETED_MEDIA = 500

def register_completed(task_id: str, rec: dict) -> None:
    completed_media[task_id] = rec
    if len(completed_media) > MAX_COMPLETED_MEDIA:
        completed_media.popitem(last=False)

def get_state(uid: int) -> UserState:
    if uid not in user_states:
        user_states[uid] = UserState(uid)
    return user_states[uid]

# ──────────────────────────────────────────────────────────────────
#  PERSISTENCE
# ──────────────────────────────────────────────────────────────────
def load_users() -> None:
    global registered_users, banned_users
    if USER_DB_FILE.exists():
        try:
            data = json.loads(USER_DB_FILE.read_text())
            registered_users = set(data.get("users", []))
            banned_users      = set(data.get("banned", []))
        except Exception as e:
            logger.warning(f"load_users: {e}")

def save_users() -> None:
    try:
        USER_DB_FILE.write_text(json.dumps({
            "users":  list(registered_users),
            "banned": list(banned_users),
        }))
    except Exception as e:
        logger.warning(f"save_users: {e}")

def register_user(uid: int) -> bool:
    if uid not in registered_users:
        registered_users.add(uid)
        save_users()
        return True
    return False

def ban_user(uid: int) -> None:
    banned_users.add(uid)
    save_users()

def unban_user(uid: int) -> None:
    banned_users.discard(uid)
    save_users()

def is_banned(uid: int) -> bool:
    return uid in banned_users

def load_stats() -> None:
    global stats_data
    if STATS_FILE.exists():
        try:
            stats_data = json.loads(STATS_FILE.read_text())
        except Exception as e:
            logger.warning(f"load_stats: {e}")
    stats_data.setdefault("total_downloads", 0)
    stats_data.setdefault("user_downloads", {})

def save_stats() -> None:
    try:
        STATS_FILE.write_text(json.dumps(stats_data))
    except Exception as e:
        logger.warning(f"save_stats: {e}")

def record_download(uid: int) -> None:
    stats_data["total_downloads"] = stats_data.get("total_downloads", 0) + 1
    ud = stats_data.setdefault("user_downloads", {})
    ud[str(uid)] = ud.get(str(uid), 0) + 1
    save_stats()

# ── NEW in v6.0: per-user profile (username / msg count / last seen / downloads) ──
def load_user_info() -> None:
    global user_info
    if USER_INFO_FILE.exists():
        try:
            user_info = json.loads(USER_INFO_FILE.read_text())
        except Exception as e:
            logger.warning(f"load_user_info: {e}")

def save_user_info() -> None:
    try:
        USER_INFO_FILE.write_text(json.dumps(user_info))
    except Exception as e:
        logger.warning(f"save_user_info: {e}")

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def touch_user(user) -> None:
    """Call on every incoming message: bumps message count / last-seen / identity."""
    uid = str(user.id)
    rec = user_info.setdefault(uid, {
        "username": None, "first_name": None,
        "msg_count": 0, "last_seen": None, "downloads": [],
    })
    rec["username"]   = getattr(user, "username", None)
    rec["first_name"] = getattr(user, "first_name", None)
    rec["msg_count"]  = rec.get("msg_count", 0) + 1
    rec["last_seen"]  = _now_str()
    save_user_info()

def log_download(uid: int, title: str) -> None:
    """Call whenever a download (video or image post) completes successfully."""
    rec = user_info.setdefault(str(uid), {
        "username": None, "first_name": None,
        "msg_count": 0, "last_seen": None, "downloads": [],
    })
    downloads = rec.setdefault("downloads", [])
    downloads.append({"title": (title or "Untitled")[:80], "time": _now_str()})
    rec["downloads"] = downloads[-20:]  # keep last 20 per user
    save_user_info()

# ──────────────────────────────────────────────────────────────────
#  TELEGRAM HELPERS
# ──────────────────────────────────────────────────────────────────
async def safe_edit(msg: Message, text: str, reply_markup=None) -> bool:
    try:
        await msg.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        return True
    except BadRequest as e:
        if "not modified" in str(e).lower():
            return False
        return False
    except (NetworkError, TimedOut):
        await asyncio.sleep(1)
        return False
    except TelegramError:
        return False

async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass

async def notify_admin(bot: Bot, text: str) -> None:
    try:
        await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.HTML, disable_notification=True)
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────
#  RE-HOST FALLBACK  (used when a file is too big to send via Telegram,
#  or when Telegram rejects the upload for size reasons). Works the
#  same way regardless of source platform — YouTube, TikTok, Facebook,
#  Instagram, etc. — since by this point it's just a local file on disk.
# ──────────────────────────────────────────────────────────────────
async def _upload_to_0x0(session: aiohttp.ClientSession, fpath: Path, data: bytes) -> Optional[str]:
    form = aiohttp.FormData()
    form.add_field("file", data, filename=fpath.name)
    async with session.post("https://0x0.st", data=form, timeout=REHOST_TIMEOUT_SECS) as resp:
        if resp.status == 200:
            link = (await resp.text()).strip()
            return link if link.startswith("http") else None
        body = (await resp.text())[:200]
        logging.warning("0x0.st upload failed: HTTP %s | %s", resp.status, body)
        return None

async def _upload_to_tmpfiles(session: aiohttp.ClientSession, fpath: Path, data: bytes) -> Optional[str]:
    form = aiohttp.FormData()
    form.add_field("file", data, filename=fpath.name)
    async with session.post("https://tmpfiles.org/api/v1/upload", data=form,
                             timeout=REHOST_TIMEOUT_SECS) as resp:
        if resp.status == 200:
            try:
                payload = await resp.json(content_type=None)
                url = payload.get("data", {}).get("url", "")
            except Exception:
                return None
            if url.startswith("http"):
                # tmpfiles.org needs "/dl/" inserted for a direct download link
                return url.replace("tmpfiles.org/", "tmpfiles.org/dl/", 1)
            return None
        body = (await resp.text())[:200]
        logging.warning("tmpfiles.org upload failed: HTTP %s | %s", resp.status, body)
        return None

_UPLOADERS = {"0x0": _upload_to_0x0, "tmpfiles": _upload_to_tmpfiles}

async def upload_to_rehost(fpath: Path) -> Optional[str]:
    """Upload a local file to a free, card-free host and return its public URL.
    NEW in v6.3: tries each host in REHOST_HOSTS, retrying a few times per
    host, before giving up — so one flaky/down provider doesn't sink the
    whole fallback-link feature."""
    try:
        async with aiofiles.open(fpath, "rb") as f:
            data = await f.read()
    except Exception as e:
        logging.error("Re-host: could not read file: %s", e)
        return None

    # NEW in v6.2: 0x0.st (and similar free hosts) can silently reject
    # uploads sent with the default aiohttp User-Agent. Send a normal
    # browser-style one so the upload isn't quietly dropped.
    async with aiohttp.ClientSession(headers={"User-Agent": BROWSER_HEADERS["User-Agent"]}) as session:
        for host in REHOST_HOSTS:
            uploader = _UPLOADERS.get(host)
            if not uploader:
                continue
            for attempt in range(1, REHOST_RETRIES + 1):
                try:
                    link = await uploader(session, fpath, data)
                    if link:
                        return link
                except Exception as e:
                    logging.warning("%s upload attempt %d/%d error: %s",
                                     host, attempt, REHOST_RETRIES, e)
                if attempt < REHOST_RETRIES:
                    await asyncio.sleep(2 * attempt)
            logging.warning("Giving up on host %s, trying next fallback host if any.", host)
    return None

async def send_fallback_link(bot: Bot, chat_id: int, status_msg: Message,
                              fpath: Path, title: str, prefix: str = "") -> bool:
    """Send a stable download link instead of the file itself. Returns True on success."""
    await safe_edit(status_msg,
        f"{prefix}{E['warn']} File too large to send directly.\n"
        f"{E['rocket']} Uploading a stable download link instead…")

    link = await upload_to_rehost(fpath)

    if link:
        await safe_edit(status_msg,
            f"{prefix}{E['check']} <b>{title[:60]}</b>\n\n"
            f"Too large to send directly — here's a stable link instead:\n"
            f"{link}\n\n"
            f"<i>Works from any device/network.</i>")
        return True
    else:
        await safe_edit(status_msg,
            f"{prefix}{E['cross']} File was too large to send, and the "
            f"fallback link upload also failed. Try a lower quality setting.")
        return False

# ──────────────────────────────────────────────────────────────────
#  PROGRESS BAR
# ──────────────────────────────────────────────────────────────────
def fmt_size(b):
    if b is None: return "?"
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def fmt_speed(bps):
    if bps is None: return "?"
    return f"{bps/1_048_576:.2f} MB/s"

def fmt_eta(s):
    if s is None: return "?"
    s = int(s)
    if s < 60: return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

def get_actual_resolution(fpath: Path) -> Optional[str]:
    """NEW in v6.4: read the real pixel resolution of a downloaded video via
    ffprobe, so completion messages show what was ACTUALLY downloaded rather
    than just repeating the preset label the user clicked."""
    try:
        import subprocess
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0", str(fpath)],
            capture_output=True, text=True, timeout=15,
        )
        line = out.stdout.strip().splitlines()[0] if out.stdout.strip() else ""
        if "," in line:
            w, h = line.split(",")[:2]
            return f"{int(w)}x{int(h)}"
    except Exception as e:
        logger.warning(f"get_actual_resolution: {e}")
    return None
    return None
    if s < 60: return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

def build_bar(pct: float) -> str:
    f = int(round(BAR_LEN * pct / 100))
    return FILLED * f + EMPTY * (BAR_LEN - f)

def build_progress_text(d: dict, title: str, prefix: str = "") -> str:
    dl    = d.get("downloaded_bytes") or 0
    total = d.get("total_bytes") or d.get("total_bytes_estimate")
    spd   = d.get("speed")
    eta   = d.get("eta")
    pct   = (dl / total * 100) if total and total > 0 else 0
    t2    = (title or "Media")[:50]
    if not total:
        return (f"{prefix}{E['film']} <b>{t2}</b>\n\n"
                f"⏳ Downloading…\n\n"
                f"{E['size']} {fmt_size(dl)}\n"
                f"{E['speed']} {fmt_speed(spd)}\n"
                f"<i>Calculating…</i>")
    return (f"{prefix}{E['film']} <b>{t2}</b>\n\n"
            f"{build_bar(pct)}  <b>{pct:.1f}%</b>\n\n"
            f"{E['size']} {fmt_size(dl)} / {fmt_size(total)}\n"
            f"{E['speed']} {fmt_speed(spd)}\n"
            f"{E['time']} ETA: {fmt_eta(eta)}")

# ──────────────────────────────────────────────────────────────────
#  KEYBOARDS
# ──────────────────────────────────────────────────────────────────
def main_menu_keyboard(uid: int) -> ReplyKeyboardMarkup:
    rows = [
        [t("menu_download", uid), t("menu_search", uid)],
        [t("menu_settings", uid), t("menu_help", uid)],
        [t("menu_stats", uid),    t("menu_language", uid)],
    ]
    if uid == ADMIN_ID:
        rows.append([t("menu_admin", uid)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)

def quality_keyboard(task_id: str, uid: int) -> InlineKeyboardMarkup:
    s = get_state(uid)
    sub = f"{E['on']} Subs"    if s.subtitles else f"{E['off']} Subs"
    th  = f"{E['on']} Thumb"   if s.thumbnail  else f"{E['off']} Thumb"
    cap = f"{E['on']} Caption" if s.caption    else f"{E['off']} Caption"
    btns = [InlineKeyboardButton(p["label"], callback_data=f"dl|{task_id}|{k}")
            for k, p in QUALITY_PRESETS.items()]
    rows = [btns[i:i+2] for i in range(0, len(btns), 2)]
    rows.append([InlineKeyboardButton(sub, callback_data=f"tog|sub|{task_id}"),
                 InlineKeyboardButton(th,  callback_data=f"tog|thumb|{task_id}"),
                 InlineKeyboardButton(cap, callback_data=f"tog|cap|{task_id}")])
    rows.append([InlineKeyboardButton(f"{E['cancel']} Cancel", callback_data=f"cancel|{task_id}")])
    return InlineKeyboardMarkup(rows)

def cancel_keyboard(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"{E['cancel']} Cancel", callback_data=f"cancel|{task_id}")
    ]])

def retry_keyboard(quality_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"{E['retry']} Retry", callback_data=f"retry|{quality_key}")
    ]])

def playlist_choice_keyboard(task_id: str, has_single: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"{E['playlist']} Download Whole Playlist", callback_data=f"pl_all|{task_id}")]]
    if has_single:
        rows.append([InlineKeyboardButton(f"{E['film']} Just This Video", callback_data=f"pl_single|{task_id}")])
    rows.append([InlineKeyboardButton(f"{E['cross']} Cancel", callback_data="search_cancel")])
    return InlineKeyboardMarkup(rows)

def playlist_quality_keyboard(task_id: str, uid: int) -> InlineKeyboardMarkup:
    s = get_state(uid)
    sub = f"{E['on']} Subs"    if s.subtitles else f"{E['off']} Subs"
    th  = f"{E['on']} Thumb"   if s.thumbnail  else f"{E['off']} Thumb"
    cap = f"{E['on']} Caption" if s.caption    else f"{E['off']} Caption"
    btns = [InlineKeyboardButton(p["label"], callback_data=f"pldl|{task_id}|{k}")
            for k, p in QUALITY_PRESETS.items()]
    rows = [btns[i:i+2] for i in range(0, len(btns), 2)]
    rows.append([InlineKeyboardButton(sub, callback_data=f"tog|sub|{task_id}"),
                 InlineKeyboardButton(th,  callback_data=f"tog|thumb|{task_id}"),
                 InlineKeyboardButton(cap, callback_data=f"tog|cap|{task_id}")])
    rows.append([InlineKeyboardButton(f"{E['cross']} Cancel", callback_data="search_cancel")])
    return InlineKeyboardMarkup(rows)

def playlist_cancel_keyboard(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"{E['cancel']} Stop After Current Item", callback_data=f"plcancel|{task_id}")
    ]])

def search_result_keyboard(results: list, page: int = 0) -> InlineKeyboardMarkup:
    start = page * SEARCH_PAGE_SIZE
    end   = start + SEARCH_PAGE_SIZE
    rows  = []
    for i, r in enumerate(results[start:end]):
        title  = (r.get("title") or "Result")[:45]
        dur    = fmt_eta(r.get("duration")) if r.get("duration") else "?"
        vid_id = r.get("id", "")
        url    = r.get("url") or r.get("webpage_url") or f"https://www.youtube.com/watch?v={vid_id}"
        rows.append([InlineKeyboardButton(f"{start+i+1}. {title} [{dur}]", callback_data=f"search_pick|{url}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(f"{E['prev']} Prev", callback_data=f"search_page|{page-1}"))
    total_pages = (len(results) + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    if end < len(results):
        nav.append(InlineKeyboardButton(f"Next {E['next']}", callback_data=f"search_page|{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(f"{E['cross']} Cancel", callback_data="search_cancel")])
    return InlineKeyboardMarkup(rows)

# ──────────────────────────────────────────────────────────────────
#  PLAYLIST / ALBUM SEARCH KEYBOARDS  (NEW in v6.7)
#  All callback_data below is INDEX-based (into state.pl_search_results /
#  state.playlist_entries), never the raw URL — playlist URLs are long
#  enough to blow past Telegram's 64-byte callback_data limit, and index
#  lookups also mean we don't need any external cache/DB for this.
# ──────────────────────────────────────────────────────────────────
def search_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{E['music']} Single Search",           callback_data="searchmode|single")],
        [InlineKeyboardButton(f"{E['playlist']} Playlist/Album Search", callback_data="searchmode|playlist")],
    ])

def playlist_search_result_keyboard(results: list, page: int = 0) -> InlineKeyboardMarkup:
    start = page * SEARCH_PAGE_SIZE
    end   = start + SEARCH_PAGE_SIZE
    rows  = []
    for i, r in enumerate(results[start:end], start):
        title = (r.get("title") or "Playlist")[:45]
        rows.append([InlineKeyboardButton(f"{i+1}. {E['playlist']} {title}", callback_data=f"plsearch_pick|{i}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(f"{E['prev']} Prev", callback_data=f"plsearch_page|{page-1}"))
    total_pages = (len(results) + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    if end < len(results):
        nav.append(InlineKeyboardButton(f"Next {E['next']}", callback_data=f"plsearch_page|{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(f"{E['cross']} Cancel", callback_data="search_cancel")])
    return InlineKeyboardMarkup(rows)

def playlist_action_keyboard(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{E['playlist']} Download Whole Playlist", callback_data=f"pl_all|{task_id}")],
        [InlineKeyboardButton("🎯 Choose Specific Videos", callback_data=f"pl_browse|{task_id}")],
        [InlineKeyboardButton(f"{E['cross']} Cancel", callback_data="search_cancel")],
    ])

def playlist_browse_keyboard(entries: list, page: int = 0) -> InlineKeyboardMarkup:
    start = page * SEARCH_PAGE_SIZE
    end   = start + SEARCH_PAGE_SIZE
    rows  = []
    for i, e in enumerate(entries[start:end], start):
        title = (e.get("title") or f"Video {i+1}")[:45]
        dur   = fmt_eta(e.get("duration")) if e.get("duration") else "?"
        rows.append([InlineKeyboardButton(f"{i+1}. {title} [{dur}]", callback_data=f"pl_item_pick|{i}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(f"{E['prev']} Prev", callback_data=f"pl_browse_page|{page-1}"))
    total_pages = (len(entries) + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    if end < len(entries):
        nav.append(InlineKeyboardButton(f"Next {E['next']}", callback_data=f"pl_browse_page|{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(f"{E['cross']} Cancel", callback_data="search_cancel")])
    return InlineKeyboardMarkup(rows)

def settings_keyboard(uid: int) -> InlineKeyboardMarkup:
    s   = get_state(uid)
    sub = f"{E['on']} Subtitles: ON"  if s.subtitles else f"{E['off']} Subtitles: OFF"
    th  = f"{E['on']} Thumbnail: ON"  if s.thumbnail  else f"{E['off']} Thumbnail: OFF"
    cap = f"{E['on']} Caption: ON"    if s.caption    else f"{E['off']} Caption: OFF"
    lang_label = LANGUAGE_LABELS.get(s.language, "🇬🇧 English")
    rows = [
        [InlineKeyboardButton(sub, callback_data="settings|tog_sub"),
         InlineKeyboardButton(th,  callback_data="settings|tog_thumb")],
        [InlineKeyboardButton(cap, callback_data="settings|tog_cap")],
        [InlineKeyboardButton(
            f"📋 Results per search: {s.search_count}",
            callback_data="settings|count_menu")],
        [InlineKeyboardButton(f"🌐 Language: {lang_label}", callback_data="settings|language")],
        [InlineKeyboardButton("✅ Done", callback_data="settings|close")],
    ]
    return InlineKeyboardMarkup(rows)

def search_count_keyboard() -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(str(n), callback_data=f"settings|set_count|{n}")
        for n in SEARCH_COUNT_OPTIONS
    ]]
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="settings|back")])
    return InlineKeyboardMarkup(rows)

def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics",  callback_data="admin|stats"),
         InlineKeyboardButton("👥 User List",   callback_data="admin|userlist")],
        [InlineKeyboardButton("🧹 Clear Cache", callback_data="admin|clearcache"),
         InlineKeyboardButton("📢 Broadcast",   callback_data="admin|broadcast")],
        [InlineKeyboardButton("🔒 Ban User",    callback_data="admin|ban"),
         InlineKeyboardButton("🔓 Unban User",  callback_data="admin|unban")],
    ])

# ──────────────────────────────────────────────────────────────────
#  TASK ID
# ──────────────────────────────────────────────────────────────────
_tc = 0
def new_task_id() -> str:
    global _tc
    _tc += 1
    return f"t{_tc}_{int(time.time())}"

# ──────────────────────────────────────────────────────────────────
#  YT-DLP HELPERS
# ──────────────────────────────────────────────────────────────────
def _is_login_error(err: str) -> bool:
    low = err.lower()
    return any(kw in low for kw in (
        "login", "sign in", "log in", "requires authentication",
        "confirm your age", "members only",
        "this content isn't available",
    ))

async def fetch_info(url: str) -> tuple:
    url   = normalize_url(url)
    is_fb = any(d in url for d in ["facebook.com", "fb.watch"])
    is_tt = any(d in url for d in ["tiktok.com", "vm.tiktok", "vt.tiktok"])
    is_yt = any(d in url for d in ["youtube.com", "youtu.be"])
    is_ig = "instagram.com" in url

    def _base_opts() -> dict:
        o = {
            "quiet": False, "no_warnings": False,
            "skip_download": True, "noplaylist": True,
            "socket_timeout": 30, "retries": 3,
            "http_headers": BROWSER_HEADERS,
            "extractor_args": EXTRACTOR_ARGS,
        }
        if COOKIES_FILE.exists():
            o["cookiefile"] = str(COOKIES_FILE)
        return o

    loop = asyncio.get_running_loop()

    def _extract(u: str, extra: dict = None) -> dict:
        opts = _base_opts()
        if extra:
            opts.update(extra)
        with yt_dlp.YoutubeDL(opts) as y:
            return y.extract_info(u, download=False)

    # ── Primary attempt ───────────────────────────────────────────
    try:
        info = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: _extract(url)), timeout=60)
        return info, None
    except asyncio.TimeoutError:
        return None, "Request timed out. Check your internet and try again."
    except yt_dlp.utils.DownloadError as e:
        err = str(e)

        # ── YouTube login-wall fallback ───────────────────────────
        if is_yt and _is_login_error(err):
            logger.info("YouTube login error — retrying with android_embedded extractor")
            for client in ("android_embedded", "web_embedded"):
                try:
                    info = await asyncio.wait_for(
                        loop.run_in_executor(None, lambda c=client: _extract(url, {
                            "extractor_args": {"youtube": {"player_client": [c]}},
                        })), timeout=60)
                    return info, None
                except Exception:
                    continue
            cookie_hint = ""
            if not COOKIES_FILE.exists():
                cookie_hint = (
                    "\n\n💡 <b>Fix:</b> Export <code>cookies.txt</code> from your "
                    "browser (logged into YouTube) and place it next to the bot script."
                )
            return None, (
                f"YouTube requires login for this content.{cookie_hint}\n"
                f"<i>Try: pip install -U yt-dlp</i>"
            )

        # ── Instagram login-wall fallback ─────────────────────────
        if is_ig and _is_login_error(err):
            logger.info("Instagram login error — retrying with mobile UA")
            mobile_ua = (
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Mobile Safari/537.36 Instagram/300.0"
            )
            try:
                info = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: _extract(url, {
                        "http_headers": {**BROWSER_HEADERS, "User-Agent": mobile_ua},
                    })), timeout=60)
                return info, None
            except Exception:
                pass
            cookie_hint = ""
            if not COOKIES_FILE.exists():
                cookie_hint = (
                    "\n\n💡 <b>Fix:</b> Export <code>cookies.txt</code> from your "
                    "browser (logged into Instagram) and place it next to the bot script."
                )
            return None, (
                f"Instagram requires login for this content.{cookie_hint}\n"
                f"<i>Try: pip install -U yt-dlp</i>"
            )

        # ── Facebook mbasic fallback ───────────────────────────────
        if is_fb and any(kw in err for kw in ("Cannot parse data", "Unsupported URL", "Unable to extract")):
            mbasic = _fb_mbasic_url(url)
            if mbasic:
                logger.info(f"FB extractor failed — retrying with mbasic: {mbasic}")
                try:
                    info = await asyncio.wait_for(
                        loop.run_in_executor(None, lambda: _extract(mbasic)), timeout=60)
                    return info, None
                except asyncio.TimeoutError:
                    return None, "Facebook: request timed out on mbasic fallback."
                except yt_dlp.utils.DownloadError as e2:
                    err2 = re.sub(r"ERROR:\s*", "", str(e2)).strip()
                    return None, (
                        f"Facebook download failed (tried both www and mbasic).\n\n"
                        f"<code>{err2[:250]}</code>\n\n"
                        f"<i>Try: pip install -U yt-dlp</i>"
                    )
                except Exception as e2:
                    return None, str(e2)[:300]

        # ── TikTok hint ───────────────────────────────────────────
        if is_tt and _is_login_error(err):
            return None, (
                "TikTok requires a login cookie for this content.\n"
                "Export cookies.txt from your browser (logged-in TikTok) "
                "and place it next to the bot script."
            )

        if is_tt and "Unsupported URL" in err:
            return None, (
                "This TikTok URL format isn't recognized by your installed "
                "yt-dlp version (common for slideshow/photo posts).\n\n"
                "<i>Fix: pip install -U yt-dlp</i> — this is patched frequently."
            )

        if "Private" in err:     return None, "This video is private."
        if "age" in err.lower(): return None, "Age-restricted — cannot fetch."
        if _is_login_error(err):
            cookie_hint = "" if COOKIES_FILE.exists() else \
                "\n\n💡 Place <code>cookies.txt</code> (from your browser) next to the bot script."
            return None, (
                f"This content requires login.{cookie_hint}\n"
                f"<i>Try: pip install -U yt-dlp</i>"
            )
        return None, re.sub(r"ERROR:\s*", "", err).strip()[:300]
    except Exception as e:
        return None, str(e)[:300]

async def fetch_playlist_info(url: str) -> tuple:
    opts = {
        "quiet": True, "no_warnings": True,
        "extract_flat": "in_playlist", "skip_download": True,
        "noplaylist": False, "playlistend": PLAYLIST_MAX_ITEMS,
        "socket_timeout": 30,
        "http_headers": BROWSER_HEADERS,
        "extractor_args": EXTRACTOR_ARGS,
    }
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
    loop = asyncio.get_running_loop()
    try:
        def _x():
            with yt_dlp.YoutubeDL(opts) as y:
                return y.extract_info(url, download=False)
        info = await asyncio.wait_for(loop.run_in_executor(None, _x), timeout=90)
        return info, None
    except asyncio.TimeoutError:
        return None, "Playlist fetch timed out."
    except Exception as e:
        return None, str(e)[:300]

async def search_youtube(query: str, n: int = SEARCH_RESULTS_COUNT) -> list:
    opts = {
        "quiet": True, "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True, "socket_timeout": 30,
    }
    loop = asyncio.get_running_loop()
    try:
        def _x():
            with yt_dlp.YoutubeDL(opts) as y:
                info = y.extract_info(f"ytsearch{n}:{query}", download=False)
                return info.get("entries", []) if info else []
        return await asyncio.wait_for(loop.run_in_executor(None, _x), timeout=60)
    except Exception as e:
        logger.warning(f"search_youtube: {e}")
        return []

async def search_youtube_playlists(query: str, n: int = SEARCH_RESULTS_COUNT) -> list:
    """NEW in v6.7: search specifically for playlists/albums. yt-dlp's
    ytsearch: prefix only returns individual videos, so instead we hit
    YouTube's own search results page with its "Playlist" filter applied
    (sp=EgIQAw%3D%3D) and flat-extract whatever comes back, then keep only
    entries that actually look like playlists (have a list= URL or a
    playlist-flavored ie_key)."""
    from urllib.parse import quote
    search_url = f"https://www.youtube.com/results?search_query={quote(query)}&sp=EgIQAw%253D%253D"
    opts = {
        "quiet": True, "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True, "socket_timeout": 30,
        "http_headers": BROWSER_HEADERS,
        "extractor_args": EXTRACTOR_ARGS,
        "playlistend": max(n * 2, n),  # pull a few extra since we filter afterward
    }
    loop = asyncio.get_running_loop()
    try:
        def _x():
            with yt_dlp.YoutubeDL(opts) as y:
                info = y.extract_info(search_url, download=False)
                entries = info.get("entries", []) if info else []
                out = []
                for e in entries:
                    if not e:
                        continue
                    url_ = e.get("url") or e.get("webpage_url") or ""
                    ie_key = (e.get("ie_key") or "").lower()
                    looks_like_playlist = (
                        "list=" in url_ or "/playlist" in url_
                        or "playlist" in ie_key or e.get("_type") == "playlist"
                    )
                    if looks_like_playlist:
                        out.append(e)
                return out[:n]
        return await asyncio.wait_for(loop.run_in_executor(None, _x), timeout=60)
    except Exception as e:
        logger.warning(f"search_youtube_playlists: {e}")
        return []

def build_ydl_opts(out_dir, fmt, is_audio, subtitles, hook, url: str = "") -> dict:
    opts = {
        "format": fmt,
        "outtmpl": str(out_dir / "%(title).80s.%(ext)s"),
        "noplaylist": True, "quiet": True, "no_warnings": True,
        "progress_hooks": [hook],
        "retries": 5, "fragment_retries": 5, "socket_timeout": 30,
        "http_headers": BROWSER_HEADERS,
        "extractor_args": EXTRACTOR_ARGS,
    }
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
    if any(t in url for t in ["tiktok.com", "vm.tiktok", "vt.tiktok"]):
        try:
            from yt_dlp.networking.impersonate import ImpersonateTarget
            opts["impersonate"] = ImpersonateTarget("chrome", None, None, None)
        except Exception:
            pass
    if is_audio:
        opts["postprocessors"] = [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "320"},
            {"key": "FFmpegMetadata"},
        ]
    else:
        if "+" in fmt:
            opts["merge_output_format"] = "mp4"
        opts["postprocessors"] = [{"key": "FFmpegMetadata"}]
    if subtitles:
        opts.update({
            "writesubtitles": True, "writeautomaticsub": True,
            "subtitleslangs": ["en", "en-US"], "subtitlesformat": "srt",
        })
    return opts

# ──────────────────────────────────────────────────────────────────
#  CORE DOWNLOAD WORKER
# ──────────────────────────────────────────────────────────────────
async def run_download(bot, chat_id, uid, url, quality_key, info, status_msg, task_id,
                       prefix="", allow_retry=True) -> bool:
    state    = get_state(uid)
    preset   = QUALITY_PRESETS[quality_key]
    is_audio = quality_key == "audio"
    title    = info.get("title", "media")
    out_dir  = DOWNLOAD_DIR / f"{uid}_{task_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    url      = normalize_url(url)
    fmt      = get_best_format(url, preset["fmt"])
    loop     = asyncio.get_running_loop()
    last_t   = [0.0]

    def hook(d):
        if d["status"] not in ("downloading", "finished"):
            return
        now = time.monotonic()
        if now - last_t[0] < PROGRESS_THROTTLE_SECS and d["status"] != "finished":
            return
        last_t[0] = now
        text = build_progress_text(d, title, prefix=prefix)
        kb   = cancel_keyboard(task_id) if allow_retry else None
        asyncio.run_coroutine_threadsafe(safe_edit(status_msg, text, reply_markup=kb), loop)

    ydl_opts = build_ydl_opts(out_dir, fmt, is_audio, state.subtitles, hook, url=url)

    try:
        await safe_edit(
            status_msg,
            f"{prefix}{E['rocket']} <b>Starting download…</b>\n\n"
            f"{E['film']} <b>{title[:60]}</b>\n"
            f"Quality: {preset['label']}",
            reply_markup=cancel_keyboard(task_id) if allow_retry else None,
        )

        est = info.get("filesize") or info.get("filesize_approx")
        if est and est / 1_048_576 > MAX_FILE_SIZE_MB:
            # Hard storage cap — too big to even download to disk. No fallback possible.
            await safe_edit(status_msg,
                f"{prefix}{E['warn']} Estimated size <b>{est/1_048_576:.0f} MB</b> "
                f"exceeds the {MAX_FILE_SIZE_MB} MB limit. Skipped.")
            return False

        def _dl():
            with yt_dlp.YoutubeDL(ydl_opts) as y:
                y.download([url])

        # NEW in v6.1: instead of one silent await for up to an hour, poll in
        # short intervals so we can (a) give up within DOWNLOAD_TIMEOUT_SECS
        # instead of 3600s, and (b) send a "still working" heartbeat if the
        # progress hook hasn't fired recently (e.g. stuck resolving formats).
        dl_future = loop.run_in_executor(None, _dl)
        start_ts  = time.monotonic()
        while True:
            try:
                await asyncio.wait_for(asyncio.shield(dl_future), timeout=HEARTBEAT_SECS)
                break
            except asyncio.TimeoutError:
                elapsed = time.monotonic() - start_ts
                if elapsed >= DOWNLOAD_TIMEOUT_SECS:
                    raise
                if time.monotonic() - last_t[0] >= HEARTBEAT_SECS:
                    last_t[0] = time.monotonic()
                    await safe_edit(status_msg,
                        f"{prefix}{E['film']} <b>{title[:60]}</b>\n\n"
                        f"⏳ Still working… ({int(elapsed)}s elapsed)\n"
                        f"<i>Resolving formats / slow connection — this can take a bit.</i>",
                        reply_markup=cancel_keyboard(task_id) if allow_retry else None)

        all_f   = [f for f in out_dir.iterdir() if f.is_file()]
        media_f = [f for f in all_f if f.suffix.lower() not in (".srt", ".vtt", ".ass")]
        sub_f   = [f for f in all_f if f.suffix.lower() in (".srt", ".vtt", ".ass")]
        if not media_f:
            raise FileNotFoundError("yt-dlp produced no output file.")

        fpath   = max(media_f, key=lambda p: p.stat().st_size)
        size_mb = fpath.stat().st_size / 1_048_576
        if size_mb > MAX_FILE_SIZE_MB:
            # Hard storage cap — too big for this bot to handle at all, even via fallback.
            await safe_edit(status_msg,
                f"{prefix}{E['warn']} File is <b>{size_mb:.0f} MB</b> — exceeds limit. Skipped.")
            return False

        cap = None
        actual_res = None
        if not is_audio:
            actual_res = await loop.run_in_executor(None, get_actual_resolution, fpath)
        if state.caption:
            up  = info.get("uploader") or info.get("channel") or "Unknown"
            res_line = f"\n{E['gear']} {actual_res}" if actual_res else ""
            cap = (f"{E['film']} <b>{title}</b>\n"
                   f"{E['user']} {up}  |  {E['time']} {fmt_eta(info.get('duration'))}"
                   f"{res_line}\n"
                   f"{E['link']} <a href='{url}'>Source</a>")

        sent_directly = False

        if size_mb <= TELEGRAM_SEND_LIMIT_MB:
            await safe_edit(status_msg,
                f"{prefix}{E['check']} <b>Done!</b> ({size_mb:.1f} MB"
                f"{', ' + actual_res if actual_res else ''})\n{E['rocket']} Uploading…")

            await bot.send_chat_action(chat_id,
                ChatAction.UPLOAD_VIDEO if not is_audio else ChatAction.UPLOAD_VOICE)

            async with aiofiles.open(fpath, "rb") as fh:
                fb = await fh.read()

            tb = None
            if state.thumbnail and not is_audio:
                tu = info.get("thumbnail")
                if tu:
                    try:
                        tp = out_dir / "thumb.jpg"
                        await loop.run_in_executor(None, lambda: urllib.request.urlretrieve(tu, str(tp)))
                        async with aiofiles.open(tp, "rb") as tf:
                            tb = await tf.read()
                    except Exception:
                        pass

            try:
                action_kb = post_action_keyboard(task_id, is_audio) if allow_retry else None
                if is_audio:
                    sent_msg = await bot.send_audio(chat_id,
                        audio=InputFile(io.BytesIO(fb), filename=fpath.name),
                        title=title[:64], performer=info.get("uploader",""),
                        caption=cap, parse_mode=ParseMode.HTML,
                        reply_markup=action_kb)
                else:
                    ti = InputFile(io.BytesIO(tb), filename="thumb.jpg") if tb else None
                    sent_msg = await bot.send_video(chat_id,
                        video=InputFile(io.BytesIO(fb), filename=fpath.name),
                        caption=cap, parse_mode=ParseMode.HTML,
                        thumbnail=ti, supports_streaming=True,
                        width=info.get("width"), height=info.get("height"),
                        duration=info.get("duration"),
                        reply_markup=action_kb)
                sent_directly = True
                # NEW in v6.6: remember this download so the action buttons
                # (attached above) know what to work from when tapped later.
                if allow_retry:
                    register_completed(task_id, {
                        "url": url, "title": title, "quality_key": quality_key,
                        "is_audio": is_audio, "thumbnail": info.get("thumbnail"),
                    })
            except (BadRequest, NetworkError, TimedOut) as send_err:
                err_l = str(send_err).lower()
                if "too large" in err_l or "entity too large" in err_l or "timed out" in err_l:
                    sent_directly = False  # fall through to re-host fallback below
                else:
                    raise
        else:
            # Already known to exceed Telegram's send limit — skip the attempt
            # entirely and go straight to the re-host fallback. Works the same
            # regardless of source site (YouTube, TikTok, Facebook, Instagram, etc.)
            # since fpath is just a local file at this point.
            pass

        if not sent_directly:
            ok = await send_fallback_link(bot, chat_id, status_msg, fpath, title, prefix=prefix)
            if not ok:
                return False

        if state.subtitles and sub_f:
            for sf in sub_f:
                async with aiofiles.open(sf, "rb") as sfh:
                    sb = await sfh.read()
                await bot.send_document(chat_id,
                    document=InputFile(io.BytesIO(sb), filename=sf.name),
                    caption=f"{E['sub']} Subtitle: <b>{title[:50]}</b>",
                    parse_mode=ParseMode.HTML)
        elif state.subtitles and not sub_f:
            await bot.send_message(chat_id,
                f"{E['warn']} No subtitle found for this video.", parse_mode=ParseMode.HTML)

        record_download(uid)
        log_download(uid, title)
        await safe_delete(status_msg)
        await notify_admin(bot,
            f"{E['bell']} Download done\nUser: <code>{uid}</code>\n"
            f"Title: {title[:50]}\nQuality: {preset['label']}")
        return True

    except asyncio.CancelledError:
        await safe_edit(status_msg, f"{prefix}{E['cancel']} Download cancelled.")
        return False
    except asyncio.TimeoutError:
        rb = retry_keyboard(quality_key) if allow_retry else None
        await safe_edit(status_msg,
            f"{prefix}{E['warn']} Download timed out after {DOWNLOAD_TIMEOUT_SECS//60} min.\n"
            f"Try again, or pick a lower quality — long/high-res videos take longer.",
            reply_markup=rb)
        return False
    except Exception as exc:
        err = str(exc)
        is_to = "timed out" in err.lower() or "timeout" in err.lower()
        if not is_to:
            await notify_admin(bot,
                f"{E['cross']} Error for <code>{uid}</code>\n<code>{err[:200]}</code>")
        rb = retry_keyboard(quality_key) if allow_retry else None
        await safe_edit(status_msg,
            f"{prefix}{E['cross']} <b>Download failed</b>\n\n<code>{err[:300]}</code>",
            reply_markup=rb)
        return False
    finally:
        state.active_procs.pop(task_id, None)
        shutil.rmtree(out_dir, ignore_errors=True)

# ──────────────────────────────────────────────────────────────────
#  IMAGE / SLIDESHOW DOWNLOAD WORKER  (NEW in v6.0)
# ──────────────────────────────────────────────────────────────────
# ──────────────────────────────────────────────────────────────────
#  POST-DOWNLOAD ACTION HELPERS  (NEW in v6.6)
#  Video/Audio swap, on-demand Subtitle fetch, on-demand Grayscale.
# ──────────────────────────────────────────────────────────────────
async def send_subtitle_on_demand(bot: Bot, chat_id: int, url: str, title: str) -> None:
    status = await bot.send_message(chat_id, f"{E['sub']} Fetching subtitles…", parse_mode=ParseMode.HTML)
    tmp_dir = DOWNLOAD_DIR / f"subs_{int(time.time())}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    opts = {
        "skip_download": True, "writesubtitles": True, "writeautomaticsub": True,
        "subtitleslangs": ["en", "en-US"], "subtitlesformat": "srt",
        "outtmpl": str(tmp_dir / "%(title).80s.%(ext)s"),
        "quiet": True, "no_warnings": True, "socket_timeout": 30,
        "http_headers": BROWSER_HEADERS, "extractor_args": EXTRACTOR_ARGS,
    }
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
    loop = asyncio.get_running_loop()
    try:
        def _dl():
            with yt_dlp.YoutubeDL(opts) as y:
                y.download([url])
        await asyncio.wait_for(loop.run_in_executor(None, _dl), timeout=120)
        sub_files = [f for f in tmp_dir.iterdir() if f.suffix.lower() in (".srt", ".vtt")]
        if not sub_files:
            await safe_edit(status, f"{E['warn']} No subtitles found for this video.")
            return
        for sf in sub_files:
            async with aiofiles.open(sf, "rb") as fh:
                data = await fh.read()
            await bot.send_document(chat_id,
                document=InputFile(io.BytesIO(data), filename=sf.name),
                caption=f"{E['sub']} Subtitle: <b>{title[:50]}</b>", parse_mode=ParseMode.HTML)
        await safe_delete(status)
    except Exception as e:
        await safe_edit(status, f"{E['cross']} Failed to fetch subtitles.\n<code>{str(e)[:200]}</code>")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

async def run_grayscale(bot: Bot, chat_id: int, uid: int, rec: dict) -> None:
    status = await bot.send_message(chat_id, f"{E['rocket']} Preparing grayscale version…", parse_mode=ParseMode.HTML)
    url = rec["url"]
    quality_key = rec.get("quality_key") or "480p"
    if quality_key == "audio":
        quality_key = "480p"  # grayscale only makes sense on video
    info, err = await fetch_info(url)
    if not info:
        await safe_edit(status, f"{E['cross']} Could not fetch info\n<code>{err}</code>")
        return
    preset  = QUALITY_PRESETS.get(quality_key, QUALITY_PRESETS["480p"])
    fmt     = get_best_format(url, preset["fmt"])
    out_dir = DOWNLOAD_DIR / f"{uid}_gray_{int(time.time())}"
    out_dir.mkdir(parents=True, exist_ok=True)
    loop = asyncio.get_running_loop()

    def _noop_hook(d):
        pass

    ydl_opts = build_ydl_opts(out_dir, fmt, False, False, _noop_hook, url=url)
    try:
        def _dl():
            with yt_dlp.YoutubeDL(ydl_opts) as y:
                y.download([url])
        await asyncio.wait_for(loop.run_in_executor(None, _dl), timeout=DOWNLOAD_TIMEOUT_SECS)

        media_files = [f for f in out_dir.iterdir() if f.is_file() and f.suffix.lower() not in (".srt", ".vtt", ".ass")]
        if not media_files:
            await safe_edit(status, f"{E['cross']} Download failed — can't apply grayscale.")
            return
        fpath     = max(media_files, key=lambda p: p.stat().st_size)
        gray_path = out_dir / f"gray_{fpath.stem}.mp4"

        await safe_edit(status, f"{E['moon']} Converting to grayscale…")

        import subprocess
        def _convert():
            subprocess.run(
                ["ffmpeg", "-y", "-i", str(fpath), "-vf", "format=gray", "-c:a", "copy", str(gray_path)],
                capture_output=True, timeout=600,
            )
        await loop.run_in_executor(None, _convert)

        if not gray_path.exists() or gray_path.stat().st_size == 0:
            await safe_edit(status, f"{E['cross']} Grayscale conversion failed.")
            return

        size_mb = gray_path.stat().st_size / 1_048_576
        title   = rec.get("title", "media")
        new_id  = new_task_id()
        if size_mb <= TELEGRAM_SEND_LIMIT_MB:
            async with aiofiles.open(gray_path, "rb") as fh:
                data = await fh.read()
            await bot.send_video(chat_id,
                video=InputFile(io.BytesIO(data), filename=gray_path.name),
                caption=f"{E['moon']} <b>{title[:60]} (Grayscale)</b>", parse_mode=ParseMode.HTML,
                reply_markup=post_action_keyboard(new_id, False))
            register_completed(new_id, {**rec, "quality_key": quality_key, "is_audio": False})
            await safe_delete(status)
        else:
            await send_fallback_link(bot, chat_id, status, gray_path, f"{title} (Grayscale)")
    except Exception as e:
        await safe_edit(status, f"{E['cross']} Grayscale failed.\n<code>{str(e)[:200]}</code>")
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)

async def run_image_download(bot, chat_id, uid, url, info, status_msg, task_id, prefix="") -> bool:
    state = get_state(uid)
    title = info.get("title") or "Photo Post"
    urls  = extract_images(info)

    if not urls:
        await safe_edit(status_msg, f"{prefix}{E['cross']} No images found in this post.")
        return False

    await safe_edit(status_msg, f"{prefix}{E['rocket']} Downloading {len(urls)} image(s)…")

    try:
        photos: List[bytes] = []
        async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as session:
            for u in urls:
                try:
                    async with session.get(u, timeout=60) as resp:
                        if resp.status == 200:
                            photos.append(await resp.read())
                except Exception:
                    continue

        if not photos:
            await safe_edit(status_msg, f"{prefix}{E['cross']} Failed to download any images.")
            return False

        cap = None
        if state.caption:
            cap = f"{E['thumb']} <b>{title}</b>\n{E['link']} <a href='{url}'>Source</a>"

        # Telegram media groups accept 2-10 items per call, so chunk if needed.
        chunks = [photos[i:i + 10] for i in range(0, len(photos), 10)]
        for ci, chunk in enumerate(chunks):
            media = []
            for j, b in enumerate(chunk):
                use_caption = cap if (ci == 0 and j == 0) else None
                media.append(InputMediaPhoto(
                    io.BytesIO(b),
                    caption=use_caption,
                    parse_mode=ParseMode.HTML if use_caption else None,
                ))
            if len(media) == 1:
                await bot.send_photo(chat_id, photo=media[0].media,
                                      caption=cap if ci == 0 else None,
                                      parse_mode=ParseMode.HTML if (ci == 0 and cap) else None)
            else:
                await bot.send_media_group(chat_id, media=media)

        record_download(uid)
        log_download(uid, title)
        await safe_delete(status_msg)
        await notify_admin(bot,
            f"{E['thumb']} Image post downloaded\nUser: <code>{uid}</code>\n"
            f"Title: {title[:50]}\nImages: {len(photos)}")
        return True

    except asyncio.CancelledError:
        await safe_edit(status_msg, f"{prefix}{E['cancel']} Download cancelled.")
        return False
    except Exception as exc:
        await notify_admin(bot,
            f"{E['cross']} Image download error for <code>{uid}</code>\n<code>{str(exc)[:200]}</code>")
        await safe_edit(status_msg,
            f"{prefix}{E['cross']} <b>Image download failed</b>\n\n<code>{str(exc)[:300]}</code>")
        return False

# ──────────────────────────────────────────────────────────────────
#  PLAYLIST WORKER
# ──────────────────────────────────────────────────────────────────
async def run_playlist(bot, chat_id, uid, entries, quality_key, overview_msg, pl_title, task_id):
    state = get_state(uid)
    total = len(entries)
    sent = failed = 0
    try:
        for idx, entry in enumerate(entries, 1):
            if state.cancel_flags.get(task_id):
                break
            vid_url = (entry.get("url") or entry.get("webpage_url") or
                       (f"https://www.youtube.com/watch?v={entry['id']}" if entry.get("id") else None))
            if not vid_url:
                failed += 1
                continue
            vid_url = normalize_url(vid_url)
            await safe_edit(overview_msg,
                f"{E['playlist']} <b>{pl_title}</b>\n\n"
                f"{E['queue']} Item <b>{idx}/{total}</b>\n"
                f"{E['check']} Sent: {sent}   {E['cross']} Failed: {failed}",
                reply_markup=playlist_cancel_keyboard(task_id))

            info, err = await fetch_info(vid_url)
            if not info:
                failed += 1
                continue

            t = (info.get("title") or "media")[:60]
            smsg = await bot.send_message(chat_id,
                f"{E['playlist']} <b>Item {idx}/{total}</b>\n{E['film']} <b>{t}</b>\n\n{E['rocket']} Preparing…",
                parse_mode=ParseMode.HTML)

            sub_id = new_task_id()
            ok = await run_download(bot, chat_id, uid, vid_url, quality_key, info,
                                    smsg, sub_id,
                                    prefix=f"{E['playlist']} <b>Item {idx}/{total}</b>\n\n",
                                    allow_retry=False)
            if ok: sent += 1
            else:  failed += 1
            if state.cancel_flags.get(task_id):
                break
    finally:
        state.cancel_flags.pop(task_id, None)
        state.active_procs.pop(task_id, None)

    await safe_edit(overview_msg,
        f"{E['check']} <b>Playlist Finished</b>\n\n"
        f"{E['playlist']} {pl_title}\n\n"
        f"Sent: <b>{sent}</b>   Failed: <b>{failed}</b>   Total: <b>{total}</b>",
        reply_markup=None)

# ──────────────────────────────────────────────────────────────────
#  QUEUE
# ──────────────────────────────────────────────────────────────────
async def enqueue_download(bot, chat_id, uid, url, quality_key, info, status_msg):
    state = get_state(uid)
    tid   = new_task_id()
    state.active_tasks = [t for t in state.active_tasks if not t.done()]
    if len(state.active_tasks) < MAX_CONCURRENT_PER_USER:
        task = asyncio.create_task(
            run_download(bot, chat_id, uid, url, quality_key, info, status_msg, tid))
        state.active_tasks.append(task)
        state.active_procs[tid] = task
    else:
        await safe_edit(status_msg,
            f"{E['queue']} You have {len(state.active_tasks)} active download(s).\n"
            f"Added to queue — will start when a slot opens.")
        await global_queue.put((bot, chat_id, uid, url, quality_key, info, status_msg, tid))

async def queue_worker():
    while True:
        bot, chat_id, uid, url, qk, info, smsg, tid = await global_queue.get()
        state = get_state(uid)
        while True:
            state.active_tasks = [t for t in state.active_tasks if not t.done()]
            if len(state.active_tasks) < MAX_CONCURRENT_PER_USER:
                break
            await asyncio.sleep(2)
        task = asyncio.create_task(run_download(bot, chat_id, uid, url, qk, info, smsg, tid))
        state.active_tasks.append(task)
        state.active_procs[tid] = task
        global_queue.task_done()

# ──────────────────────────────────────────────────────────────────
#  COMMAND HANDLERS
# ──────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = user.id
    new  = register_user(uid)
    touch_user(user)
    greet = t("welcome_new", uid, name=user.first_name) if new else t("welcome_back", uid, name=user.first_name)
    await update.message.reply_text(
        f"{E['wave']} {greet}\n\n{t('intro', uid)}",
        parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard(uid))
    if new:
        await notify_admin(context.bot, f"{E['user']} New user: <code>{uid}</code> — {user.first_name}")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"{E['info']} <b>NEXUS Bot Help</b>\n\n"
        f"<b>Download:</b>\n"
        f"  Send any URL → pick quality → done {E['rocket']}\n\n"
        f"<b>Supported sites:</b>\n"
        f"  YouTube & Shorts ✅  |  Playlists {E['playlist']} ✅\n"
        f"  TikTok ✅ (video & photo posts)  |  Instagram ✅  |  Twitter/X ✅\n"
        f"  Pinterest ✅  |  Facebook ✅  |  1000+ more ✅\n\n"
        f"<b>Playlist download:</b>\n"
        f"  1. Send a YouTube playlist link\n"
        f"  2. Choose <b>Download Whole Playlist</b> or <b>Just This Video</b>\n"
        f"  3. Pick quality — applies to all items\n"
        f"  4. Bot sends each video one by one\n"
        f"  5. Tap <b>Stop After Current Item</b> anytime\n"
        f"  Max {PLAYLIST_MAX_ITEMS} items per playlist.\n\n"
        f"<b>Image / slideshow posts:</b>\n"
        f"  Send a TikTok photo-post link — the bot detects it automatically "
        f"  and sends all images as an album.\n\n"
        f"<b>Subtitles:</b>\n"
        f"  Tap 🔴 Subs → 🟢 ON in quality menu — .srt sent after video\n\n"
        f"<b>Settings:</b>\n"
        f"  Change search results count (10/20/50/100) and your language\n\n"
        f"{E['stats']} /stats — personal download count\n"
        f"{E['globe']} /language — change bot language\n"
        f"/cancel — cancel all active downloads",
        parse_mode=ParseMode.HTML)

async def cmd_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await update.message.reply_text(
        t("choose_language", uid), parse_mode=ParseMode.HTML,
        reply_markup=language_keyboard())

async def cmd_cancel_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    state = get_state(uid)
    n = sum(1 for t in state.active_tasks if not t.done() and not t.cancel())
    state.active_tasks.clear()
    await update.message.reply_text(f"{E['cancel']} Cancelled {n} active download(s).")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = user.id
    register_user(uid)
    cnt     = stats_data.get("user_downloads", {}).get(str(uid), 0)
    state   = get_state(uid)
    pending = sum(1 for t in state.active_tasks if not t.done())
    name    = user.full_name or user.first_name or "Unknown"
    await update.message.reply_text(
        f"{E['stats']} <b>My Stats</b>\n\n"
        f"👤 <b>Name:</b> {name}\n"
        f"🆔 <b>ID:</b> <code>{uid}</code>\n"
        f"{E['dl']} <b>Total Downloads:</b> {cnt}\n"
        f"{E['queue']} <b>Pending:</b> {pending}",
        parse_mode=ParseMode.HTML)

async def cmd_formats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """NEW in v6.9: admin-only diagnostic — lists every format yt-dlp
    actually sees for a URL (height / format id / extension / filesize),
    so quality-selection issues can be diagnosed from real data instead
    of guessed at from file sizes alone. Usage: /formats <url>"""
    uid = update.effective_user.id
    if uid != ADMIN_ID:
        await update.message.reply_text(f"{E['cross']} Unauthorized.")
        return
    if not context.args:
        await update.message.reply_text(f"{E['info']} Usage: <code>/formats &lt;url&gt;</code>", parse_mode=ParseMode.HTML)
        return

    url = normalize_url(context.args[0])
    msg = await update.message.reply_text(f"{E['search']} Checking available formats…", parse_mode=ParseMode.HTML)
    info, err = await fetch_info(url)
    if not info:
        await safe_edit(msg, f"{E['cross']} Could not fetch info\n<code>{err}</code>")
        return

    formats = info.get("formats") or []
    if not formats:
        await safe_edit(msg, f"{E['warn']} yt-dlp reported NO formats at all for this URL — "
                              f"extraction is failing completely, not just filtering to one quality.")
        return

    lines = [f"{E['gear']} <b>Formats for:</b> {info.get('title','?')[:50]}\n"]
    seen_heights = set()
    for f in formats:
        h      = f.get("height")
        vcodec = f.get("vcodec") or "none"
        acodec = f.get("acodec") or "none"
        ext    = f.get("ext") or "?"
        size   = f.get("filesize") or f.get("filesize_approx")
        size_s = fmt_size(size) if size else "?"
        fid    = f.get("format_id", "?")
        tag    = f"{h}p" if h else ("audio" if vcodec == "none" else "?")
        lines.append(f"• <code>{fid}</code> {tag} {ext} {size_s} v={vcodec} a={acodec}")
        if h:
            seen_heights.add(h)

    heights_str = ", ".join(str(h) + "p" for h in sorted(seen_heights, reverse=True)) or "none found"
    lines.append(f"\n{E['check']} <b>Distinct video heights available:</b> {heights_str}")
    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n<i>…(truncated, too many formats to show)</i>"
    await safe_edit(msg, text)

# ──────────────────────────────────────────────────────────────────
#  MESSAGE HANDLER
#  Menu buttons are matched via resolve_menu_key() so they work no
#  matter which language the user has selected.
#  Admin special modes (broadcast/ban) are handled inside handle_message
#  itself, NOT in a separate group-0 handler that intercepts all admin text.
# ──────────────────────────────────────────────────────────────────
URL_RE = re.compile(r"https?://\S+")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid  = update.effective_user.id
    register_user(uid)
    touch_user(update.effective_user)

    # ── Ban check ─────────────────────────────────────────────────
    if is_banned(uid) and uid != ADMIN_ID:
        await update.message.reply_text(
            "🚫 You have been banned from using this bot.\n"
            "Contact the admin if you think this is a mistake.")
        return

    # ── Admin special modes (broadcast / ban / unban) ─────────────
    # These consume the message ONLY when the admin has an active mode.
    if uid == ADMIN_ID:
        action = context.user_data.get("admin_action")
        if action in ("ban", "unban"):
            context.user_data.pop("admin_action", None)
            if text.startswith("/cancel"):
                await update.message.reply_text(f"{E['check']} Action cancelled.")
                return
            try:
                target_id = int(text.strip())
            except ValueError:
                await update.message.reply_text(f"{E['cross']} Invalid user ID. Please send a numeric ID.")
                return
            if action == "ban":
                ban_user(target_id)
                await update.message.reply_text(
                    f"🔒 User <code>{target_id}</code> has been <b>banned</b>.",
                    parse_mode=ParseMode.HTML)
                try:
                    await context.bot.send_message(target_id,
                        "🚫 You have been banned from using this bot.\n"
                        "Contact the admin if you think this is a mistake.")
                except Exception:
                    pass
            else:
                unban_user(target_id)
                await update.message.reply_text(
                    f"🔓 User <code>{target_id}</code> has been <b>unbanned</b>.",
                    parse_mode=ParseMode.HTML)
                try:
                    await context.bot.send_message(target_id,
                        f"{E['check']} You have been unbanned. Welcome back!")
                except Exception:
                    pass
            return

        if context.user_data.get("broadcast_mode"):
            context.user_data["broadcast_mode"] = False
            if text.startswith("/cancel"):
                await update.message.reply_text(f"{E['check']} Broadcast cancelled.")
                return
            sent = failed = 0
            for u in list(registered_users):
                try:
                    await context.bot.send_message(u,
                        f"{E['bell']} <b>Announcement</b>\n\n{text}", parse_mode=ParseMode.HTML)
                    sent += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
            await update.message.reply_text(
                f"{E['check']} Broadcast done.\nSent: {sent}  |  Failed: {failed}")
            return

    # ── Menu buttons (language-agnostic) ───────────────────────────
    menu_key = resolve_menu_key(text)

    if menu_key == "menu_download":
        context.user_data["waiting_for"] = "url"
        await update.message.reply_text(t("send_url_prompt", uid), parse_mode=ParseMode.HTML)
        return

    if menu_key == "menu_search":
        await update.message.reply_text(
            f"{E['search']} <b>What would you like to search for?</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=search_type_keyboard())
        return

    if menu_key == "menu_settings":
        s = get_state(uid)
        await update.message.reply_text(
            f"{E['gear']} <b>⚙️ Current Settings</b>\n\n"
            f"Subtitles: {'🟢 ON' if s.subtitles else '🔴 OFF'}\n"
            f"Thumbnail: {'🟢 ON' if s.thumbnail else '🔴 OFF'}\n"
            f"Caption:   {'🟢 ON' if s.caption   else '🔴 OFF'}\n\n"
            f"📋 Results per search: <b>{s.search_count}</b>\n"
            f"🌐 Language: <b>{LANGUAGE_LABELS.get(s.language, '🇬🇧 English')}</b>\n\n"
            f"<i>Toggle below or when picking quality after sending a link.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=settings_keyboard(uid))
        return

    if menu_key == "menu_help":
        await cmd_help(update, context)
        return

    if menu_key == "menu_stats":
        await cmd_stats(update, context)
        return

    if menu_key == "menu_language":
        await update.message.reply_text(
            t("choose_language", uid), parse_mode=ParseMode.HTML,
            reply_markup=language_keyboard())
        return

    if menu_key == "menu_admin":
        if uid != ADMIN_ID:
            await update.message.reply_text(f"{E['cross']} Unauthorized.")
            return
        await update.message.reply_text(
            f"{E['admin']} <b>Admin Dashboard</b>\n\n"
            f"Select an action:",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_keyboard())
        return

    # ── URL or search ─────────────────────────────────────────────
    # Always handle URLs for everyone
    m = URL_RE.search(text)
    if m:
        context.user_data.pop("waiting_for", None)
        await process_url(update, context, m.group(0))
        return

    # For non-admin users: always fall through to search
    # For admin: only search if they pressed Search button or are in search/url mode
    waiting = context.user_data.pop("waiting_for", None)
    if uid != ADMIN_ID or waiting in ("search", "url"):
        state = get_state(uid)
        mode  = state.search_mode
        state.search_mode = "single"  # reset so a future plain search doesn't stay in playlist mode
        if mode == "playlist" and waiting == "search":
            await handle_playlist_search(update, context, text)
        else:
            await handle_search(update, context, text)
    # else: admin typed something random with no mode — silently ignore

async def process_url(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str):
    uid       = update.effective_user.id
    state     = get_state(uid)
    clean_url = normalize_url(url)
    fetching  = await update.message.reply_text(
        f"{E['search']} Fetching link info…", parse_mode=ParseMode.HTML)

    if is_playlist_url(clean_url):
        pl_info, pl_err = await fetch_playlist_info(clean_url)
        if pl_info and pl_info.get("_type") == "playlist" and pl_info.get("entries"):
            entries = [e for e in pl_info["entries"] if e]
            if entries:
                state.playlist_entries = entries
                state.playlist_title   = pl_info.get("title") or "Playlist"
                state.playlist_url     = clean_url
                task_id    = new_task_id()
                count      = len(entries)
                has_single = has_single_video(clean_url)
                note = f" (first {PLAYLIST_MAX_ITEMS})" if count >= PLAYLIST_MAX_ITEMS else ""
                await safe_edit(fetching,
                    f"{E['playlist']} <b>Playlist Detected</b>\n\n"
                    f"<b>{state.playlist_title}</b>\n"
                    f"{E['queue']} {count} videos found{note}\n\n"
                    f"What would you like to do?",
                    reply_markup=playlist_choice_keyboard(task_id, has_single))
                return

        if not has_single_video(clean_url):
            await safe_edit(fetching,
                f"{E['cross']} <b>Could not load playlist</b>\n\n"
                f"<code>{pl_err or 'No videos found or playlist is private/unavailable.'}</code>\n\n"
                f"<i>Try: pip install -U yt-dlp</i>")
            return

        clean_url = strip_playlist(clean_url)

    info, err = await fetch_info(clean_url)
    if not info:
        await safe_edit(fetching,
            f"{E['cross']} <b>Could not fetch media info</b>\n\n"
            f"{err}\n\n"
            f"<i>Try: pip install -U yt-dlp</i>")
        return

    state.pending_url  = clean_url
    state.pending_info = info

    # ── NEW in v6.0: detect image / slideshow posts (e.g. TikTok photo mode) ──
    image_urls = extract_images(info)
    if image_urls and not info.get("duration"):
        task_id = new_task_id()
        title   = info.get("title") or "Photo Post"
        preview = (
            f"╔══ {E['thumb']} <b>IMAGE POST DETECTED</b> ══╗\n\n"
            f"<b>{title}</b>\n\n"
            f"🖼️ {len(image_urls)} image(s) found\n\n"
            f"╚══ Tap below to download as an album ══╝"
        )
        await safe_edit(fetching, preview, reply_markup=image_download_keyboard(task_id, len(image_urls)))
        return

    title    = info.get("title", "Unknown")
    uploader = info.get("uploader") or info.get("channel") or "Unknown"
    dur_str  = fmt_eta(info.get("duration"))
    views    = info.get("view_count")
    view_str = f"{views:,}" if views else "N/A"
    ext      = (info.get("ext") or "?").upper()
    thumb    = info.get("thumbnail", "")
    task_id  = new_task_id()

    preview = (
        f"╔══ {E['film']} <b>LINK PREVIEW</b> ══╗\n\n"
        f"<b>{title}</b>\n\n"
        f"{E['user']} <b>Uploader:</b> {uploader}\n"
        f"{E['time']} <b>Duration:</b> {dur_str}\n"
        f"{E['globe']} <b>Views:</b> {view_str}\n"
        f"{E['gear']} <b>Format:</b> {ext}\n\n"
        f"╚══ Select Quality Below ══╝"
    )
    kb = quality_keyboard(task_id, uid)
    if thumb:
        try:
            await update.message.reply_photo(photo=thumb, caption=preview,
                parse_mode=ParseMode.HTML, reply_markup=kb)
            await safe_delete(fetching)
            return
        except Exception:
            pass
    await safe_edit(fetching, preview, reply_markup=kb)

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    if not query:
        return
    state = get_state(update.effective_user.id)
    n     = state.search_count
    msg   = await update.message.reply_text(
        f"{E['search']} Searching: <b>{query}</b>…\n<i>Fetching {n} results…</i>",
        parse_mode=ParseMode.HTML)
    results = await search_youtube(query, n=n)
    if not results:
        await safe_edit(msg, f"{E['cross']} No results found. Try a different query.")
        return
    state.search_results = results
    state.search_page    = 0
    lines = [f"{E['search']} <b>YouTube Results</b> ({len(results)} found)\n"]
    for i, r in enumerate(results[:SEARCH_PAGE_SIZE], 1):
        dur = fmt_eta(r.get("duration")) if r.get("duration") else "?"
        lines.append(f"{i}. <b>{(r.get('title') or '?')[:45]}</b>  [{dur}]")
    await safe_edit(msg, "\n".join(lines), reply_markup=search_result_keyboard(results, page=0))

async def handle_playlist_search(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """NEW in v6.7: search specifically for playlists/albums instead of
    individual videos."""
    if not query:
        return
    uid   = update.effective_user.id
    state = get_state(uid)
    n     = state.search_count
    msg   = await update.message.reply_text(
        f"{E['playlist']} Searching playlists/albums: <b>{query}</b>…\n<i>Fetching results…</i>",
        parse_mode=ParseMode.HTML)
    results = await search_youtube_playlists(query, n=n)
    if not results:
        await safe_edit(msg,
            f"{E['cross']} No playlists/albums found for that query.\n"
            f"<i>Try different wording, or update yt-dlp: pip install -U yt-dlp</i>")
        return
    state.pl_search_results = results
    state.pl_search_page    = 0
    lines = [f"{E['playlist']} <b>Playlist/Album Results</b> ({len(results)} found)\n"]
    for i, r in enumerate(results[:SEARCH_PAGE_SIZE], 1):
        lines.append(f"{i}. {E['playlist']} <b>{(r.get('title') or 'Playlist')[:45]}</b>")
    await safe_edit(msg, "\n".join(lines), reply_markup=playlist_search_result_keyboard(results, page=0))

async def send_single_video_preview(bot: Bot, chat_id: int, uid: int, url: str) -> None:
    """NEW in v6.7: shared "fetch info -> show quality picker (or image-post
    picker)" flow, used when a user picks an individual video while browsing
    inside a playlist/album. Always sends a NEW message so whatever list
    they were browsing stays visible underneath."""
    state    = get_state(uid)
    fetching = await bot.send_message(chat_id, f"{E['search']} Fetching video info…", parse_mode=ParseMode.HTML)
    info, err = await fetch_info(url)
    if not info:
        await safe_edit(fetching, f"{E['cross']} Could not fetch info\n<code>{err}</code>")
        return
    state.pending_url  = url
    state.pending_info = info

    image_urls = extract_images(info)
    if image_urls and not info.get("duration"):
        task_id = new_task_id()
        title   = info.get("title") or "Photo Post"
        preview = (
            f"╔══ {E['thumb']} <b>IMAGE POST DETECTED</b> ══╗\n\n"
            f"<b>{title}</b>\n\n"
            f"🖼️ {len(image_urls)} image(s) found\n\n"
            f"╚══ Tap below to download as an album ══╝"
        )
        await safe_edit(fetching, preview, reply_markup=image_download_keyboard(task_id, len(image_urls)))
        return

    task_id = new_task_id()
    await safe_edit(fetching,
        f"{E['film']} <b>{info.get('title','Unknown')[:60]}</b>\n\nChoose quality:",
        reply_markup=quality_keyboard(task_id, uid))

# ──────────────────────────────────────────────────────────────────
#  CALLBACK HANDLER
# ──────────────────────────────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    d   = q.data or ""
    await q.answer()

    # ── Search type chooser: Single vs Playlist/Album (NEW in v6.7) ──
    if d.startswith("searchmode|"):
        mode  = d.split("|", 1)[1]
        state = get_state(uid)
        state.search_mode = mode if mode in ("single", "playlist") else "single"
        context.user_data["waiting_for"] = "search"
        if state.search_mode == "playlist":
            await safe_edit(q.message, f"{E['playlist']} Type your playlist/album search query:")
        else:
            await safe_edit(q.message, t("send_search_prompt", uid))
        return

    # ── Playlist/album search: result picked ─────────────────────────
    if d.startswith("plsearch_pick|"):
        state = get_state(uid)
        try:
            idx = int(d.split("|", 1)[1])
        except ValueError:
            return
        if idx < 0 or idx >= len(state.pl_search_results):
            await q.answer("This result has expired — please search again.", show_alert=True)
            return
        r = state.pl_search_results[idx]
        pl_url = r.get("url") or r.get("webpage_url")
        if not pl_url and r.get("id"):
            pl_url = f"https://www.youtube.com/playlist?list={r['id']}"
        if not pl_url:
            await q.answer("Couldn't resolve this playlist's link.", show_alert=True)
            return

        fetching = await context.bot.send_message(
            q.message.chat_id, f"{E['search']} Fetching playlist info…", parse_mode=ParseMode.HTML)
        pl_info, pl_err = await fetch_playlist_info(pl_url)
        entries = [e for e in (pl_info.get("entries") if pl_info else []) if e]
        if not entries:
            await safe_edit(fetching,
                f"{E['cross']} Could not load this playlist.\n"
                f"<code>{pl_err or 'No videos found, or it is private/unavailable.'}</code>")
            return

        state.playlist_entries = entries
        state.playlist_title   = pl_info.get("title") or r.get("title") or "Playlist"
        state.playlist_url     = pl_url
        state.pl_browse_page   = 0
        task_id = new_task_id()
        note = f" (first {PLAYLIST_MAX_ITEMS})" if len(entries) >= PLAYLIST_MAX_ITEMS else ""
        await safe_edit(fetching,
            f"{E['playlist']} <b>{state.playlist_title}</b>\n"
            f"{E['queue']} {len(entries)} videos found{note}\n\n"
            f"What would you like to do?",
            reply_markup=playlist_action_keyboard(task_id))
        return

    # ── Playlist/album search: pagination ────────────────────────────
    if d.startswith("plsearch_page|"):
        page  = int(d.split("|")[1])
        state = get_state(uid)
        results = state.pl_search_results
        if not results:
            await q.answer("Search expired. Please search again.")
            return
        state.pl_search_page = page
        start = page * SEARCH_PAGE_SIZE
        lines = [f"{E['playlist']} <b>Playlist/Album Results</b> ({len(results)} found)\n"]
        for i, r in enumerate(results[start:start + SEARCH_PAGE_SIZE], start + 1):
            lines.append(f"{i}. {E['playlist']} <b>{(r.get('title') or 'Playlist')[:45]}</b>")
        try:
            await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML,
                reply_markup=playlist_search_result_keyboard(results, page=page))
        except BadRequest:
            pass
        return

    # ── Playlist: browse individual videos instead of the whole thing ──
    if d.startswith("pl_browse|"):
        state = get_state(uid)
        if not state.playlist_entries:
            await safe_edit(q.message, f"{E['warn']} Session expired. Please search again.")
            return
        state.pl_browse_page = 0
        await safe_edit(q.message,
            f"{E['playlist']} <b>{state.playlist_title}</b>\n\n"
            f"Pick a video to download individually:",
            reply_markup=playlist_browse_keyboard(state.playlist_entries, page=0))
        return

    if d.startswith("pl_browse_page|"):
        page  = int(d.split("|")[1])
        state = get_state(uid)
        if not state.playlist_entries:
            await q.answer("Session expired.")
            return
        state.pl_browse_page = page
        try:
            await q.edit_message_text(
                f"{E['playlist']} <b>{state.playlist_title}</b>\n\n"
                f"Pick a video to download individually:",
                parse_mode=ParseMode.HTML,
                reply_markup=playlist_browse_keyboard(state.playlist_entries, page=page))
        except BadRequest:
            pass
        return

    # ── Playlist: individual video picked while browsing ─────────────
    if d.startswith("pl_item_pick|"):
        state = get_state(uid)
        try:
            idx = int(d.split("|", 1)[1])
        except ValueError:
            return
        if idx < 0 or idx >= len(state.playlist_entries):
            await q.answer("This item has expired — browse again.", show_alert=True)
            return
        entry = state.playlist_entries[idx]
        vid_url = (entry.get("url") or entry.get("webpage_url") or
                   (f"https://www.youtube.com/watch?v={entry['id']}" if entry.get("id") else None))
        if not vid_url:
            await q.answer("Couldn't resolve this video's link.", show_alert=True)
            return
        vid_url = normalize_url(vid_url)
        await q.answer()
        await send_single_video_preview(context.bot, q.message.chat_id, uid, vid_url)
        return

    # ── Post-download action buttons (NEW in v6.6) ──────────────────
    if d.startswith("pact|"):
        parts = d.split("|")
        if len(parts) < 3:
            return
        action, task_id = parts[1], parts[2]
        rec = completed_media.get(task_id)
        if not rec:
            await q.answer("Session expired — please redownload.", show_alert=True)
            return

        if action == "rmbtn":
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except BadRequest:
                pass
            return

        if action == "thumb":
            if rec.get("thumbnail"):
                await context.bot.send_photo(q.message.chat_id, photo=rec["thumbnail"],
                    caption=f"{E['thumb']} <b>{rec.get('title','')[:60]}</b>", parse_mode=ParseMode.HTML)
            else:
                await q.answer("No thumbnail available for this item.", show_alert=True)
            return

        if action == "sub":
            await q.answer("Fetching subtitles…")
            await send_subtitle_on_demand(context.bot, q.message.chat_id, rec["url"], rec.get("title", "media"))
            return

        if action == "gray":
            await q.answer("Starting grayscale conversion…")
            await run_grayscale(context.bot, q.message.chat_id, uid, rec)
            return

        if action == "swap":
            await q.answer("Starting…")
            want_audio = not rec.get("is_audio", False)
            qk = "audio" if want_audio else (rec.get("quality_key") if rec.get("quality_key") != "audio" else "best")
            status = await context.bot.send_message(
                q.message.chat_id,
                f"{E['rocket']} Preparing {'audio' if want_audio else 'video'} version…",
                parse_mode=ParseMode.HTML)
            info, err = await fetch_info(rec["url"])
            if not info:
                await safe_edit(status, f"{E['cross']} Could not fetch info\n<code>{err}</code>")
                return
            await enqueue_download(context.bot, q.message.chat_id, uid, rec["url"], qk, info, status)
            return

    # ── Language change (NEW in v6.0) ──────────────────────────────
    if d.startswith("lang|"):
        code  = d.split("|", 1)[1]
        state = get_state(uid)
        if code in LANGUAGE_LABELS:
            state.language = code
        label = LANGUAGE_LABELS.get(code, code)
        await safe_edit(q.message, t("language_set", uid, lang=label))
        try:
            await context.bot.send_message(
                q.message.chat_id, t("intro", uid),
                parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard(uid))
        except Exception:
            pass
        return

    # ── Image / slideshow download (NEW in v6.0) ───────────────────
    if d.startswith("img|"):
        task_id = d.split("|", 1)[1]
        state   = get_state(uid)
        url, info = state.pending_url, state.pending_info
        if not url or not info:
            await safe_edit(q.message, f"{E['warn']} Session expired. Resend the link.")
            return
        await safe_edit(q.message, f"{E['rocket']} <b>Starting image download…</b>", reply_markup=None)
        await run_image_download(context.bot, q.message.chat_id, uid, url, info, q.message, task_id)
        return

    # ── Single download ───────────────────────────────────────────
    if d.startswith("dl|"):
        parts = d.split("|")
        if len(parts) < 3: return
        task_id, qk = parts[1], parts[2]
        state = get_state(uid)
        now   = time.monotonic()
        if now - state.last_req_time < DOWNLOAD_COOLDOWN_SECS:
            await q.answer("⏳ Please wait a moment.", show_alert=False)
            return
        state.last_req_time = now
        url, info = state.pending_url, state.pending_info
        if not url or not info:
            await safe_edit(q.message, f"{E['warn']} Session expired. Resend the link.")
            return
        await safe_edit(q.message, f"{E['rocket']} <b>Queuing download…</b>", reply_markup=None)
        await enqueue_download(context.bot, q.message.chat_id, uid, url, qk, info, q.message)
        await notify_admin(context.bot,
            f"{E['dl']} Download started\nUser: <code>{uid}</code>\n"
            f"Quality: {qk}\nTitle: {info.get('title','?')[:50]}")
        return

    # ── Retry ─────────────────────────────────────────────────────
    if d.startswith("retry|"):
        qk    = d.split("|", 1)[1]
        state = get_state(uid)
        now   = time.monotonic()
        if now - state.last_req_time < DOWNLOAD_COOLDOWN_SECS:
            await q.answer("⏳ Please wait.", show_alert=False)
            return
        state.last_req_time = now
        url, info = state.pending_url, state.pending_info
        if not url or not info:
            await safe_edit(q.message, f"{E['warn']} Session expired. Resend the link.")
            return
        await safe_edit(q.message, f"{E['rocket']} <b>Retrying…</b>", reply_markup=None)
        await enqueue_download(context.bot, q.message.chat_id, uid, url, qk, info, q.message)
        return

    # ── Toggle settings ───────────────────────────────────────────
    if d.startswith("tog|"):
        parts = d.split("|")
        if len(parts) < 3: return
        setting, task_id = parts[1], parts[2]
        state = get_state(uid)
        if setting == "sub":   state.subtitles = not state.subtitles
        elif setting == "thumb": state.thumbnail = not state.thumbnail
        elif setting == "cap":   state.caption   = not state.caption
        is_pl = any(
            btn.callback_data and btn.callback_data.startswith("pldl|")
            for row in (q.message.reply_markup.inline_keyboard if q.message.reply_markup else [])
            for btn in row
        )
        try:
            kb = playlist_quality_keyboard(task_id, uid) if is_pl else quality_keyboard(task_id, uid)
            await q.edit_message_reply_markup(reply_markup=kb)
        except BadRequest:
            pass
        return

    # ── Cancel single download ────────────────────────────────────
    if d.startswith("cancel|"):
        tid   = d.split("|", 1)[1]
        state = get_state(uid)
        task  = state.active_procs.get(tid)
        if task and not task.done():
            task.cancel()
            await safe_edit(q.message, f"{E['cancel']} Cancelling…")
        else:
            await safe_edit(q.message, f"{E['warn']} No active download to cancel.")
        return

    # ── Playlist: whole playlist ──────────────────────────────────
    if d.startswith("pl_all|"):
        task_id = d.split("|", 1)[1]
        state   = get_state(uid)
        if not state.playlist_entries:
            await safe_edit(q.message, f"{E['warn']} Session expired. Resend the link.")
            return
        await safe_edit(q.message,
            f"{E['playlist']} <b>{state.playlist_title}</b>\n"
            f"{E['queue']} {len(state.playlist_entries)} videos\n\n"
            f"Choose quality for <b>ALL</b> videos:",
            reply_markup=playlist_quality_keyboard(task_id, uid))
        return

    # ── Playlist: just this video ─────────────────────────────────
    if d.startswith("pl_single|"):
        state      = get_state(uid)
        single_url = strip_playlist(state.playlist_url)
        await safe_edit(q.message, f"{E['search']} Fetching video info…")
        info, err = await fetch_info(single_url)
        if not info:
            await safe_edit(q.message, f"{E['cross']} Could not fetch info\n<code>{err}</code>")
            return
        state.pending_url  = single_url
        state.pending_info = info
        task_id  = new_task_id()
        title    = info.get("title", "Unknown")
        uploader = info.get("uploader") or info.get("channel") or "Unknown"
        dur_str  = fmt_eta(info.get("duration"))
        ext      = (info.get("ext") or "?").upper()
        preview  = (
            f"╔══ {E['film']} <b>LINK PREVIEW</b> ══╗\n\n<b>{title}</b>\n\n"
            f"{E['user']} <b>Uploader:</b> {uploader}\n"
            f"{E['time']} <b>Duration:</b> {dur_str}\n"
            f"{E['gear']} <b>Format:</b> {ext}\n\n╚══ Select Quality Below ══╝"
        )
        await safe_edit(q.message, preview, reply_markup=quality_keyboard(task_id, uid))
        return

    # ── Playlist: quality chosen → start ─────────────────────────
    if d.startswith("pldl|"):
        parts = d.split("|")
        if len(parts) < 3: return
        task_id, qk = parts[1], parts[2]
        state = get_state(uid)
        now   = time.monotonic()
        if now - state.last_req_time < DOWNLOAD_COOLDOWN_SECS:
            await q.answer("⏳ Please wait.", show_alert=False)
            return
        state.last_req_time = now
        entries, pl_title = state.playlist_entries, state.playlist_title
        if not entries:
            await safe_edit(q.message, f"{E['warn']} Session expired. Resend the link.")
            return
        await safe_edit(q.message,
            f"{E['playlist']} <b>{pl_title}</b>\n\n"
            f"{E['rocket']} Starting playlist ({len(entries)} items)…",
            reply_markup=playlist_cancel_keyboard(task_id))
        state.cancel_flags[task_id] = False
        task = asyncio.create_task(
            run_playlist(context.bot, q.message.chat_id, uid,
                         entries, qk, q.message, pl_title, task_id))
        state.active_tasks = [t for t in state.active_tasks if not t.done()]
        state.active_tasks.append(task)
        state.active_procs[task_id] = task
        await notify_admin(context.bot,
            f"{E['playlist']} Playlist started\nUser: <code>{uid}</code>\n"
            f"Playlist: {pl_title[:50]}\nItems: {len(entries)}\nQuality: {qk}")
        return

    # ── Playlist stop ─────────────────────────────────────────────
    if d.startswith("plcancel|"):
        tid = d.split("|", 1)[1]
        get_state(uid).cancel_flags[tid] = True
        await q.answer("Stopping after the current item finishes…")
        return

    # ── Search pagination ─────────────────────────────────────────
    if d.startswith("search_page|"):
        page    = int(d.split("|")[1])
        state   = get_state(uid)
        results = state.search_results
        if not results:
            await q.answer("Search expired. Please search again.")
            return
        state.search_page = page
        start = page * SEARCH_PAGE_SIZE
        lines = [f"{E['search']} <b>YouTube Results</b> ({len(results)} found)\n"]
        for i, r in enumerate(results[start:start+SEARCH_PAGE_SIZE], start+1):
            dur = fmt_eta(r.get("duration")) if r.get("duration") else "?"
            lines.append(f"{i}. <b>{(r.get('title') or '?')[:45]}</b>  [{dur}]")
        try:
            await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML,
                reply_markup=search_result_keyboard(results, page=page))
        except BadRequest:
            pass
        return

    if d == "noop":
        return

    # ── Search result picked ──────────────────────────────────────
    if d.startswith("search_pick|"):
        url   = d[len("search_pick|"):]
        state = get_state(uid)
        state.pending_url = url
        # NEW in v6.6: don't edit the results-list message (q.message) — send
        # a separate message for fetching/preview so the search results stay
        # visible and scrollable/reusable underneath.
        fetching = await context.bot.send_message(
            q.message.chat_id, f"{E['search']} Fetching video info…", parse_mode=ParseMode.HTML)
        info, err = await fetch_info(url)
        if not info:
            await safe_edit(fetching, f"{E['cross']} Could not fetch info\n<code>{err}</code>")
            return
        state.pending_info = info

        # ── NEW in v6.0: image posts can also come from search picks ──
        image_urls = extract_images(info)
        if image_urls and not info.get("duration"):
            task_id = new_task_id()
            title   = info.get("title") or "Photo Post"
            preview = (
                f"╔══ {E['thumb']} <b>IMAGE POST DETECTED</b> ══╗\n\n"
                f"<b>{title}</b>\n\n"
                f"🖼️ {len(image_urls)} image(s) found\n\n"
                f"╚══ Tap below to download as an album ══╝"
            )
            await safe_edit(fetching, preview, reply_markup=image_download_keyboard(task_id, len(image_urls)))
            return

        task_id = new_task_id()
        await safe_edit(fetching,
            f"{E['film']} <b>{info.get('title','Unknown')[:60]}</b>\n\nChoose quality:",
            reply_markup=quality_keyboard(task_id, uid))
        return

    if d == "search_cancel":
        await safe_edit(q.message, f"{E['cross']} Cancelled.")
        return

    # ── Settings callbacks ────────────────────────────────────────
    if d.startswith("settings|"):
        action = d[len("settings|"):]
        state  = get_state(uid)

        def _settings_text() -> str:
            return (
                f"{E['gear']} <b>⚙️ Current Settings</b>\n\n"
                f"Subtitles: {'🟢 ON' if state.subtitles else '🔴 OFF'}\n"
                f"Thumbnail: {'🟢 ON' if state.thumbnail else '🔴 OFF'}\n"
                f"Caption:   {'🟢 ON' if state.caption   else '🔴 OFF'}\n\n"
                f"📋 Results per search: <b>{state.search_count}</b>\n"
                f"🌐 Language: <b>{LANGUAGE_LABELS.get(state.language, '🇬🇧 English')}</b>\n\n"
                f"<i>Toggle below or when picking quality after sending a link.</i>"
            )

        if action == "tog_sub":
            state.subtitles = not state.subtitles
            await q.edit_message_text(_settings_text(), parse_mode=ParseMode.HTML,
                                       reply_markup=settings_keyboard(uid))
            return

        if action == "tog_thumb":
            state.thumbnail = not state.thumbnail
            await q.edit_message_text(_settings_text(), parse_mode=ParseMode.HTML,
                                       reply_markup=settings_keyboard(uid))
            return

        if action == "tog_cap":
            state.caption = not state.caption
            await q.edit_message_text(_settings_text(), parse_mode=ParseMode.HTML,
                                       reply_markup=settings_keyboard(uid))
            return

        if action == "count_menu":
            await q.edit_message_text(
                f"{E['gear']} <b>Results per Search</b>\n\n"
                f"Current: <b>{state.search_count}</b>\n"
                f"Choose a value (min 10, max 100):",
                parse_mode=ParseMode.HTML,
                reply_markup=search_count_keyboard())
            return

        if action.startswith("set_count|"):
            n = int(action.split("|", 1)[1])
            if 10 <= n <= 100:
                state.search_count = n
            await q.edit_message_text(_settings_text(), parse_mode=ParseMode.HTML,
                                       reply_markup=settings_keyboard(uid))
            return

        # ── NEW in v6.0: language picker reachable from Settings ──
        if action == "language":
            await q.edit_message_text(t("choose_language", uid), parse_mode=ParseMode.HTML,
                                       reply_markup=language_keyboard())
            return

        if action == "back":
            await q.edit_message_text(_settings_text(), parse_mode=ParseMode.HTML,
                                       reply_markup=settings_keyboard(uid))
            return

        if action == "close":
            await safe_edit(q.message,
                f"{E['check']} <b>Settings saved!</b>\n\n"
                f"Subtitles: {'🟢 ON' if state.subtitles else '🔴 OFF'}\n"
                f"Thumbnail: {'🟢 ON' if state.thumbnail else '🔴 OFF'}\n"
                f"Caption:   {'🟢 ON' if state.caption   else '🔴 OFF'}\n"
                f"📋 {state.search_count} results per search\n"
                f"🌐 {LANGUAGE_LABELS.get(state.language, '🇬🇧 English')}")
            return
        return

    # ── Admin callbacks ───────────────────────────────────────────
    if d.startswith("admin|") and uid == ADMIN_ID:
        action = d.split("|")[1]

        if action == "stats":
            q_size = global_queue.qsize() if global_queue else 0
            active = sum(len([t for t in s.active_tasks if not t.done()]) for s in user_states.values())
            await safe_edit(q.message,
                f"{E['admin']} <b>Statistics</b>\n\n"
                f"👥 Registered users: <b>{len(registered_users)}</b>\n"
                f"🚫 Banned users:     <b>{len(banned_users)}</b>\n"
                f"{E['dl']} Total downloads:  <b>{stats_data.get('total_downloads',0)}</b>\n"
                f"{E['queue']} Queue pending:   <b>{q_size}</b>\n"
                f"{E['fire']} Active downloads: <b>{active}</b>",
                reply_markup=admin_keyboard())

        elif action == "userlist":
            # ── UPGRADED in v6.0: username, ID, message count, last-seen, last download ──
            uids  = list(registered_users)
            lines = [f"👥 <b>User List</b> ({len(uids)} total)\n"]
            for i, u in enumerate(uids[:30], 1):
                rec        = user_info.get(str(u), {})
                uname      = rec.get("username")
                fname      = rec.get("first_name") or "Unknown"
                display    = f"@{uname}" if uname else fname
                msg_cnt    = rec.get("msg_count", 0)
                last_seen  = rec.get("last_seen") or "—"
                banned_tag = " 🚫" if u in banned_users else ""
                lines.append(f"{i}. <b>{display}</b> (<code>{u}</code>){banned_tag}")
                lines.append(f"   💬 {msg_cnt} msgs | {last_seen}")
                downloads = rec.get("downloads", [])
                if downloads:
                    last_dl = downloads[-1]
                    lines.append(f"   🎬 Last: {last_dl.get('title','?')}")
                lines.append("")
            if len(uids) > 30:
                lines.append(f"<i>…and {len(uids)-30} more</i>")
            await safe_edit(q.message, "\n".join(lines), reply_markup=admin_keyboard())

        elif action == "clearcache":
            removed = 0
            try:
                for item in DOWNLOAD_DIR.iterdir():
                    if item.is_dir():
                        shutil.rmtree(item, ignore_errors=True)
                        removed += 1
                    elif item.is_file():
                        item.unlink(missing_ok=True)
                        removed += 1
            except Exception as ex:
                logger.warning(f"clearcache: {ex}")
            await safe_edit(q.message,
                f"🧹 <b>Cache Cleared</b>\n\nRemoved <b>{removed}</b> item(s).",
                reply_markup=admin_keyboard())

        elif action == "broadcast":
            context.user_data["broadcast_mode"] = True
            await safe_edit(q.message,
                f"{E['bell']} <b>Broadcast Mode Active</b>\n\n"
                f"Send your next message — it will go to all "
                f"<b>{len(registered_users)}</b> users.\n\nSend /cancel to abort.")

        elif action == "ban":
            context.user_data["admin_action"] = "ban"
            await safe_edit(q.message,
                f"🔒 <b>Ban User</b>\n\nReply with the user ID to ban.\nSend /cancel to abort.")

        elif action == "unban":
            context.user_data["admin_action"] = "unban"
            await safe_edit(q.message,
                f"🔓 <b>Unban User</b>\n\nReply with the user ID to unban.\nSend /cancel to abort.")
        return

# ──────────────────────────────────────────────────────────────────
#  ERROR HANDLER
# ──────────────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    exc = str(context.error).lower()
    if "timed out" in exc or "timeout" in exc:
        logger.warning(f"Timeout suppressed: {context.error}")
        return
    logger.error(f"Unhandled error: {context.error}", exc_info=True)
    try:
        if update and isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(f"{E['warn']} An error occurred. Please try again.")
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────
#  POST-INIT
# ──────────────────────────────────────────────────────────────────
async def post_init(app: Application):
    global global_queue
    global_queue = asyncio.Queue()
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    load_users()
    load_stats()
    load_user_info()
    asyncio.create_task(queue_worker())

    try:
        import importlib.metadata
        ver = importlib.metadata.version("yt-dlp")
        logger.info(f"yt-dlp version: {ver}")
    except Exception:
        ver = "unknown"

    cookie_status = "✅ found" if COOKIES_FILE.exists() else "❌ not found (place cookies.txt here to fix login errors)"
    logger.info(f"🍪 cookies.txt: {cookie_status}")
    logger.info("✅ NEXUS Bot v6.7 is live!")

    await notify_admin(app.bot,
        f"{E['rocket']} NEXUS Bot <b>v6.0</b> started\n"
        f"Users: <b>{len(registered_users)}</b>  |  "
        f"Downloads: <b>{stats_data.get('total_downloads',0)}</b>\n"
        f"yt-dlp: <code>{ver}</code>\n"
        f"🍪 cookies.txt: {'✅ found' if COOKIES_FILE.exists() else '❌ not found'}")

# ──────────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────────
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

def _run_health_server():
    """Minimal HTTP server so Render's free Web Service health check passes.
    Render requires something listening on $PORT; this just says OK."""
    port = int(os.environ.get("PORT", "10000"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"NEXUS bot is running")
        def log_message(self, format, *args):
            pass  # silence noisy request logs

    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

def local_bot_api_available(base_url: str, timeout: float = 3.0) -> bool:
    """NEW in v6.8: quick reachability check for a self-hosted Bot API
    server before committing to use it — any successful connection (even
    a 404) means the server process is up; a connection error means it
    isn't, and we should fall back to Telegram's cloud API instead."""
    try:
        import urllib.request
        import urllib.error
        urllib.request.urlopen(base_url, timeout=timeout)
        return True
    except urllib.error.HTTPError:
        return True  # server responded — that's enough to prove it's up
    except Exception as e:
        logger.warning(f"local_bot_api_available: {base_url} not reachable ({e})")
        return False

def main():
    global TELEGRAM_SEND_LIMIT_MB
    threading.Thread(target=_run_health_server, daemon=True).start()

    builder = Application.builder().token(BOT_TOKEN).post_init(post_init)

    if LOCAL_BOT_API_URL:
        if local_bot_api_available(LOCAL_BOT_API_URL):
            builder = builder.base_url(f"{LOCAL_BOT_API_URL}/bot") \
                              .base_file_url(f"{LOCAL_BOT_API_URL}/file/bot")
            TELEGRAM_SEND_LIMIT_MB = MAX_FILE_SIZE_MB  # local server allows up to 2000MB
            logger.info(f"✅ Using local Bot API server at {LOCAL_BOT_API_URL} "
                        f"— send limit raised to {TELEGRAM_SEND_LIMIT_MB}MB, "
                        f"fallback-link path will rarely be needed.")
        else:
            logger.warning(f"⚠️ LOCAL_BOT_API_URL is set to {LOCAL_BOT_API_URL} but not "
                            f"reachable — falling back to Telegram's cloud API "
                            f"(50MB send limit; large files will use the fallback link).")

    app = builder.build()

    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("help",     cmd_help))
    app.add_handler(CommandHandler("cancel",   cmd_cancel_all))
    app.add_handler(CommandHandler("stats",    cmd_stats))
    app.add_handler(CommandHandler("language", cmd_language))
    app.add_handler(CommandHandler("formats",  cmd_formats))
    # Single message handler for everyone — admin modes handled inside
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_error_handler(error_handler)

    logger.info("🚀 Starting NEXUS Bot v7.0 — polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
