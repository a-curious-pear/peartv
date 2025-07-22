#!/usr/bin/env python3
"""
Smart EPG Generator with:
- Priority matching (tvg-id first, then tvg-name)
- Case-insensitive matching with spaces
- Auto-translation of non-English content
- Configurable timezone handling
- Blacklist support
"""

import os
import re
import requests
from datetime import datetime, timedelta, timezone
from xml.sax import make_parser, handler
from xml.sax.saxutils import escape
import gzip
import io
from collections import defaultdict
from difflib import SequenceMatcher
from googletrans import Translator

# Configuration
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCE = "https://epg.pw/xmltv/epg.xml.gz?lang=en&timezone=QXNpYS9LYXJhY2hp"
OUTPUT_FILE = "custom_epg.xml"
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming
MAX_RETRIES = 3

# Settings (1 = ON, 0 = OFF)
ENABLE_TIMEZONE_CORRECTION = 1  # GMT+5 conversion
ENABLE_TRANSLATION = 1  # Auto-translation
GMT5 = timezone(timedelta(hours=5)) if ENABLE_TIMEZONE_CORRECTION else timezone.utc

# Channel blacklist
BLACKLIST = {'SSTV', 'LOCAL1', 'LOCAL2'}

# Initialize translator
translator = Translator()

class EPGHandler(handler.ContentHandler):
    """SAX handler with language detection"""
    def __init__(self):
        self.channels = {}
        self.programs = defaultdict(list)
        self.current_channel = None
        self.current_program = None
        self.current_element = None
        self.current_text = ""

    def startElement(self, name, attrs):
        self.current_element = name
        if name == "channel":
            self.current_channel = attrs.get("id", "").lower()
            if self.current_channel in BLACKLIST:
                self.current_channel = None
                return
            self.channels[self.current_channel] = {
                'display_names': [],
                'original_id': attrs.get("id", ""),
                'lang': 'en'
            }
        elif name == "programme":
            channel = attrs.get("channel", "").lower()
            if channel in BLACKLIST:
                return
            self.current_program = {
                'channel': channel,
                'start': self.parse_time(attrs.get("start", "")),
                'stop': self.parse_time(attrs.get("stop", "")),
                'title': "",
                'desc': "",
                'lang': 'en'
            }

    def characters(self, content):
        if self.current_element in ['display-name', 'title', 'desc']:
            self.current_text += content

    def endElement(self, name):
        if name == "display-name" and self.current_channel:
            text = self.current_text.strip()
            self.channels[self.current_channel]['display_names'].append(text)
            if ENABLE_TRANSLATION and text:
                try:
                    lang = translator.detect(text).lang
                    self.channels[self.current_channel]['lang'] = lang
                except:
                    pass
        elif name == "title" and self.current_program:
            self.current_program['title'] = self.current_text.strip()
        elif name == "desc" and self.current_program:
            self.current_program['desc'] = self.current_text.strip()
        elif name == "programme" and self.current_program:
            if ENABLE_TRANSLATION and self.current_program['title']:
                try:
                    lang = translator.detect(self.current_program['title']).lang
                    self.current_program['lang'] = lang
                except:
                    pass
            self.programs[self.current_program['channel']].append(self.current_program)
            self.current_program = None
        elif name == "channel":
            self.current_channel = None
        self.current_text = ""
        self.current_element = None

    def parse_time(self, time_str):
        try:
            dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc).astimezone(GMT5)
        except:
            return None

def translate_text(text, src_lang):
    if not ENABLE_TRANSLATION or src_lang == 'en' or not text.strip():
        return text
    try:
        return translator.translate(text, src=src_lang, dest='en').text
    except:
        return text

def fetch_m3u_channels():
    try:
        response = requests.get(M3U_URL, timeout=30)
        response.raise_for_status()
        channels = []
        
        for line in response.text.splitlines():
            line = line.strip()
            if line.startswith("#EXTINF"):
                channel = {
                    'tvg-id': (re.search(r'tvg-id="([^"]*)"', line).group(1) 
                              if 'tvg-id=' in line else None),
                    'tvg-name': (re.search(r'tvg-name="([^"]*)"', line).group(1) 
                               if 'tvg-name=' in line else None),
                    'name': line.split(',')[-1].strip() if ',' in line else None,
                    'original_tvg-id': None
                }
                if channel['tvg-id']:
                    channel['original_tvg-id'] = channel['tvg-id']
                channels.append(channel)
        return channels
    except Exception as e:
        print(f"[ERROR] Fetching M3U: {str(e)}")
        return []

def download_epg():
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(EPG_SOURCE, stream=True, timeout=60)
            response.raise_for_status()
            
            handler = EPGHandler()
            parser = make_parser()
            parser.setContentHandler(handler)
            
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
                parser.parse(gz_file)
            
            return handler.channels, handler.programs
        except Exception as e:
            print(f"[RETRY {attempt + 1}] EPG download failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                return {}, defaultdict(list)
            time.sleep(5)

def normalize_string(s):
    """Normalize for case-insensitive comparison with spaces"""
    return re.sub(r'\s+', ' ', s.lower().strip()) if s else ""

def create_id_mapping(m3u_channels, epg_channels):
    id_mapping = {}
    used_epg_ids = set()
    
    # First pass: exact tvg-id matches (case-insensitive)
    for channel in m3u_channels:
        if not channel['original_tvg-id'] or channel['original_tvg-id'] in BLACKLIST:
            continue
            
        normalized_tvg_id = normalize_string(channel['original_tvg-id'])
        for epg_id, epg_data in epg_channels.items():
            if epg_id in used_epg_ids:
                continue
                
            if normalize_string(epg_id) == normalized_tvg_id:
                id_mapping[epg_id] = channel['original_tvg-id']
                used_epg_ids.add(epg_id)
                print(f"[MATCH] Exact tvg-id match: {channel['original_tvg-id']}")
                break
    
    # Second pass: tvg-name matches (case-insensitive with spaces)
    for channel in m3u_channels:
        if not channel['original_tvg-id'] or channel['original_tvg-id'] in BLACKLIST:
            continue
        if channel['original_tvg-id'] in id_mapping.values():
            continue
            
        if not channel['tvg-name']:
            continue
            
        normalized_tvg_name = normalize_string(channel['tvg-name'])
        best_match = None
        best_score = 0
        
        for epg_id, epg_data in epg_channels.items():
            if epg_id in used_epg_ids:
                continue
                
            for epg_name in epg_data['display_names']:
                normalized_epg_name = normalize_string(epg_name)
                # Calculate match score (1.0 = perfect match)
                score = SequenceMatcher(None, normalized_tvg_name, normalized_epg_name).ratio()
                if score > best_score and score >= 0.8:  # 80% similarity threshold
                    best_score = score
                    best_match = epg_id
        
        if best_match:
            id_mapping[best_match] = channel['original_tvg-id']
            used_epg_ids.add(best_match)
            print(f"[MATCH] tvg-name match ({int(best_score*100)}%): {channel['original_tvg-id']} -> {epg_channels[best_match]['original_id']}")
    
    return id_mapping

def generate_epg():
    print(f"[START] EPG generation (Timezone: {'GMT+5' if ENABLE_TIMEZONE_CORRECTION else 'UTC'})")
    print(f"[CONFIG] Auto-Translation: {'ON' if ENABLE_TRANSLATION else 'OFF'}")
    
    # Fetch M3U channels
    print("[STEP] Fetching M3U channels")
    m3u_channels = fetch_m3u_channels()
    if not m3u_channels:
        print("[ERROR] No channels found in M3U - aborting")
        return
    
    print(f"[INFO] Found {len(m3u_channels)} channels in M3U")
    
    # Download and parse EPG
    print("[STEP] Downloading and parsing EPG data")
    epg_channels, epg_programs = download_epg()
    if not epg_channels:
        print("[ERROR] Failed to get EPG data - aborting")
        return
    
    print(f"[INFO] Found {len(epg_channels)} EPG channels")
    
    # Create channel mapping
    print("[STEP] Creating channel mapping")
    id_mapping = create_id_mapping(m3u_channels, epg_channels)
    print(f"[INFO] Mapped {len(id_mapping)} channels")
    
    # Generate output EPG
    print("[STEP] Generating output EPG")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f_out:
        f_out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f_out.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        f_out.write('<tv>\n')
        
        # Process each M3U channel
        for channel in m3u_channels:
            if not channel['original_tvg-id'] or channel['original_tvg-id'] in BLACKLIST:
                continue
                
            matched = False
            program_count = 0
            
            # Find matching EPG channel
            for epg_id, original_id in id_mapping.items():
                if original_id == channel['original_tvg-id']:
                    matched = True
                    # Write channel info
                    f_out.write(f'<channel id="{original_id}">\n')
                    for name in epg_channels[epg_id]['display_names']:
                        f_out.write(f'<display-name>{escape(name)}</display-name>\n')
                    f_out.write('</channel>\n')
                    
                    # Write programs with translation
                    programs = epg_programs.get(epg_id, [])
                    program_count = len(programs)
                    for program in programs:
                        start_time = program['start'].strftime("%Y%m%d%H%M%S %z") if program['start'] else ""
                        stop_time = program['stop'].strftime("%Y%m%d%H%M%S %z") if program['stop'] else ""
                        
                        title = translate_text(program['title'], program['lang'])
                        desc = translate_text(program['desc'], program['lang'])
                        
                        f_out.write(f'<programme channel="{original_id}" start="{start_time}" stop="{stop_time}">\n')
                        f_out.write(f'<title>{escape(title)}</title>\n')
                        if desc:
                            f_out.write(f'<desc>{escape(desc)}</desc>\n')
                        f_out.write('</programme>\n')
                    break
            
            # Print channel status
            status = f"{channel['original_tvg-id']}: "
            if matched:
                status += f"Found {program_count} programs"
                if program_count > 0 and ENABLE_TRANSLATION:
                    status += " (translated)"
            else:
                status += "No EPG data"
                if channel['original_tvg-id'] in BLACKLIST:
                    status += " (blacklisted)"
            print(f"[CHANNEL] {status}")
        
        f_out.write('</tv>\n')
    
    print("[DONE] EPG generation completed")

if __name__ == "__main__":
    generate_epg()
