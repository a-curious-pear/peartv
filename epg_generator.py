#!/usr/bin/env python3
"""
GitHub-Compatible EPG Generator with Print Logging
Uses print() for logging visible in GitHub Actions
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

# Configuration
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCE = "https://epg.pw/xmltv/epg.xml.gz"
OUTPUT_FILE = "custom_epg.xml"
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming
MAX_RETRIES = 3
GMT5 = timezone(timedelta(hours=5))  # GMT+5 timezone

class EPGHandler(handler.ContentHandler):
    """SAX handler for processing EPG data"""
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
            self.channels[self.current_channel] = {
                'display_names': [],
                'original_id': attrs.get("id", "")
            }
        elif name == "programme":
            self.current_program = {
                'channel': attrs.get("channel", "").lower(),
                'start': self.parse_time(attrs.get("start", "")),
                'stop': self.parse_time(attrs.get("stop", "")),
                'title': "",
                'desc': ""
            }

    def characters(self, content):
        if self.current_element in ['display-name', 'title', 'desc']:
            self.current_text += content

    def endElement(self, name):
        if name == "display-name" and self.current_channel:
            self.channels[self.current_channel]['display_names'].append(self.current_text.strip())
        elif name == "title" and self.current_program:
            self.current_program['title'] = self.current_text.strip()
        elif name == "desc" and self.current_program:
            self.current_program['desc'] = self.current_text.strip()
        elif name == "programme" and self.current_program:
            self.programs[self.current_program['channel']].append(self.current_program)
            self.current_program = None
        elif name == "channel":
            self.current_channel = None
        
        self.current_text = ""
        self.current_element = None

    def parse_time(self, time_str):
        """Parse time string and convert to GMT+5"""
        try:
            dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc).astimezone(GMT5)
        except:
            return None

def fetch_m3u_channels():
    """Fetch and parse M3U playlist"""
    try:
        response = requests.get(M3U_URL, timeout=30)
        response.raise_for_status()
        channels = []
        
        for line in response.text.splitlines():
            line = line.strip()
            if line.startswith("#EXTINF"):
                tvg_id = re.search(r'tvg-id="([^"]*)"', line)
                tvg_name = re.search(r'tvg-name="([^"]*)"', line)
                name = line.split(',')[-1].strip() if ',' in line else None
                
                channel = {
                    'tvg-id': tvg_id.group(1) if tvg_id else None,
                    'tvg-name': tvg_name.group(1) if tvg_name else None,
                    'name': name,
                    'original_tvg-id': tvg_id.group(1) if tvg_id else None
                }
                channels.append(channel)
        return channels
    except Exception as e:
        print(f"[ERROR] Fetching M3U: {str(e)}")
        return []

def download_epg():
    """Download and parse EPG data"""
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

def create_id_mapping(m3u_channels, epg_channels):
    """Create mapping between M3U and EPG channels"""
    id_mapping = {}
    
    for channel in m3u_channels:
        if not channel['original_tvg-id']:
            continue
            
        # Try exact match first
        for epg_id, epg_data in epg_channels.items():
            if epg_id.lower() == channel['original_tvg-id'].lower():
                id_mapping[epg_id] = channel['original_tvg-id']
                print(f"[MATCH] Exact match for {channel['original_tvg-id']}")
                break
        else:
            # Try name matching
            channel_names = set()
            if channel['tvg-id']:
                channel_names.add(channel['tvg-id'].lower())
            if channel['tvg-name']:
                channel_names.add(channel['tvg-name'].lower())
            if channel['name']:
                channel_names.add(channel['name'].lower())
                # Add simplified version
                simple_name = re.sub(r'[^a-z0-9]', '', channel['name'].lower())
                if simple_name:
                    channel_names.add(simple_name)
            
            # Find best match in EPG display names
            best_match = None
            best_score = 0
            
            for epg_id, epg_data in epg_channels.items():
                if epg_id in id_mapping.values():
                    continue
                    
                for epg_name in epg_data['display_names']:
                    epg_name_lower = epg_name.lower()
                    for m3u_name in channel_names:
                        # Simple matching - no external deps
                        score = len(set(m3u_name.split()) & set(epg_name_lower.split())) / \
                                max(len(set(m3u_name.split())), 1)
                        
                        if score > best_score and score >= 0.5:  # 50% match threshold
                            best_score = score
                            best_match = epg_id
            
            if best_match:
                id_mapping[best_match] = channel['original_tvg-id']
                print(f"[MATCH] Fuzzy match ({int(best_score*100)}%) for {channel['original_tvg-id']}")
    
    return id_mapping

def generate_epg():
    """Main EPG generation function"""
    print(f"[START] EPG generation at {datetime.now(GMT5).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
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
    
    program_count = sum(len(p) for p in epg_programs.values())
    print(f"[INFO] Found {len(epg_channels)} channels and {program_count} programs in EPG")
    
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
            if not channel['original_tvg-id']:
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
                    
                    # Write programs
                    programs = epg_programs.get(epg_id, [])
                    program_count = len(programs)
                    for program in programs:
                        start_time = program['start'].strftime("%Y%m%d%H%M%S %z") if program['start'] else ""
                        stop_time = program['stop'].strftime("%Y%m%d%H%M%S %z") if program['stop'] else ""
                        
                        f_out.write(f'<programme channel="{original_id}" start="{start_time}" stop="{stop_time}">\n')
                        f_out.write(f'<title>{escape(program["title"])}</title>\n')
                        if program['desc']:
                            f_out.write(f'<desc>{escape(program["desc"])}</desc>\n')
                        f_out.write('</programme>\n')
                    break
            
            # Print channel status
            if matched:
                print(f"[CHANNEL] {channel['original_tvg-id']}: Found {program_count} programs")
            else:
                print(f"[CHANNEL] {channel['original_tvg-id']}: No EPG data found")
        
        f_out.write('</tv>\n')
    
    print(f"[DONE] EPG generation completed at {datetime.now(GMT5).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    generate_epg()
