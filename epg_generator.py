#!/usr/bin/env python3
"""
Memory-efficient EPG Generator for Large GZIP Files
This version handles 300MB+ EPG files with minimal memory usage.
"""

import os
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict
import json
import gzip
import io
import time
from xml.sax import make_parser, handler

# Constants
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCES = [
    "https://epgshare01.online/epgshare01/epg_ripper_USUKCA.xml.gz",  # Smaller regional file
    "https://epgshare01.online/epgshare01/epg_ripper_ASIA.xml.gz"     # Another regional file
]
OUTPUT_FILE = "epg.xml"
CACHE_FILE = "epg_cache.json"
CACHE_EXPIRY_DAYS = 1
MAX_RETRIES = 3
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming

class EPGChannelCollector(handler.ContentHandler):
    """SAX parser to collect channel IDs with minimal memory usage."""
    def __init__(self):
        self.channel_ids = set()
        self.current_tag = ""
    
    def startElement(self, name, attrs):
        self.current_tag = name
        if name == "channel":
            self.channel_ids.add(attrs.get("id", "").lower())
    
    def characters(self, content):
        pass
    
    def endElement(self, name):
        self.current_tag = ""

class EPGFilter(handler.ContentHandler):
    """SAX parser to filter EPG for specific channels."""
    def __init__(self, output_file, matched_ids):
        self.output_file = output_file
        self.matched_ids = matched_ids
        self.in_channel = False
        self.in_programme = False
        self.current_channel_id = ""
        self.buffer = []
        self.programme_count = 0
        self.channel_count = 0
        
    def startElement(self, name, attrs):
        if name == "channel":
            self.in_channel = True
            self.current_channel_id = attrs.get("id", "").lower()
            if self.current_channel_id in self.matched_ids:
                self.buffer.append(f'<channel id="{attrs.get("id", "")}">')
                self.channel_count += 1
        elif name == "programme":
            self.in_programme = True
            channel_id = attrs.get("channel", "").lower()
            if channel_id in self.matched_ids:
                self.buffer.append(f'<programme channel="{attrs.get("channel", "")}" '
                                 f'start="{attrs.get("start", "")}" '
                                 f'stop="{attrs.get("stop", "")}">')
                self.programme_count += 1
        elif (self.in_channel and self.current_channel_id in self.matched_ids) or \
             (self.in_programme and attrs.get("channel", "").lower() in self.matched_ids):
            self.buffer.append(f'<{name}')
            for k, v in attrs.items():
                self.buffer.append(f' {k}="{v}"')
            self.buffer.append('>')
    
    def characters(self, content):
        if len(self.buffer) > 0:
            self.buffer.append(content)
    
    def endElement(self, name):
        if name == "channel":
            self.in_channel = False
            if self.current_channel_id in self.matched_ids:
                self.buffer.append(f'</channel>')
                self._flush_buffer()
        elif name == "programme":
            self.in_programme = False
            self.buffer.append(f'</programme>')
            self._flush_buffer()
        elif len(self.buffer) > 0:
            self.buffer.append(f'</{name}>')
    
    def _flush_buffer(self):
        if len(self.buffer) > 0:
            self.output_file.write(''.join(self.buffer))
            self.buffer = []

def fetch_m3u_channels(m3u_url):
    """Fetch and parse M3U playlist to extract channel information."""
    try:
        response = requests.get(m3u_url)
        response.raise_for_status()
        m3u_content = response.text
        
        channels = []
        current_channel = None
        
        for line in m3u_content.splitlines():
            line = line.strip()
            if line.startswith("#EXTINF"):
                current_channel = {
                    'tvg-id': None,
                    'tvg-name': None,
                    'tvg-logo': None,
                    'group-title': None,
                    'name': None
                }
                
                # Extract attributes
                tvg_id_match = re.search(r'tvg-id="([^"]*)"', line)
                if tvg_id_match:
                    current_channel['tvg-id'] = tvg_id_match.group(1)
                
                tvg_name_match = re.search(r'tvg-name="([^"]*)"', line)
                if tvg_name_match:
                    current_channel['tvg-name'] = tvg_name_match.group(1)
                
                tvg_logo_match = re.search(r'tvg-logo="([^"]*)"', line)
                if tvg_logo_match:
                    current_channel['tvg-logo'] = tvg_logo_match.group(1)
                
                group_title_match = re.search(r'group-title="([^"]*)"', line)
                if group_title_match:
                    current_channel['group-title'] = group_title_match.group(1)
                
                # Extract channel name (after the last comma)
                name_match = re.search(r',(.*)$', line)
                if name_match:
                    current_channel['name'] = name_match.group(1).strip()
            
            elif line and not line.startswith("#") and current_channel:
                current_channel['url'] = line
                channels.append(current_channel)
                current_channel = None
        
        return channels
    
    except Exception as e:
        print(f"Error fetching M3U playlist: {e}")
        return []

def stream_download_gzipped_epg(source_url, temp_file):
    """Stream download and decompress GZIPPED EPG data directly to file."""
    for attempt in range(MAX_RETRIES):
        try:
            with requests.get(source_url, stream=True, timeout=30) as response:
                response.raise_for_status()
                
                with gzip.GzipFile(fileobj=response.raw) as gz_file:
                    with open(temp_file, 'wb') as f_out:
                        while True:
                            chunk = gz_file.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            f_out.write(chunk)
            return True
                    
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {source_url}: {e}")
            if attempt == MAX_RETRIES - 1:
                return False
            time.sleep(5)

def get_channel_ids_from_epg(epg_file):
    """Extract channel IDs from EPG file using SAX parser."""
    collector = EPGChannelCollector()
    parser = make_parser()
    parser.setContentHandler(collector)
    
    try:
        parser.parse(epg_file)
        return collector.channel_ids
    except Exception as e:
        print(f"Error parsing EPG file: {e}")
        return set()

def get_channel_aliases(channel_info):
    """Generate possible aliases for channel matching."""
    aliases = set()
    
    # Original IDs and names
    if channel_info.get('tvg-id'):
        aliases.add(channel_info['tvg-id'].lower().strip())
    
    if channel_info.get('tvg-name'):
        aliases.add(channel_info['tvg-name'].lower().strip())
    
    if channel_info.get('name'):
        aliases.add(channel_info['name'].lower().strip())
    
    # Variations without spaces and special chars
    for alias in list(aliases):
        # Remove common suffixes
        for suffix in ['hd', 'fhd', 'uhd', 'tv', 'channel']:
            if alias.endswith(suffix):
                aliases.add(alias[:-len(suffix)].strip())
        
        # Space variations
        no_space = alias.replace(' ', '')
        if no_space != alias:
            aliases.add(no_space)
        
        no_space_dash = alias.replace(' ', '-')
        if no_space_dash != alias:
            aliases.add(no_space_dash)
        
        # Special character variations
        no_special = re.sub(r'[^a-z0-9]', '', alias)
        if no_special != alias:
            aliases.add(no_special)
    
    return aliases

def match_channels(channels, epg_channel_ids):
    """Match M3U channels to EPG channel IDs."""
    matched_ids = set()
    
    for channel in channels:
        aliases = get_channel_aliases(channel)
        
        # Check for direct matches
        for epg_id in epg_channel_ids:
            if any(alias == epg_id for alias in aliases):
                matched_ids.add(epg_id)
                break
        else:
            # Check for partial matches
            for epg_id in epg_channel_ids:
                if any(alias in epg_id or epg_id in alias for alias in aliases):
                    matched_ids.add(epg_id)
                    break
    
    return matched_ids

def filter_epg(input_file, output_file, matched_ids):
    """Filter EPG file to only include matched channels."""
    with open(output_file, 'w', encoding='utf-8') as f_out:
        # Write XML header
        f_out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f_out.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        f_out.write('<tv>\n')
        
        # Create and run SAX parser
        filter_handler = EPGFilter(f_out, matched_ids)
        parser = make_parser()
        parser.setContentHandler(filter_handler)
        parser.parse(input_file)
        
        # Close TV tag
        f_out.write('</tv>\n')
    
    return filter_handler.channel_count, filter_handler.programme_count

def is_cache_valid():
    """Check if cached EPG is still valid."""
    if not os.path.exists(CACHE_FILE):
        return False
    
    try:
        with open(CACHE_FILE, 'r') as f:
            cache_data = json.load(f)
        
        cache_time = datetime.fromisoformat(cache_data['timestamp'])
        return (datetime.now() - cache_time) < timedelta(days=CACHE_EXPIRY_DAYS)
    except:
        return False

def save_matched_channels(matched_ids):
    """Save matched channel IDs to cache."""
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'matched_ids': list(matched_ids)
    }
    
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_data, f)

def load_matched_channels():
    """Load matched channel IDs from cache."""
    try:
        with open(CACHE_FILE, 'r') as f:
            cache_data = json.load(f)
        return set(cache_data.get('matched_ids', []))
    except:
        return set()

def generate_epg():
    """Main function to generate EPG."""
    print("Starting EPG generation...")
    start_time = datetime.now()
    
    # Check cache first
    matched_ids = load_matched_channels()
    if is_cache_valid() and matched_ids:
        print("Using cached channel matches...")
    else:
        print("Fetching M3U playlist...")
        channels = fetch_m3u_channels(M3U_URL)
        print(f"Found {len(channels)} channels in M3U playlist")
        
        # Process EPG sources
        all_epg_channel_ids = set()
        temp_files = []
        
        for source in EPG_SOURCES:
            temp_file = f"temp_{os.path.basename(source)}.xml"
            temp_files.append(temp_file)
            
            print(f"Downloading {source}...")
            if not stream_download_gzipped_epg(source, temp_file):
                continue
                
            print(f"Extracting channel IDs from {source}...")
            epg_channel_ids = get_channel_ids_from_epg(temp_file)
            all_epg_channel_ids.update(epg_channel_ids)
            print(f"Found {len(epg_channel_ids)} channels in this source")
        
        print("Matching channels...")
        matched_ids = match_channels(channels, all_epg_channel_ids)
        print(f"Matched {len(matched_ids)} channels")
        
        # Save matches to cache
        save_matched_channels(matched_ids)
        
        # Clean up temp files
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except:
                pass
    
    if not matched_ids:
        print("No channel matches found, exiting")
        return
    
    # Filter the first available EPG source
    for source in EPG_SOURCES:
        temp_file = f"temp_{os.path.basename(source)}.xml"
        
        print(f"Processing {source} for filtering...")
        if stream_download_gzipped_epg(source, temp_file):
            print(f"Filtering EPG data...")
            channel_count, programme_count = filter_epg(temp_file, OUTPUT_FILE, matched_ids)
            print(f"Saved {channel_count} channels and {programme_count} programmes to {OUTPUT_FILE}")
            
            try:
                os.remove(temp_file)
            except:
                pass
            break
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"EPG generation completed in {duration:.2f} seconds")

if __name__ == "__main__":
    generate_epg()
