#!/usr/bin/env python3
"""
Memory-efficient EPG Generator with Exact tvg-id Matching
Preserves original tvg-ids from M3U while handling large EPG files
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

# Configuration
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCES = [
    "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
]
OUTPUT_FILE = "custom_epg.xml"  # Changed to custom_epg.xml as requested
CACHE_FILE = "epg_cache.json"
CACHE_EXPIRY_DAYS = 1
MAX_RETRIES = 3
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming

class EPGChannelCollector(handler.ContentHandler):
    """SAX parser to collect channel IDs and names with minimal memory usage."""
    def __init__(self):
        self.channel_data = {}  # Stores both ID and display names
        self.current_tag = ""
        self.current_channel = None
    
    def startElement(self, name, attrs):
        self.current_tag = name
        if name == "channel":
            self.current_channel = {
                'id': attrs.get("id", "").lower(),
                'names': set()
            }
    
    def characters(self, content):
        pass
    
    def endElement(self, name):
        if name == "channel":
            if self.current_channel:
                self.channel_data[self.current_channel['id']] = self.current_channel
            self.current_channel = None
        elif name == "display-name" and self.current_channel and self.current_tag == "display-name":
            if hasattr(self, 'current_text'):
                self.current_channel['names'].add(self.current_text.lower())
        self.current_tag = ""

class EPGFilter(handler.ContentHandler):
    """SAX parser to filter EPG and preserve original tvg-ids."""
    def __init__(self, output_file, id_mapping):
        self.output_file = output_file
        self.id_mapping = id_mapping  # Maps EPG IDs to original tvg-ids
        self.in_channel = False
        self.in_programme = False
        self.current_channel_id = ""
        self.buffer = []
        self.programme_count = 0
        self.channel_count = 0
        
    def startElement(self, name, attrs):
        if name == "channel":
            epg_id = attrs.get("id", "").lower()
            if epg_id in self.id_mapping:
                self.in_channel = True
                self.current_channel_id = epg_id
                # Use original tvg-id from M3U
                original_id = self.id_mapping[epg_id]
                self.buffer.append(f'<channel id="{original_id}">')
                self.channel_count += 1
        elif name == "programme":
            epg_id = attrs.get("channel", "").lower()
            if epg_id in self.id_mapping:
                self.in_programme = True
                # Use original tvg-id from M3U
                original_id = self.id_mapping[epg_id]
                self.buffer.append(f'<programme channel="{original_id}" '
                                 f'start="{attrs.get("start", "")}" '
                                 f'stop="{attrs.get("stop", "")}">')
                self.programme_count += 1
        elif (self.in_channel and self.current_channel_id in self.id_mapping) or \
             (self.in_programme and attrs.get("channel", "").lower() in self.id_mapping):
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
            if self.current_channel_id in self.id_mapping:
                self.buffer.append('</channel>')
                self._flush_buffer()
            self.current_channel_id = ""
        elif name == "programme":
            self.in_programme = False
            self.buffer.append('</programme>')
            self._flush_buffer()
        elif len(self.buffer) > 0:
            self.buffer.append(f'</{name}>')
    
    def _flush_buffer(self):
        if len(self.buffer) > 0:
            self.output_file.write(''.join(self.buffer))
            self.buffer = []

def fetch_m3u_channels(m3u_url):
    """Fetch and parse M3U playlist, preserving original tvg-ids exactly."""
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
                    'name': None
                }
                
                # Extract tvg-id exactly as it appears in M3U
                tvg_id_match = re.search(r'tvg-id="([^"]*)"', line)
                if tvg_id_match:
                    current_channel['tvg-id'] = tvg_id_match.group(1)
                
                tvg_name_match = re.search(r'tvg-name="([^"]*)"', line)
                if tvg_name_match:
                    current_channel['tvg-name'] = tvg_name_match.group(1)
                
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

def get_epg_channel_data(epg_file):
    """Extract channel IDs and names from EPG file using SAX parser."""
    collector = EPGChannelCollector()
    parser = make_parser()
    parser.setContentHandler(collector)
    
    try:
        parser.parse(epg_file)
        return collector.channel_data
    except Exception as e:
        print(f"Error parsing EPG file: {e}")
        return {}

def create_id_mapping(channels, epg_channel_data):
    """Create mapping between EPG IDs and original tvg-ids."""
    id_mapping = {}
    
    for channel in channels:
        if not channel.get('tvg-id'):
            continue
            
        original_tvg_id = channel['tvg-id']
        channel_aliases = get_channel_aliases(channel)
        
        # Check for direct matches with EPG channel IDs
        for epg_id, epg_data in epg_channel_data.items():
            if epg_id in channel_aliases:
                id_mapping[epg_id] = original_tvg_id
                break
        else:
            # Check for matches with display names
            for epg_id, epg_data in epg_channel_data.items():
                for epg_name in epg_data['names']:
                    if any(alias in epg_name or epg_name in alias for alias in channel_aliases):
                        id_mapping[epg_id] = original_tvg_id
                        break
    
    return id_mapping

def get_channel_aliases(channel_info):
    """Generate possible aliases for channel matching."""
    aliases = set()
    
    if channel_info.get('tvg-id'):
        aliases.add(channel_info['tvg-id'].lower().strip())
    
    if channel_info.get('tvg-name'):
        aliases.add(channel_info['tvg-name'].lower().strip())
    
    if channel_info.get('name'):
        aliases.add(channel_info['name'].lower().strip())
        # Add variations without special characters
        simplified = re.sub(r'[^a-z0-9]', '', channel_info['name'].lower())
        if simplified:
            aliases.add(simplified)
    
    return aliases

def filter_epg(input_file, output_file, id_mapping):
    """Filter EPG file using the ID mapping."""
    with open(output_file, 'w', encoding='utf-8') as f_out:
        # Write XML header
        f_out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f_out.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        f_out.write('<tv>\n')
        
        # Create and run SAX parser
        filter_handler = EPGFilter(f_out, id_mapping)
        parser = make_parser()
        parser.setContentHandler(filter_handler)
        parser.parse(input_file)
        
        # Close TV tag
        f_out.write('</tv>\n')
    
    return filter_handler.channel_count, filter_handler.programme_count

def save_id_mapping(id_mapping):
    """Save ID mapping to cache."""
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'id_mapping': id_mapping
    }
    
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_data, f)

def load_id_mapping():
    """Load ID mapping from cache."""
    try:
        with open(CACHE_FILE, 'r') as f:
            cache_data = json.load(f)
        return cache_data.get('id_mapping', {})
    except:
        return {}

def is_cache_valid():
    """Check if cached mapping is still valid."""
    if not os.path.exists(CACHE_FILE):
        return False
    
    try:
        with open(CACHE_FILE, 'r') as f:
            cache_data = json.load(f)
        
        cache_time = datetime.fromisoformat(cache_data['timestamp'])
        return (datetime.now() - cache_time) < timedelta(days=CACHE_EXPIRY_DAYS)
    except:
        return False

def generate_epg():
    """Main function to generate EPG with exact tvg-id matching."""
    print("Starting EPG generation with exact tvg-id matching...")
    start_time = datetime.now()
    
    # Check cache first
    id_mapping = load_id_mapping()
    if is_cache_valid() and id_mapping:
        print("Using cached channel mapping...")
    else:
        print("Fetching M3U playlist...")
        channels = fetch_m3u_channels(M3U_URL)
        print(f"Found {len(channels)} channels in M3U playlist")
        
        # Process EPG sources
        all_epg_channel_data = {}
        temp_files = []
        
        for source in EPG_SOURCES:
            temp_file = f"temp_{os.path.basename(source)}.xml"
            temp_files.append(temp_file)
            
            print(f"Downloading {source}...")
            if not stream_download_gzipped_epg(source, temp_file):
                continue
                
            print(f"Extracting channel data from {source}...")
            epg_channel_data = get_epg_channel_data(temp_file)
            all_epg_channel_data.update(epg_channel_data)
            print(f"Found {len(epg_channel_data)} channels in this source")
        
        print("Creating ID mapping...")
        id_mapping = create_id_mapping(channels, all_epg_channel_data)
        print(f"Mapped {len(id_mapping)} channels to original tvg-ids")
        
        # Save mapping to cache
        save_id_mapping(id_mapping)
        
        # Clean up temp files
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except:
                pass
    
    if not id_mapping:
        print("No channel mappings found, exiting")
        return
    
    # Filter the first available EPG source
    for source in EPG_SOURCES:
        temp_file = f"temp_{os.path.basename(source)}.xml"
        
        print(f"Processing {source} for filtering...")
        if stream_download_gzipped_epg(source, temp_file):
            print(f"Filtering EPG data with exact tvg-ids...")
            channel_count, programme_count = filter_epg(temp_file, OUTPUT_FILE, id_mapping)
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
