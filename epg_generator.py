#!/usr/bin/env python3
"""
Robust EPG Generator with Exact tvg-id Matching
Memory-efficient processing with thorough error checking
"""

import os
import re
import requests
from datetime import datetime
import gzip
import io
import time
from xml.sax import make_parser, handler
from difflib import SequenceMatcher

# Configuration
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCE = "https://epg.pw/xmltv/epg.xml.gz"
OUTPUT_FILE = "custom_epg.xml"
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming
MATCH_THRESHOLD = 0.8  # Similarity threshold for fuzzy matching
MAX_RETRIES = 3

class EPGChannelCollector(handler.ContentHandler):
    """Collects channel data from EPG while preserving original IDs"""
    def __init__(self):
        self.channels = {}
        self.current_channel = None
        self.current_text = ""
    
    def startElement(self, name, attrs):
        if name == "channel":
            self.current_channel = {
                'id': attrs.get("id", "").lower(),
                'names': set(),
                'original_id': attrs.get("id", "")
            }
        self.current_text = ""
    
    def characters(self, content):
        self.current_text += content
    
    def endElement(self, name):
        if name == "channel" and self.current_channel:
            self.channels[self.current_channel['id']] = self.current_channel
            self.current_channel = None
        elif name == "display-name" and self.current_channel:
            clean_name = self.current_text.strip().lower()
            if clean_name:
                self.current_channel['names'].add(clean_name)
        self.current_text = ""

class EPGFilter(handler.ContentHandler):
    """Filters EPG while maintaining original tvg-ids"""
    def __init__(self, output_file, id_mapping):
        self.output_file = output_file
        self.id_mapping = id_mapping
        self.current_channel = None
        self.buffer = []
        self.stats = {'channels': 0, 'programmes': 0}
    
    def startElement(self, name, attrs):
        if name == "channel":
            epg_id = attrs.get("id", "").lower()
            if epg_id in self.id_mapping:
                self.current_channel = epg_id
                self.buffer.append(f'<channel id="{self.id_mapping[epg_id]}">')
                self.stats['channels'] += 1
        elif name == "programme":
            channel = attrs.get("channel", "").lower()
            if channel in self.id_mapping:
                self.buffer.append(
                    f'<programme channel="{self.id_mapping[channel]}" '
                    f'start="{attrs.get("start", "")}" '
                    f'stop="{attrs.get("stop", "")}">'
                )
                self.stats['programmes'] += 1
        elif self.buffer:
            self.buffer.append(f'<{name}')
            for k, v in attrs.items():
                self.buffer.append(f' {k}="{v}"')
            self.buffer.append('>')
    
    def characters(self, content):
        if self.buffer:
            self.buffer.append(content)
    
    def endElement(self, name):
        if not self.buffer:
            return
            
        if name == "channel" and self.current_channel:
            self.buffer.append('</channel>')
            self._flush_buffer()
            self.current_channel = None
        elif name == "programme":
            self.buffer.append('</programme>')
            self._flush_buffer()
        else:
            self.buffer.append(f'</{name}>')
    
    def _flush_buffer(self):
        if self.buffer:
            self.output_file.write(''.join(self.buffer))
            self.buffer = []

def fetch_m3u_channels():
    """Extract channels from M3U with original tvg-ids"""
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
        print(f"Error fetching M3U: {str(e)}")
        return []

def download_epg(temp_file):
    """Download and save EPG with retries"""
    for attempt in range(MAX_RETRIES):
        try:
            with requests.get(EPG_SOURCE, stream=True, timeout=60) as response:
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
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                return False
            time.sleep(5)

def calculate_similarity(a, b):
    """Calculate string similarity score"""
    return SequenceMatcher(None, a, b).ratio()

def create_id_mapping(m3u_channels, epg_channels):
    """Create mapping between EPG IDs and original tvg-ids"""
    id_mapping = {}
    used_epg_ids = set()
    
    # First pass: exact matches
    for channel in m3u_channels:
        if not channel['original_tvg-id']:
            continue
            
        for epg_id, epg_data in epg_channels.items():
            if epg_id.lower() == channel['original_tvg-id'].lower():
                id_mapping[epg_id] = channel['original_tvg-id']
                used_epg_ids.add(epg_id)
                break
    
    # Second pass: fuzzy matching
    for channel in m3u_channels:
        if not channel['original_tvg-id'] or channel['original_tvg-id'].lower() in (v.lower() for v in id_mapping.values()):
            continue
            
        channel_names = set()
        if channel['tvg-id']:
            channel_names.add(channel['tvg-id'].lower())
        if channel['tvg-name']:
            channel_names.add(channel['tvg-name'].lower())
        if channel['name']:
            channel_names.add(channel['name'].lower())
            simple_name = re.sub(r'[^a-z0-9]', '', channel['name'].lower())
            if simple_name:
                channel_names.add(simple_name)
        
        best_match = None
        best_score = 0
        
        for epg_id, epg_data in epg_channels.items():
            if epg_id in used_epg_ids:
                continue
                
            epg_names = {epg_id.lower()}
            epg_names.update(epg_data['names'])
            
            for epg_name in epg_names:
                for m3u_name in channel_names:
                    score = calculate_similarity(m3u_name, epg_name)
                    if score > best_score and score >= MATCH_THRESHOLD:
                        best_score = score
                        best_match = epg_id
        
        if best_match:
            id_mapping[best_match] = channel['original_tvg-id']
            used_epg_ids.add(best_match)
    
    return id_mapping

def generate_epg():
    """Main EPG generation process"""
    print("Starting EPG generation...")
    start_time = datetime.now()
    
    # Get M3U channels
    print("Fetching M3U channels...")
    m3u_channels = fetch_m3u_channels()
    if not m3u_channels:
        print("No channels found in M3U")
        return
    
    print(f"Found {len(m3u_channels)} channels in M3U")
    
    # Download EPG
    temp_file = "temp_epg.xml"
    print(f"Downloading EPG from {EPG_SOURCE}...")
    if not download_epg(temp_file):
        print("Failed to download EPG")
        return
    
    # Parse EPG channels
    print("Parsing EPG channels...")
    channel_collector = EPGChannelCollector()
    parser = make_parser()
    parser.setContentHandler(channel_collector)
    parser.parse(temp_file)
    epg_channels = channel_collector.channels
    print(f"Found {len(epg_channels)} EPG channels")
    
    # Create ID mapping
    print("Creating channel mapping...")
    id_mapping = create_id_mapping(m3u_channels, epg_channels)
    print(f"Mapped {len(id_mapping)} channels")
    
    if not id_mapping:
        print("No channel matches found")
        try:
            os.remove(temp_file)
        except:
            pass
        return
    
    # Generate filtered EPG
    print("Generating filtered EPG...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        f_out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f_out.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        f_out.write('<tv>\n')
        
        filter_handler = EPGFilter(f_out, id_mapping)
        parser = make_parser()
        parser.setContentHandler(filter_handler)
        parser.parse(temp_file)
        
        f_out.write('</tv>\n')
    
    print(f"Generated EPG with {filter_handler.stats['channels']} channels and {filter_handler.stats['programmes']} programmes")
    
    # Clean up
    try:
        os.remove(temp_file)
    except:
        pass
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"Completed in {duration:.2f} seconds")

if __name__ == "__main__":
    generate_epg()
