#!/usr/bin/env python3
"""
EPG Generator for M3U Playlist
This script generates an EPG (Electronic Program Guide) for IPTV channels in an M3U playlist.
It uses the reliable EPG source from epgshare01.online with GZIP support.
"""

import os
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import quote
import json
import gzip
import io

# Constants
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCES = [
    "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
]
OUTPUT_FILE = "epg.xml"
CACHE_FILE = "epg_cache.json"
CACHE_EXPIRY_DAYS = 1
MAX_RETRIES = 3

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

def fetch_gzipped_epg(source_url):
    """Fetch and decompress GZIPPED EPG data."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(source_url, timeout=15)
            response.raise_for_status()
            
            # Decompress the GZIP content
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
                return gz_file.read().decode('utf-8')
                
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {source_url}: {e}")
            if attempt == MAX_RETRIES - 1:
                return None
            time.sleep(2)  # Wait before retrying

def merge_epg_sources(sources):
    """Merge multiple EPG sources into one."""
    merged_root = ET.Element("tv")
    channel_ids = set()
    
    for source in sources:
        try:
            print(f"Fetching EPG from {source}...")
            epg_content = fetch_gzipped_epg(source)
            if not epg_content:
                continue
                
            root = ET.fromstring(epg_content)
            
            # Count programs before merging for logging
            programs_before = len(merged_root.findall("programme"))
            
            for channel in root.findall("channel"):
                channel_id = channel.get("id")
                if channel_id not in channel_ids:
                    merged_root.append(channel)
                    channel_ids.add(channel_id)
            
            for programme in root.findall("programme"):
                channel_id = programme.get("channel")
                if channel_id in channel_ids:
                    merged_root.append(programme)
            
            # Log statistics
            programs_added = len(merged_root.findall("programme")) - programs_before
            print(f"Added {len(root.findall('channel'))} channels and {programs_added} programs from {source}")
                    
        except Exception as e:
            print(f"Error processing EPG source {source}: {e}")
    
    return ET.ElementTree(merged_root)

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

def match_channels_to_epg(channels, epg_tree):
    """Match M3U channels to EPG data with improved matching."""
    matched_channels = defaultdict(list)
    epg_channels = epg_tree.findall("channel")
    
    # Build EPG channel index
    epg_index = {}
    for epg_channel in epg_channels:
        epg_id = epg_channel.get("id").lower().strip()
        epg_names = set()
        
        # Get all display names
        for display_name in epg_channel.findall("display-name"):
            if display_name.text:
                epg_names.add(display_name.text.lower().strip())
        
        epg_index[epg_id] = {
            'names': epg_names,
            'node': epg_channel
        }
    
    # Match channels
    for channel in channels:
        aliases = get_channel_aliases(channel)
        best_match = None
        best_score = 0
        
        for epg_id, epg_data in epg_index.items():
            # Check direct ID match
            if epg_id in aliases:
                best_match = epg_id
                best_score = 100  # Highest score for direct match
                break
            
            # Check name matches
            for epg_name in epg_data['names']:
                # Exact match
                if epg_name in aliases:
                    current_score = 95
                # Contains match
                elif any(alias in epg_name or epg_name in alias for alias in aliases):
                    current_score = max(
                        len(alias) / len(epg_name) * 90 
                        for alias in aliases 
                        if alias in epg_name or epg_name in alias
                    )
                else:
                    continue
                
                if current_score > best_score:
                    best_score = current_score
                    best_match = epg_id
        
        if best_match and best_score > 70:  # Only consider good matches
            matched_channels[channel['name']].append(best_match)
    
    print(f"Channel matching completed with {len(matched_channels)} matches")
    return matched_channels

def filter_epg_for_channels(epg_tree, matched_channels):
    """Filter EPG to only include matched channels."""
    all_matched_ids = set()
    for ids in matched_channels.values():
        all_matched_ids.update(ids)
    
    new_root = ET.Element("tv")
    
    # Add matched channels
    for channel in epg_tree.findall("channel"):
        if channel.get("id") in all_matched_ids:
            new_root.append(channel)
    
    # Add programmes for matched channels (7 days worth)
    cutoff = datetime.now() + timedelta(days=7)
    for programme in epg_tree.findall("programme"):
        if programme.get("channel") in all_matched_ids:
            try:
                start = programme.get("start")
                if start:
                    start_time = datetime.strptime(start[:14], "%Y%m%d%H%M%S")
                    if start_time <= cutoff:
                        new_root.append(programme)
            except:
                new_root.append(programme)  # Include if we can't parse time
    
    return ET.ElementTree(new_root)

def save_epg_to_file(epg_tree, filename):
    """Save EPG data to XML file with proper formatting."""
    # Create XML with declaration and doctype
    xml_str = ET.tostring(epg_tree.getroot(), encoding='utf-8').decode()
    
    # Pretty-print the XML (basic indentation)
    xml_str = xml_str.replace('><', '>\n<')
    
    xml_with_declaration = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE tv SYSTEM "xmltv.dtd">\n'
        f'{xml_str}'
    )
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(xml_with_declaration)

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

def load_cached_epg():
    """Load EPG from cache if available."""
    try:
        with open(CACHE_FILE, 'r') as f:
            cache_data = json.load(f)
        return ET.fromstring(cache_data['epg'])
    except:
        return None

def save_epg_to_cache(epg_tree):
    """Save EPG to cache file."""
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'epg': ET.tostring(epg_tree.getroot(), encoding='utf-8').decode()
    }
    
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_data, f)

def generate_epg():
    """Main function to generate EPG."""
    print("Starting EPG generation...")
    start_time = datetime.now()
    
    # Check cache first
    if is_cache_valid():
        print("Loading EPG from cache...")
        epg_tree = load_cached_epg()
        if epg_tree:
            save_epg_to_file(ET.ElementTree(epg_tree), OUTPUT_FILE)
            print(f"EPG saved to {OUTPUT_FILE} (from cache)")
            return
    
    print("Fetching M3U playlist...")
    channels = fetch_m3u_channels(M3U_URL)
    print(f"Found {len(channels)} channels in M3U playlist")
    
    print("Fetching and merging EPG sources...")
    epg_tree = merge_epg_sources(EPG_SOURCES)
    
    print("Matching channels to EPG data...")
    matched_channels = match_channels_to_epg(channels, epg_tree)
    print(f"Matched {len(matched_channels)}/{len(channels)} channels to EPG data")
    
    print("Filtering EPG for matched channels...")
    filtered_epg = filter_epg_for_channels(epg_tree, matched_channels)
    
    print("Saving EPG to file...")
    save_epg_to_file(filtered_epg, OUTPUT_FILE)
    save_epg_to_cache(filtered_epg)
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"EPG generation completed in {duration:.2f} seconds")
    print(f"EPG saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    import time
    generate_epg()
