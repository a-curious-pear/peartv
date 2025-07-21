#!/usr/bin/env python3
"""
EPG Generator that matches M3U tvg-ids exactly
Generates an EPG where channel IDs are forced to match your M3U's tvg-id values
"""

import os
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import gzip
import io

# Configuration
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCE = "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
OUTPUT_FILE = "custom_epg.xml"

def get_m3u_tvg_ids():
    """Extract all tvg-id values from the M3U file"""
    try:
        response = requests.get(M3U_URL)
        response.raise_for_status()
        return set(re.findall(r'tvg-id="([^"]+)"', response.text))
    except Exception as e:
        print(f"Error fetching M3U: {e}")
        return set()

def download_and_filter_epg():
    """Download EPG and adapt it to match M3U tvg-ids"""
    tvg_ids = get_m3u_tvg_ids()
    if not tvg_ids:
        print("No tvg-ids found in M3U")
        return False

    try:
        # Download EPG
        print(f"Downloading EPG from {EPG_SOURCE}...")
        response = requests.get(EPG_SOURCE, stream=True, timeout=30)
        response.raise_for_status()
        
        # Decompress and parse
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
            epg_content = gz_file.read().decode('utf-8')
        
        print("Processing EPG data...")
        root = ET.fromstring(epg_content)
        
        # Create new EPG structure
        new_root = ET.Element("tv")
        channel_id_map = {}
        matched_channels = 0
        matched_programs = 0
        
        # First pass: find matching channels and create mapping
        for channel in root.findall("channel"):
            epg_id = channel.get("id", "")
            
            # Check if EPG channel matches any tvg-id (case insensitive)
            for tvg_id in tvg_ids:
                if epg_id.lower() == tvg_id.lower():
                    # Create new channel node with exact tvg-id from M3U
                    new_channel = ET.SubElement(new_root, "channel", id=tvg_id)
                    
                    # Copy all display names
                    for display in channel.findall("display-name"):
                        ET.SubElement(new_channel, "display-name").text = display.text
                    
                    # Copy other elements (icon, etc.)
                    for child in channel:
                        if child.tag not in ["display-name"]:
                            new_child = ET.SubElement(new_channel, child.tag)
                            new_child.text = child.text
                            for k, v in child.attrib.items():
                                new_child.set(k, v)
                    
                    channel_id_map[epg_id] = tvg_id
                    matched_channels += 1
                    break

        # Second pass: copy programs for matched channels
        for program in root.findall("programme"):
            original_id = program.get("channel", "")
            if original_id in channel_id_map:
                # Create program with corrected channel ID
                new_program = ET.SubElement(new_root, "programme", {
                    "channel": channel_id_map[original_id],
                    "start": program.get("start", ""),
                    "stop": program.get("stop", "")
                })
                
                # Copy all program elements
                for child in program:
                    new_child = ET.SubElement(new_program, child.tag)
                    new_child.text = child.text
                    for k, v in child.attrib.items():
                        new_child.set(k, v)
                
                matched_programs += 1

        print(f"Matched {matched_channels} channels and {matched_programs} programs")
        
        # Save the new EPG
        tree = ET.ElementTree(new_root)
        tree.write(OUTPUT_FILE, encoding='utf-8', xml_declaration=True)
        
        # Add DOCTYPE manually
        with open(OUTPUT_FILE, 'r+', encoding='utf-8') as f:
            content = f.read()
            f.seek(0, 0)
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE tv SYSTEM "xmltv.dtd">\n' + content)
        
        return True

    except Exception as e:
        print(f"EPG processing error: {e}")
        return False

if __name__ == "__main__":
    if download_and_filter_epg():
        print(f"Successfully generated compatible EPG at {OUTPUT_FILE}")
    else:
        print("Failed to generate EPG")
