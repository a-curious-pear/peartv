#!/usr/bin/env python3
"""
Memory-efficient EPG Generator for Large Files
Processes 300MB+ EPG files without loading entire file into memory
Generates custom_epg.xml with exact tvg-id matching
"""

import os
import re
import requests
import gzip
import io
import xml.sax
from xml.sax.handler import ContentHandler
from datetime import datetime

# Configuration
M3U_URL = "https://raw.githubusercontent.com/a-curious-pear/peartv/main/peartv.m3u"
EPG_SOURCE = "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
OUTPUT_FILE = "custom_epg.xml"
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming

class M3UParser:
    """Extracts tvg-ids from M3U file"""
    @staticmethod
    def get_tvg_ids():
        try:
            response = requests.get(M3U_URL)
            response.raise_for_status()
            return set(re.findall(r'tvg-id="([^"]+)"', response.text))
        except Exception as e:
            print(f"Error fetching M3U: {e}")
            return set()

class EPGFilter(ContentHandler):
    """SAX parser to filter and rewrite EPG with matching tvg-ids"""
    def __init__(self, output_file, tvg_ids):
        self.output_file = output_file
        self.tvg_ids = {id.lower(): id for id in tvg_ids}  # Case-insensitive lookup
        self.current_channel = None
        self.matched_channels = set()
        self.in_programme = False
        self.buffer = []
        self.program_count = 0
        self.channel_count = 0

    def startDocument(self):
        self.output_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        self.output_file.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        self.output_file.write('<tv>\n')

    def endDocument(self):
        self.output_file.write('</tv>\n')
        print(f"Matched {self.channel_count} channels with {self.program_count} programs")

    def startElement(self, name, attrs):
        if name == "channel":
            epg_id = attrs.get("id", "").lower()
            if epg_id in self.tvg_ids:
                self.current_channel = self.tvg_ids[epg_id]
                self.buffer.append(f'<channel id="{self.current_channel}">')
                self.channel_count += 1
                self.matched_channels.add(epg_id)
        elif name == "programme":
            channel = attrs.get("channel", "").lower()
            if channel in self.matched_channels:
                self.in_programme = True
                self.buffer.append(
                    f'<programme channel="{self.tvg_ids[channel]}" '
                    f'start="{attrs.get("start", "")}" '
                    f'stop="{attrs.get("stop", "")}">'
                )
                self.program_count += 1
        elif self.current_channel or self.in_programme:
            self.buffer.append(f'<{name}')
            for k, v in attrs.items():
                self.buffer.append(f' {k}="{v}"')
            self.buffer.append('>')

    def characters(self, content):
        if self.buffer:
            self.buffer.append(content)

    def endElement(self, name):
        if name == "channel" and self.current_channel:
            self.buffer.append('</channel>')
            self._flush_buffer()
            self.current_channel = None
        elif name == "programme" and self.in_programme:
            self.buffer.append('</programme>')
            self._flush_buffer()
            self.in_programme = False
        elif self.buffer:
            self.buffer.append(f'</{name}>')

    def _flush_buffer(self):
        if self.buffer:
            self.output_file.write(''.join(self.buffer))
            self.buffer = []

def download_epg():
    """Stream download and process EPG with minimal memory usage"""
    tvg_ids = M3UParser.get_tvg_ids()
    if not tvg_ids:
        print("No tvg-ids found in M3U")
        return False

    try:
        print(f"Downloading and processing {EPG_SOURCE}...")
        with requests.get(EPG_SOURCE, stream=True, timeout=60) as response:
            response.raise_for_status()
            
            with gzip.GzipFile(fileobj=response.raw) as gz_file:
                with open(OUTPUT_FILE, 'w', encoding='utf-8') as out_file:
                    parser = xml.sax.make_parser()
                    parser.setContentHandler(EPGFilter(out_file, tvg_ids))
                    
                    # Process in chunks to avoid memory overload
                    while True:
                        chunk = gz_file.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        parser.feed(chunk)
                    
                    parser.close()
        
        print(f"EPG successfully generated at {OUTPUT_FILE}")
        return True

    except Exception as e:
        print(f"EPG processing error: {e}")
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)
        return False

if __name__ == "__main__":
    download_epg()
