import requests
from pathlib import Path
from urllib.parse import urlparse
import hashlib
from datetime import datetime

# Configuration
SOURCE_URL = "http://hilay.tv/play.m3u"
OUTPUT_FILE = "playlist.m3u"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
REFERER = "http://hilay.tv/"

def fetch_playlist(url):
    """Fetch the playlist with proper headers to avoid bot detection"""
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": REFERER,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching playlist: {e}")
        return None

def remove_duplicates(playlist_content):
    """Remove duplicate entries from the M3U playlist"""
    if not playlist_content:
        return None
    
    lines = playlist_content.splitlines()
    unique_entries = set()
    output_lines = []
    current_entry = []
    entry_hash = None
    
    for line in lines:
        if line.startswith("#EXTINF:"):
            # Start of a new entry
            current_entry = [line]
            entry_hash = hashlib.md5(line.encode()).hexdigest()
        elif line.startswith("#"):
            # Metadata line, add to current entry
            if current_entry:
                current_entry.append(line)
                entry_hash = hashlib.md5((entry_hash + line).encode()).hexdigest()
        elif line.strip() and not line.startswith("#"):
            # URL line
            if current_entry:
                current_entry.append(line)
                entry_hash = hashlib.md5((entry_hash + line).encode()).hexdigest()
                
                if entry_hash not in unique_entries:
                    unique_entries.add(entry_hash)
                    output_lines.extend(current_entry)
                    output_lines.append("")  # Add empty line between entries
                
                current_entry = []
                entry_hash = None
        else:
            # Empty line or other content
            if current_entry:
                current_entry.append(line)
    
    # Join the lines and ensure proper M3U format
    processed_content = "\n".join(output_lines).strip()
    if not processed_content.startswith("#EXTM3U"):
        processed_content = "#EXTM3U\n" + processed_content
    
    return processed_content

def save_playlist(content, filename):
    """Save the processed playlist to a file"""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Playlist saved to {filename}")
        return True
    except IOError as e:
        print(f"Error saving playlist: {e}")
        return False

def main():
    print(f"Processing playlist at {datetime.now().isoformat()}")
    
    # Fetch the original playlist
    print("Fetching original playlist...")
    original_content = fetch_playlist(SOURCE_URL)
    
    if not original_content:
        print("Failed to fetch original playlist. Exiting.")
        return
    
    # Process the playlist
    print("Removing duplicates...")
    processed_content = remove_duplicates(original_content)
    
    if not processed_content:
        print("Failed to process playlist. Exiting.")
        return
    
    # Save the processed playlist
    print("Saving processed playlist...")
    if save_playlist(processed_content, OUTPUT_FILE):
        print("Playlist processing completed successfully.")
    else:
        print("Playlist processing completed with errors.")

if __name__ == "__main__":
    main()
