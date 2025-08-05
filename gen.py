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
    print(f"⏳ Fetching playlist from {url}")
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
        
        # Verify we got actual M3U content
        if not response.text.strip().startswith("#EXTM3U"):
            print("⚠️ Warning: Response doesn't start with #EXTM3U - may not be valid M3U")
        
        print(f"✅ Successfully fetched playlist ({len(response.text)} bytes)")
        return response.text
    except requests.RequestException as e:
        print(f"❌ Error fetching playlist: {e}")
        return None

def remove_duplicates(playlist_content):
    """Remove duplicate entries from the M3U playlist"""
    if not playlist_content:
        print("❌ Empty playlist content received")
        return None
    
    print("🔍 Processing playlist content...")
    
    lines = playlist_content.splitlines()
    unique_entries = set()
    output_lines = []
    current_entry = []
    entry_hash = None
    total_entries = 0
    duplicate_count = 0
    
    for line in lines:
        if line.startswith("#EXTINF:"):
            # Start of a new entry
            if current_entry and entry_hash:
                # If we have a previous entry that wasn't completed
                if entry_hash not in unique_entries:
                    unique_entries.add(entry_hash)
                    output_lines.extend(current_entry)
                    output_lines.append("")  # Add empty line between entries
                    total_entries += 1
                else:
                    duplicate_count += 1
            
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
                    total_entries += 1
                else:
                    duplicate_count += 1
                
                current_entry = []
                entry_hash = None
        else:
            # Empty line or other content
            if current_entry:
                current_entry.append(line)
    
    # Handle any remaining entry
    if current_entry and entry_hash:
        if entry_hash not in unique_entries:
            unique_entries.add(entry_hash)
            output_lines.extend(current_entry)
            output_lines.append("")
            total_entries += 1
        else:
            duplicate_count += 1
    
    # Join the lines and ensure proper M3U format
    processed_content = "\n".join(output_lines).strip()
    if not processed_content.startswith("#EXTM3U"):
        processed_content = "#EXTM3U\n" + processed_content
    
    print(f"📊 Processing complete: {total_entries} unique entries, {duplicate_count} duplicates removed")
    return processed_content

def save_playlist(content, filename):
    """Save the processed playlist to a file"""
    print(f"💾 Saving processed playlist to {filename}")
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        
        # Verify the saved file
        with open(filename, "r", encoding="utf-8") as f:
            saved_content = f.read()
            line_count = len(saved_content.splitlines())
            print(f"✅ Successfully saved playlist ({len(saved_content)} bytes, {line_count} lines)")
            return True
    except IOError as e:
        print(f"❌ Error saving playlist: {e}")
        return False

def verify_playlist(filename):
    """Verify the processed playlist"""
    print(f"🔎 Verifying {filename}...")
    try:
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
        
        if not content:
            print("❌ Error: Playlist file is empty")
            return False
        
        if not content.startswith("#EXTM3U"):
            print("⚠️ Warning: Processed playlist doesn't start with #EXTM3U")
        
        lines = content.splitlines()
        extinf_count = sum(1 for line in lines if line.startswith("#EXTINF:"))
        url_count = sum(1 for line in lines if line.strip() and not line.startswith("#"))
        
        print(f"📊 Verification: {extinf_count} EXTINF entries, {url_count} URLs")
        
        if extinf_count != url_count:
            print("⚠️ Warning: EXTINF count doesn't match URL count")
        
        return True
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

def main():
    print(f"\n🔄 Processing started at {datetime.now().isoformat()}")
    print("="*50)
    
    # Fetch the original playlist
    original_content = fetch_playlist(SOURCE_URL)
    
    if not original_content:
        print("❌ Aborting due to fetch error")
        return
    
    # Process the playlist
    processed_content = remove_duplicates(original_content)
    
    if not processed_content:
        print("❌ Aborting due to processing error")
        return
    
    # Save the processed playlist
    if not save_playlist(processed_content, OUTPUT_FILE):
        print("❌ Aborting due to save error")
        return
    
    # Verify the saved playlist
    verify_playlist(OUTPUT_FILE)
    
    print("="*50)
    print(f"✅ Processing completed at {datetime.now().isoformat()}\n")

if __name__ == "__main__":
    main()
