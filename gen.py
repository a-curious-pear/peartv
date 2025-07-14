import requests
import json
import re
import subprocess
import os # Added for os.getenv in get_youtube_m3u8_link

# --- NEW: YouTube Conversion Imports and Functions ---
def is_youtube_url(url):
    """Checks if a given URL is a YouTube video/live stream URL."""
    youtube_patterns = [
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/watch\?v=([a-zA-Z0-9_-]+)",
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/live/([a-zA-Z0-9_-]+)",
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/embed/([a-zA-Z0-9_-]+)",
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/v/([a-zA-Z0-9_-]+)",
        r"(?:https?://)?youtu\.be/([a-zA-Z0-9_-]+)"
    ]
    for pattern in youtube_patterns:
        if re.match(pattern, url):
            return True
    return False

def get_youtube_m3u8_link(youtube_url):
    """
    Extracts the M3U8 (HLS) live stream URL from a YouTube video URL using yt-dlp.
    Returns the M3U8 URL or None if extraction fails.
    """
    # Using 'hls' format explicitly for live streams, and '-g' to get the URL
    command_hls = ['yt-dlp', '-f', 'hls', '-g', youtube_url]
    command_general = ['yt-dlp', '-g', youtube_url] # Fallback

    try:
        print(f"  Attempting yt-dlp HLS extraction for YouTube URL: {youtube_url}")
        result = subprocess.run(command_hls, capture_output=True, text=True, check=True, timeout=30)
        m3u8_url = result.stdout.strip()

        if "m3u8" in m3u8_url:
            print(f"  Successfully extracted HLS (M3U8) URL: {m3u8_url}")
            return m3u8_url
        else:
            print(f"  HLS format not found directly. Trying general extraction.")
            result = subprocess.run(command_general, capture_output=True, text=True, check=True, timeout=30)
            general_url = result.stdout.strip()
            if "m3u8" in general_url:
                print(f"  Successfully extracted M3U8 URL via general extraction: {general_url}")
                return general_url
            else:
                print(f"  Could not find a suitable M3U8 URL for {youtube_url}.")
                return None
    except subprocess.CalledProcessError as e:
        print(f"  Error extracting YouTube URL with yt-dlp for {youtube_url}: {e.stderr.strip()}")
        return None
    except subprocess.TimeoutExpired:
        print(f"  yt-dlp command timed out for {youtube_url}. Stream might be unavailable.")
        return None
    except FileNotFoundError:
        print("  Error: 'yt-dlp' command not found. Please ensure yt-dlp is installed and in your system's PATH.")
        print("  You can install it with: pip install yt-dlp")
        return None
    except Exception as e:
        print(f"  An unexpected error occurred during YouTube URL extraction for {youtube_url}: {e}")
        return None
# --- END NEW ---

def get_final_url(url):
    try:
        session = requests.Session()
        session.max_redirects = 5
        response = session.head(url, allow_redirects=False, timeout=10)
        if 300 <= response.status_code < 400:
            return response.headers['Location']
        return url
    except requests.exceptions.RequestException:
        return url

# classification mapping
genre_keywords_map = {
    "news": "News",
    "current affairs": "News",
    "live": "News",
    "documentary": "Documentary",
    "sports": "Sports",
    "soccer": "Sports",
    "football": "Sports",
    "cricket": "Sports",
    "billiard": "Sports",
    "league": "Sports",
    "politics": "Politics",
    "parliament": "Politics",
    "government": "Government",
    "kids": "Kids",
    "child": "Kids",
    "learning": "Kids",
    "entertainment": "Entertainment",
    "culture": "Entertainment",
    "events": "Entertainment",
    "music": "Music",
    "radio": "Radio",
    "education": "Education",
    "movies": "Movies",
    "cinema": "Movies",
    "lifestyle": "Lifestyle",
    "shopping": "Shopping",
    "docu": "Documentary",
    "info": "General",
    "general": "General",
}

def classify_genre_smart(genres_list):
    combined = " ".join(genres_list).lower()
    if any(x in combined for x in ["islam", "muslim", "quran", "quruan"]):
        return "Religion"
    for keyword, category in genre_keywords_map.items():
        if keyword in combined:
            return category
    return "General"

def generate_m3u_playlist():
    manifest_url = 'https://hilaytv.vercel.app/manifest.json'
    catalog_url = 'https://hilaytv.vercel.app/catalog/tv/maldives.json'

    # Fetch manifest
    try:
        response = requests.get(manifest_url, timeout=10)
        response.raise_for_status()
        manifest = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch manifest.json: {e}")
        return

    # Fetch catalog
    try:
        response = requests.get(catalog_url, timeout=10)
        response.raise_for_status()
        catalog = response.json()
        catalog_metas = catalog.get("metas", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch catalog: {e}")
        return

    # Build id lookup for logo and genre
    id_data_map = {}
    for channel in catalog_metas:
        channel_id = channel.get("id", "").lower().replace(".mv", "")
        logo = channel.get("logo", "")
        genres_list = []
        if "genres" in channel:
            for g in channel["genres"]:
                genres_list.append(g.split("|")[0].strip())
        genre = classify_genre_smart(genres_list)
        id_data_map[channel_id] = {"logo": logo, "genre": genre}

    # Prepare M3U header
    m3u_content = "#EXTM3U\n\n"

    processed_count = 0
    for prefix in manifest.get('idPrefixes', []):
        clean_prefix = prefix.lower().replace(".mv", "")
        channel_url_primary = f"https://hilaytv.vercel.app/stream/tv/{prefix}.mv.json"
        channel_url_fallback = f"https://hilaytv.vercel.app/stream/tv/{prefix}.json"

        channel_data = None
        try:
            response_primary = requests.get(channel_url_primary, timeout=10)
            response_primary.raise_for_status()
            channel_data = response_primary.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Primary URL failed for {prefix} ({channel_url_primary}): {e}. Trying fallback.")
            channel_data = None

        if not channel_data or not channel_data.get('streams'):
            try:
                response_fallback = requests.get(channel_url_fallback, timeout=10)
                response_fallback.raise_for_status()
                channel_data = response_fallback.json()
                if channel_data and channel_data.get('streams'):
                    print(f"Successfully used fallback URL for {prefix}")
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                print(f"Fallback URL also failed for {prefix} ({channel_url_fallback}): {e}. Skipping.")
                continue

        if channel_data and channel_data.get('streams'):
            stream = channel_data['streams'][0]
            name = stream.get('name', prefix)
            original_url = stream.get('url', '')
            if not original_url:
                print(f"Skipping {prefix}: No URL found after trying both channels")
                continue

            # --- NEW: YouTube URL Conversion Logic ---
            processed_url = original_url
            if is_youtube_url(original_url):
                print(f"Detected YouTube URL for {name}: {original_url}")
                youtube_m3u8 = get_youtube_m3u8_link(original_url)
                if youtube_m3u8:
                    processed_url = youtube_m3u8
                    print(f"  Converted YouTube URL to M3U8: {processed_url}")
                else:
                    print(f"  Failed to convert YouTube URL for {name}. Using original URL as fallback.")
            else:
                # For non-YouTube URLs, proceed with existing redirection logic
                processed_url = get_final_url(original_url)
            # --- END NEW ---

            # Lookup logo and genre
            logo = ""
            genre = "General"
            if clean_prefix in id_data_map:
                logo = id_data_map[clean_prefix]["logo"]
                genre = id_data_map[clean_prefix]["genre"]

            m3u_content += f'#EXTINF:-1 tvg-id="{prefix}" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre}",{name}\n'
            m3u_content += f"{processed_url}\n\n"

            print(f"Processed: {name} - Original: {original_url} - Final/Processed: {processed_url}")
            processed_count += 1
        else:
            print(f"Skipping {prefix}: No stream data found in either URL.")

    filename = 'peartv.m3u'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(m3u_content)

    print(f"\nM3U playlist generated: {filename}")
    print(f"Total channels processed: {processed_count}/{len(manifest.get('idPrefixes', []))}")

if __name__ == "__main__":
    generate_m3u_playlist()
