import requests
import json
import re
import subprocess
import os
import logging
from datetime import datetime

def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"playlist_generation_{timestamp}.log")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    logger.info(f"Logging started. Log file: {log_filename}")
    return logger

logger = setup_logging()

def is_youtube_url(url):
    youtube_patterns = [
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/watch\?v=([a-zA-Z0-9_-]+)",
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/live/([a-zA-Z0-9_-]+)",
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/embed/([a-zA-Z0-9_-]+)",
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/v/([a-zA-Z0-9_-]+)",
        r"(?:https?://)?youtu\.be/([a-zA-Z0-9_-]+)",
        r"http://googleusercontent\.com/youtube\.com/([a-zA-Z0-9_-]+)"
    ]
    for pattern in youtube_patterns:
        if re.match(pattern, url):
            return True
    return False

def get_youtube_m3u8_link(youtube_url):
    command_hls = ['yt-dlp', '-f', 'hls', '-g', youtube_url]
    command_general = ['yt-dlp', '-g', youtube_url]

    try:
        logger.info(f"  Attempting yt-dlp HLS extraction for YouTube URL: {youtube_url}")
        result = subprocess.run(command_hls, capture_output=True, text=True, check=True, timeout=30)
        m3u8_url = result.stdout.strip()

        if "m3u8" in m3u8_url:
            logger.info(f"  Successfully extracted HLS (M3U8) URL: {m3u8_url}")
            return m3u8_url
        else:
            logger.info(f"  HLS format not found directly. Trying general extraction.")
            result = subprocess.run(command_general, capture_output=True, text=True, check=True, timeout=30)
            general_url = result.stdout.strip()
            if "m3u8" in general_url:
                logger.info(f"  Successfully extracted M3U8 URL via general extraction: {general_url}")
                return general_url
            else:
                logger.warning(f"  Could not find a suitable M3U8 URL for {youtube_url}.")
                return None
    except subprocess.CalledProcessError as e:
        logger.error(f"  Error extracting YouTube URL with yt-dlp for {youtube_url}:")
        logger.error(f"  Return Code: {e.returncode}")
        logger.error(f"  Stderr: {e.stderr.strip()}")
        return None
    except subprocess.TimeoutExpired:
        logger.error(f"  yt-dlp command timed out for {youtube_url}. Stream might be unavailable or network issues.")
        return None
    except FileNotFoundError:
        logger.critical("  Error: 'yt-dlp' command not found. Please ensure yt-dlp is installed and in your system's PATH.")
        logger.critical("  You can install it with: pip install yt-dlp")
        return None
    except Exception as e:
        logger.error(f"  An unexpected error occurred during YouTube URL extraction for {youtube_url}: {e}")
        return None

def get_final_url(url):
    try:
        session = requests.Session()
        session.max_redirects = 5
        response = session.head(url, allow_redirects=False, timeout=10)
        if 300 <= response.status_code < 400:
            logger.info(f"  Redirected URL: {url} -> {response.headers['Location']}")
            return response.headers['Location']
        logger.info(f"  No redirection for URL: {url}")
        return url
    except requests.exceptions.RequestException as e:
        logger.warning(f"  Failed to get final URL for {url}: {e}")
        return url

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
    logger.info("--- Starting M3U Playlist Generation ---")
    manifest_url = 'https://hilaytv.vercel.app/manifest.json'
    catalog_url = 'https://hilaytv.vercel.app/catalog/tv/maldives.json'

    try:
        logger.info(f"Fetching manifest from: {manifest_url}")
        response = requests.get(manifest_url, timeout=10)
        response.raise_for_status()
        manifest = response.json()
        logger.info("Manifest fetched successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch manifest.json: {e}")
        return

    try:
        logger.info(f"Fetching catalog from: {catalog_url}")
        response = requests.get(catalog_url, timeout=10)
        response.raise_for_status()
        catalog = response.json()
        catalog_metas = catalog.get("metas", [])
        logger.info("Catalog fetched successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch catalog: {e}")
        return

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
    logger.info(f"Built ID data map for {len(id_data_map)} channels.")

    m3u_content = "#EXTM3U\n\n"

    processed_count = 0
    total_prefixes = len(manifest.get('idPrefixes', []))
    logger.info(f"Processing {total_prefixes} channel prefixes from manifest.")

    for i, prefix in enumerate(manifest.get('idPrefixes', [])):
        clean_prefix = prefix.lower().replace(".mv", "")
        channel_url_primary = f"https://hilaytv.vercel.app/stream/tv/{prefix}.mv.json"
        channel_url_fallback = f"https://hilaytv.vercel.app/stream/tv/{prefix}.json"

        channel_data = None
        logger.info(f"\n--- Processing Channel {i+1}/{total_prefixes}: {prefix} ---")
        try:
            logger.info(f"  Attempting primary URL: {channel_url_primary}")
            response_primary = requests.get(channel_url_primary, timeout=10)
            response_primary.raise_for_status()
            channel_data = response_primary.json()
            logger.info(f"  Primary URL successful for {prefix}.")
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            logger.warning(f"  Primary URL failed for {prefix} ({channel_url_primary}): {e}. Trying fallback.")
            channel_data = None

        if not channel_data or not channel_data.get('streams'):
            try:
                logger.info(f"  Attempting fallback URL: {channel_url_fallback}")
                response_fallback = requests.get(channel_url_fallback, timeout=10)
                response_fallback.raise_for_status()
                channel_data = response_fallback.json()
                if channel_data and channel_data.get('streams'):
                    logger.info(f"  Successfully used fallback URL for {prefix}")
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.error(f"  Fallback URL also failed for {prefix} ({channel_url_fallback}): {e}. Skipping channel.")
                continue

        if channel_data and channel_data.get('streams'):
            stream = channel_data['streams'][0]
            name = stream.get('name', prefix)
            original_url = stream.get('url', '')
            if not original_url:
                logger.warning(f"  Skipping {prefix}: No URL found after trying both channels JSON.")
                continue

            processed_url = original_url
            logger.info(f"  Initial URL from JSON: {original_url}")

            # First, check if the original URL is a YouTube link
            if is_youtube_url(original_url):
                logger.info(f"  Original URL is a YouTube link: {original_url}")
                youtube_m3u8 = get_youtube_m3u8_link(original_url)
                if youtube_m3u8:
                    processed_url = youtube_m3u8
                    logger.info(f"  SUCCESS: Converted original YouTube URL to M3U8: {processed_url}")
                else:
                    logger.warning(f"  FAILURE: get_youtube_m3u8_link returned None for original YouTube URL. Attempting redirect check.")
                    # If original YouTube conversion fails, still try to follow redirects
                    final_redirect_url = get_final_url(original_url)
                    if final_redirect_url != original_url and is_youtube_url(final_redirect_url):
                        logger.info(f"  Redirected URL is also a YouTube link: {final_redirect_url}. Attempting yt-dlp conversion.")
                        youtube_m3u8_redirect = get_youtube_m3u8_link(final_redirect_url)
                        if youtube_m3u8_redirect:
                            processed_url = youtube_m3u8_redirect
                            logger.info(f"  SUCCESS: Converted redirected YouTube URL to M3U8: {processed_url}")
                        else:
                            logger.warning(f"  FAILURE: get_youtube_m3u8_link returned None for redirected YouTube URL. Using original URL as final.")
                    else:
                        logger.info(f"  Redirected URL is not a YouTube link or no redirect occurred. Using original URL as final.")
            else:
                logger.info(f"  Original URL is NOT a YouTube link: {original_url}. Checking for redirects.")
                # For non-YouTube URLs, proceed with existing redirection logic
                final_redirect_url = get_final_url(original_url)
                if final_redirect_url != original_url:
                    logger.info(f"  Original URL redirected to: {final_redirect_url}")
                    # Now, check if the redirected URL is a YouTube link
                    if is_youtube_url(final_redirect_url):
                        logger.info(f"  Redirected URL is a YouTube link: {final_redirect_url}. Attempting yt-dlp conversion.")
                        youtube_m3u8_redirect = get_youtube_m3u8_link(final_redirect_url)
                        if youtube_m3u8_redirect:
                            processed_url = youtube_m3u8_redirect
                            logger.info(f"  SUCCESS: Converted redirected YouTube URL to M3U8: {processed_url}")
                        else:
                            logger.warning(f"  FAILURE: get_youtube_m3u8_link returned None for redirected YouTube URL. Using redirected URL as final.")
                            processed_url = final_redirect_url # Use the redirected URL even if yt-dlp fails
                    else:
                        logger.info(f"  Redirected URL is NOT a YouTube link. Using redirected URL as final.")
                        processed_url = final_redirect_url
                else:
                    logger.info(f"  No redirection occurred. Using original URL as final.")
                    processed_url = original_url # No change, keep original_url

            logo = ""
            genre = "General"
            if clean_prefix in id_data_map:
                logo = id_data_map[clean_prefix]["logo"]
                genre = id_data_map[clean_prefix]["genre"]
            logger.info(f"  Lookup: Logo='{logo}', Genre='{genre}'")

            m3u_content += f'#EXTINF:-1 tvg-id="{prefix}" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre}",{name}\n'
            m3u_content += f"{processed_url}\n\n"

            logger.info(f"  Final URL for M3U: {processed_url}")
            processed_count += 1
        else:
            logger.warning(f"Skipping {prefix}: No stream data found in either URL after trying both primary and fallback.")

    filename = 'peartv.m3u'
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(m3u_content)
        logger.info(f"\nM3U playlist generated: {filename}")
        logger.info(f"Total channels processed: {processed_count}/{total_prefixes}")
    except IOError as e:
        logger.critical(f"Failed to write M3U file '{filename}': {e}")
        return

    logger.info("--- M3U Playlist Generation Completed ---")

if __name__ == "__main__":
    generate_m3u_playlist()
