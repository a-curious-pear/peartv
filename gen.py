import requests
import json
import re
import pandas as pd
from thefuzz import process, fuzz
from io import StringIO
import random
import time
from fake_useragent import UserAgent
import gzip
import brotli

try:
    ua = UserAgent()
    user_agent_source_type = "fake"
except Exception as e:
    print(f"WARNING: Could not initialize fake_useragent, falling back to static User-Agents: {e}")
    STATIC_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/126.0.0.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.3 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 9; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Mobile Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 12_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; U; Android 8.1.0; en-us; Redmi 5 Build/OPM1.171019.011) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/61.0.3163.128 Mobile Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
        "Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)",
        "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36",
        "Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Mobile Safari/537.36 Edge/15.15254",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36",
        "Mozilla/5.0 (X11; Linux i686; rv:45.0) Gecko/20100101 Firefox/45.0",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)",
        "Mozilla/5.0 (Android 4.4; Mobile; rv:41.0) Gecko/41.0 Firefox/41.0",
        "Mozilla/5.0 (X11; CrOS armv7l 13597.84.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        "Mozilla/5.0 (Linux; U; Android 7.0; en-us; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 OPR/77.0.4054.203",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    ]
    ua = None
    user_agent_source_type = "static"

def get_random_user_agent():
    if ua:
        return ua.random
    else:
        return random.choice(STATIC_USER_AGENTS)

def get_random_delay(min_sec=1, max_sec=4):
    return random.uniform(min_sec, max_sec)

REFERER_DOMAINS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    "https://hilaytv.vercel.app/",
    "https://www.facebook.com/",
    "https://t.co/",
    "http://googleusercontent.com/",
    "https://search.yahoo.com/",
    "https://www.youtube.com/",
    "https://www.wikipedia.org/",
    "https://www.reddit.com/",
    "https://news.ycombinator.com/",
    "https://www.linkedin.com/",
    None
]

ACCEPT_HEADERS = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "application/json, text/javascript, */*; q=0.01",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "*/*"
]

ACCEPT_ENCODINGS = [
    "gzip, deflate, br", # Added 'br' for Brotli
    "gzip, deflate",
    "br",
    "identity"
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.8",
    "q=0.8,en-US;q=0.6,en;q=0.4",
    "en-CA,en;q=0.9",
    "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
]

SEC_FETCH_DESTS = ["document", "empty", "image", "script", "style", "font"]
SEC_FETCH_MODES = ["navigate", "no-cors", "cors"]
SEC_FETCH_SITES = ["none", "cross-site", "same-origin", "same-site"]

def get_dynamic_headers():
    selected_user_agent = get_random_user_agent()
    current_source_type = "fake" if ua else "static"
    print(f"INFO: Using User-Agent ({current_source_type}): {selected_user_agent}")

    headers = {
        "User-Agent": selected_user_agent,
        "Accept": random.choice(ACCEPT_HEADERS),
        "Accept-Encoding": random.choice(ACCEPT_ENCODINGS), # Ensure 'br' is in Accept-Encoding
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1" if random.random() < 0.8 else None,
        "Sec-Fetch-Dest": random.choice(SEC_FETCH_DESTS),
        "Sec-Fetch-Mode": random.choice(SEC_FETCH_MODES),
        "Sec-Fetch-Site": random.choice(SEC_FETCH_SITES),
        "Sec-Fetch-User": "?1" if random.random() < 0.5 else None,
        "TE": "Trailers" if random.random() < 0.1 else None,
        "Pragma": "no-cache" if random.random() < 0.2 else None,
        "Cache-Control": "no-cache" if random.random() < 0.2 else None,
    }

    referer = random.choice(REFERER_DOMAINS)
    if referer:
        headers["Referer"] = referer

    return {k: v for k, v in headers.items() if v is not None}

def decode_response_content(response):
    """
    Attempts to decode response content as JSON, handling potential gzip/brotli compression
    even if Content-Encoding header is missing or incorrect.
    """
    # First, try requests' built-in .json() which handles common encodings
    try:
        return response.json()
    except json.JSONDecodeError:
        # If .json() fails, try manual decompression based on common types
        content_type = response.headers.get('Content-Type', '').lower()
        content_encoding = response.headers.get('Content-Encoding', '').lower()

        # Try Gzip decompression
        if 'gzip' in content_encoding or (response.content and response.content.startswith(b'\x1f\x8b')):
            try:
                decompressed_data = gzip.decompress(response.content).decode('utf-8')
                return json.loads(decompressed_data)
            except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"DEBUG: Gzip decompression failed: {e}")

        # Try Brotli decompression if brotli library is available
        if 'br' in content_encoding or (brotli and response.content and response.content.startswith(b'\x0b')): # Brotli magic number can be tricky, this is a simplified check
            try:
                if brotli: # Check if brotli library was successfully imported
                    decompressed_data = brotli.decompress(response.content).decode('utf-8')
                    return json.loads(decompressed_data)
            except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"DEBUG: Brotli decompression failed: {e}")
            except NameError: # brotli not imported
                pass

        # If all else fails, raise the original JSONDecodeError
        raise

def get_final_url(url):
    try:
        session = requests.Session()
        session.headers.update(get_dynamic_headers())
        session.max_redirects = 5
        response = session.head(url, allow_redirects=False, timeout=10)
        time.sleep(get_random_delay(0.5, 2))
        if 300 <= response.status_code < 400:
            return response.headers['Location']
        return url
    except requests.exceptions.RequestException as e:
        print(f"DEBUG: Failed to get final URL for {url}: {e}")
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

def fetch_iptv_channels_csv(url):
    print(f"INFO: Attempting to fetch channels.csv from {url}")
    try:
        session = requests.Session()
        session.headers.update(get_dynamic_headers())
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        time.sleep(get_random_delay(1, 3))
        
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)

        if 'id' not in df.columns:
            print(f"ERROR: 'id' column not found in {url}. Cannot use for logo lookup.")
            return {}, []

        logo_column_name = df.columns[-1]

        iptv_id_to_logo_map = {}
        original_iptv_ids = []
        for index, row in df.iterrows():
            channel_id = str(row['id']).lower()
            logo_url = str(row[logo_column_name])
            if channel_id and logo_url:
                iptv_id_to_logo_map[channel_id] = logo_url
                original_iptv_ids.append(channel_id)

        print(f"INFO: Successfully loaded {len(iptv_id_to_logo_map)} entries from channels.csv.")
        return iptv_id_to_logo_map, original_iptv_ids

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch channels.csv from {url}: {e}")
        return {}, []
    except pd.errors.EmptyDataError:
        print(f"ERROR: channels.csv from {url} is empty.")
        return {}, []
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while processing channels.csv: {e}")
        return {}, []

def generate_m3u_playlist():
    print("INFO: Starting M3U playlist generation.")
    manifest_url = 'https://hilaytv.vercel.app/manifest.json'
    catalog_url = 'https://hilaytv.vercel.app/catalog/tv/maldives.json'
    iptv_channels_csv_url = 'https://raw.githubusercontent.com/iptv-org/database/refs/heads/master/data/channels.csv'
    
    session = requests.Session()

    MAX_RETRIES = 5
    manifest = {}
    for attempt in range(MAX_RETRIES):
        print(f"INFO: Fetching manifest from {manifest_url} (Attempt {attempt + 1}/{MAX_RETRIES})")
        try:
            session.headers.update(get_dynamic_headers())
            response = session.get(manifest_url, timeout=random.uniform(8, 12))
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            manifest = decode_response_content(response) # Use the new decoding function
            print(f"INFO: Successfully fetched manifest.json with {len(manifest.get('idPrefixes', []))} idPrefixes.")
            time.sleep(get_random_delay(1, 3))
            break
        except json.JSONDecodeError as e:
            # If decode_response_content couldn't handle it, it raises JSONDecodeError
            print(f"CRITICAL: Failed to decode JSON from manifest URL: {e}. Status Code: {response.status_code}. Response Text (first 500 chars): {response.text[:500]} Retrying...")
            time.sleep(get_random_delay(2, 5))
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                print(f"WARNING: Manifest URL returned 403 Forbidden. Retrying with new User-Agent...")
                time.sleep(get_random_delay(2, 5))
            else:
                print(f"CRITICAL: Failed to fetch manifest.json with HTTP error: {e}. Status: {response.status_code}. Skipping retries.")
                return
        except requests.exceptions.RequestException as e:
            print(f"CRITICAL: Failed to fetch manifest.json due to network/other error: {e}. Retrying...")
            time.sleep(get_random_delay(2, 5))
    else:
        print(f"CRITICAL: Failed to fetch manifest.json after {MAX_RETRIES} attempts. Exiting.")
        return

    print(f"INFO: Fetching catalog from {catalog_url}")
    catalog = {}
    try:
        session.headers.update(get_dynamic_headers())
        response = session.get(catalog_url, timeout=random.uniform(8, 12))
        response.raise_for_status()
        catalog = decode_response_content(response) # Use the new decoding function
        catalog_metas = catalog.get("metas", [])
        print(f"INFO: Successfully fetched catalog with {len(catalog_metas)} metas entries.")
        time.sleep(get_random_delay(1, 3))
    except json.JSONDecodeError as e:
        print(f"CRITICAL: Failed to fetch catalog: {e}. Status Code: {response.status_code}. Response Text (first 500 chars): {response.text[:500]}")
        return
    except requests.exceptions.RequestException as e:
        print(f"CRITICAL: Failed to fetch catalog: {e}")
        return

    iptv_id_to_logo_map, original_iptv_ids = fetch_iptv_channels_csv(iptv_channels_csv_url)
    time.sleep(get_random_delay(2, 5))

    id_data_map = {}
    for channel in catalog.get("metas", []): # Use .get with default empty list
        channel_id = channel.get("id", "").lower().replace(".mv", "")
        logo = channel.get("logo", "")
        genres_list = []
        if "genres" in channel:
            for g in channel["genres"]:
                genres_list.append(g.split("|")[0].strip())
        genre = classify_genre_smart(genres_list)
        id_data_map[channel_id] = {"logo": logo, "genre": genre}
    print(f"INFO: Built ID data map from catalog with {len(id_data_map)} entries.")

    m3u_content = "#EXTM3U\n\n"

    processed_count = 0
    total_prefixes = len(manifest.get('idPrefixes', []))
    print(f"INFO: Starting to process {total_prefixes} channel prefixes.")

    for i, prefix in enumerate(manifest.get('idPrefixes', [])):
        print(f"\n--- Processing channel {i+1}/{total_prefixes}: '{prefix}' ---")
        current_prefix_lower = prefix.lower()
        clean_prefix_no_mv = current_prefix_lower.replace(".mv", "")
        
        channel_url_primary = f"https://hilaytv.vercel.app/stream/tv/{prefix}.mv.json"
        channel_url_fallback = f"https://hilaytv.vercel.app/stream/tv/{prefix}.json"

        channel_data = None
        print(f"DEBUG: Trying primary URL: {channel_url_primary}")
        try:
            session.headers.update(get_dynamic_headers())
            response_primary = session.get(channel_url_primary, timeout=random.uniform(8, 12))
            response_primary.raise_for_status()
            channel_data = decode_response_content(response_primary) # Use new decoding function
            print(f"INFO: Primary URL successful for {prefix}.")
            time.sleep(get_random_delay(0.5, 2))
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"WARNING: Primary URL failed for {prefix} ({channel_url_primary}): {e}. Trying fallback.")
            channel_data = None
            time.sleep(get_random_delay(0.5, 2))

        if not channel_data or not channel_data.get('streams'):
            print(f"DEBUG: Trying fallback URL: {channel_url_fallback}")
            try:
                session.headers.update(get_dynamic_headers())
                response_fallback = session.get(channel_url_fallback, timeout=random.uniform(8, 12))
                response_fallback.raise_for_status()
                channel_data = decode_response_content(response_fallback) # Use new decoding function
                if channel_data and channel_data.get('streams'):
                    print(f"INFO: Successfully used fallback URL for {prefix}.")
                    time.sleep(get_random_delay(0.5, 2))
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                print(f"ERROR: Fallback URL also failed for {prefix} ({channel_url_fallback}): {e}. Skipping channel.")
                time.sleep(get_random_delay(1, 3))
                continue

        if channel_data and channel_data.get('streams'):
            stream = channel_data['streams'][0]
            name = stream.get('name', prefix)
            original_url = stream.get('url', '')
            if not original_url:
                print(f"WARNING: Skipping {prefix}: No URL found after trying both channels.")
                continue
            
            final_url = get_final_url(original_url)
            print(f"DEBUG: Original URL: {original_url}, Final URL: {final_url}")

            logo = ""
            
            if current_prefix_lower in iptv_id_to_logo_map:
                logo = iptv_id_to_logo_map[current_prefix_lower]
                print(f"INFO: Logo for '{prefix}' found via direct exact match in channels.csv.")
            elif clean_prefix_no_mv in iptv_id_to_logo_map:
                logo = iptv_id_to_logo_map[clean_prefix_no_mv]
                print(f"INFO: Logo for '{prefix}' found via direct match (cleaned) in channels.csv.")
            else:
                if ".mv" in current_prefix_lower:
                    for csv_id in iptv_id_to_logo_map.keys():
                        if csv_id == clean_prefix_no_mv:
                            logo = iptv_id_to_logo_map[csv_id]
                            print(f"INFO: Logo for '{prefix}' found via direct match (prefix with .mv vs CSV without) in channels.csv.")
                            break
            
            if not logo and iptv_id_to_logo_map:
                best_match = None
                best_score = -1
                
                for csv_id in original_iptv_ids:
                    csv_id_lower = csv_id.lower().replace(".mv", "")

                    if not clean_prefix_no_mv or not csv_id_lower:
                        continue
                    
                    if len(clean_prefix_no_mv) < 4 and len(csv_id_lower) > len(clean_prefix_no_mv) + 5:
                        continue

                    score_ratio = fuzz.ratio(clean_prefix_no_mv, csv_id_lower)
                    score_token_sort = fuzz.token_sort_ratio(clean_prefix_no_mv, csv_id_lower)
                    
                    current_score = max(score_ratio, score_token_sort)

                    len_diff_percentage = abs(len(clean_prefix_no_mv) - len(csv_id_lower)) / max(len(clean_prefix_no_mv), len(csv_id_lower), 1) * 100
                    
                    VERY_STRICT_FUZZY_THRESHOLD = 90
                    MAX_LEN_DIFF_PERCENTAGE = 30

                    if len(clean_prefix_no_mv) <= 4:
                        VERY_STRICT_FUZZY_THRESHOLD = 95
                        MAX_LEN_DIFF_PERCENTAGE = 20

                    if current_score >= VERY_STRICT_FUZZY_THRESHOLD and len_diff_percentage <= MAX_LEN_DIFF_PERCENTAGE:
                        if current_score > best_score:
                            best_score = current_score
                            best_match = csv_id_lower

                if best_match:
                    logo = iptv_id_to_logo_map.get(best_match, "")
                    if logo:
                        print(f"INFO: Logo for '{prefix}' found via very strict fuzzy match ('{best_match}', score: {best_score}) in channels.csv.")
                else:
                    print(f"DEBUG: No very strict fuzzy match found for '{prefix}'.")

            if not logo:
                logo = id_data_map.get(clean_prefix_no_mv, {}).get("logo", "")
                if logo:
                    print(f"INFO: Logo for '{prefix}' falling back to original catalog URL.")
                else:
                    print(f"WARNING: No logo found for '{prefix}' from any source. Using empty logo.")

            genre = id_data_map.get(clean_prefix_no_mv, {}).get("genre", "General")

            m3u_content += f'#EXTINF:-1 tvg-id="{prefix}" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre}",{name}\n'
            m3u_content += f"{final_url}\n\n"

            print(f"INFO: Processed channel '{name}'.")
            processed_count += 1
        else:
            print(f"WARNING: Skipping {prefix}: No stream data found in either URL.")

    filename = 'peartv.m3u'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(m3u_content)

    print(f"\nINFO: M3U playlist generated: {filename}")
    print(f"INFO: Total channels processed: {processed_count}/{total_prefixes}")
    print("INFO: M3U playlist generation complete.")

if __name__ == "__main__":
    generate_m3u_playlist()
