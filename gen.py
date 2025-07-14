import requests
import json
import re
import pandas as pd
from thefuzz import process, fuzz
from io import StringIO

def get_final_url(url):
    """
    Follows redirects to get the final URL for a given stream.
    """
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
    """
    Classifies the genre based on a list of keywords.
    """
    combined = " ".join(genres_list).lower()
    if any(x in combined for x in ["islam", "muslim", "quran", "quruan"]):
        return "Religion"
    for keyword, category in genre_keywords_map.items():
        if keyword in combined:
            return category
    return "General"

def fetch_iptv_channels_csv(url):
    """
    Fetches the channels.csv from the given URL and processes it for logo lookup.
    Returns a dictionary mapping cleaned IDs to logo URLs, and a list of original IDs for fuzzy matching.
    """
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)

        # Ensure 'id' column exists and get the last column for logo
        if 'id' not in df.columns:
            print(f"Warning: 'id' column not found in {url}. Cannot use for logo lookup.")
            return {}, []

        # The last column contains the logo URL
        logo_column_name = df.columns[-1]

        iptv_id_to_logo_map = {}
        original_iptv_ids = []
        for index, row in df.iterrows():
            channel_id = str(row['id']).lower() # Convert to string and lowercase for consistent keys
            logo_url = str(row[logo_column_name])
            if channel_id and logo_url: # Ensure both are not empty
                iptv_id_to_logo_map[channel_id] = logo_url
                original_iptv_ids.append(channel_id) # Store original IDs for fuzzy matching

        print(f"Successfully loaded {len(iptv_id_to_logo_map)} entries from channels.csv.")
        return iptv_id_to_logo_map, original_iptv_ids

    except requests.exceptions.RequestException as e:
        print(f"Error fetching channels.csv from {url}: {e}")
        return {}, []
    except pd.errors.EmptyDataError:
        print(f"Error: channels.csv from {url} is empty.")
        return {}, []
    except Exception as e:
        print(f"An unexpected error occurred while processing channels.csv: {e}")
        return {}, []

def generate_m3u_playlist():
    """
    Generates an M3U playlist by fetching manifest and catalog data,
    and intelligently assigning channel logos.
    """
    manifest_url = 'https://hilaytv.vercel.app/manifest.json'
    catalog_url = 'https://hilaytv.vercel.app/catalog/tv/maldives.json'
    iptv_channels_csv_url = 'https://raw.githubusercontent.com/iptv-org/database/refs/heads/master/data/channels.csv'
    FUZZY_MATCH_THRESHOLD = 85 # Minimum score for a fuzzy match to be considered valid

    # Fetch manifest
    try:
        response = requests.get(manifest_url, timeout=10)
        response.raise_for_status()
        manifest = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch manifest.json: {e}")
        return

    # Fetch catalog (for original logo and genre lookup)
    try:
        response = requests.get(catalog_url, timeout=10)
        response.raise_for_status()
        catalog = response.json()
        catalog_metas = catalog.get("metas", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch catalog: {e}")
        return

    # Fetch and process iptv-org channels.csv for logo lookup
    iptv_id_to_logo_map, original_iptv_ids = fetch_iptv_channels_csv(iptv_channels_csv_url)

    # Build id lookup for logo and genre from original catalog
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
        current_prefix_lower = prefix.lower()
        clean_prefix_no_mv = current_prefix_lower.replace(".mv", "")
        
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
            final_url = get_final_url(original_url)

            # --- Logo Assignment Logic ---
            logo = ""
            
            # Attempt 1: Direct match (case-insensitive, including .mv)
            if current_prefix_lower in iptv_id_to_logo_map:
                logo = iptv_id_to_logo_map[current_prefix_lower]
                print(f"Logo for '{prefix}' found via direct exact match in channels.csv.")
            else:
                # Attempt 2: Direct match ignoring .mv, handle multiple matches
                potential_matches_no_mv = [
                    csv_id for csv_id in iptv_id_to_logo_map.keys()
                    if csv_id.replace(".mv", "") == clean_prefix_no_mv
                ]

                if len(potential_matches_no_mv) == 1:
                    logo = iptv_id_to_logo_map[potential_matches_no_mv[0]]
                    print(f"Logo for '{prefix}' found via direct match (ignoring .mv) in channels.csv.")
                elif len(potential_matches_no_mv) > 1:
                    # If multiple matches after ignoring .mv, prioritize the one with .mv if current_prefix also has it
                    if ".mv" in current_prefix_lower:
                        mv_match = next((m for m in potential_matches_no_mv if ".mv" in m), None)
                        if mv_match:
                            logo = iptv_id_to_logo_map[mv_match]
                            print(f"Logo for '{prefix}' found via direct match (prioritizing .mv) in channels.csv.")
                        else: # If multiple, but none with .mv, just take the first
                            logo = iptv_id_to_logo_map[potential_matches_no_mv[0]]
                            print(f"Logo for '{prefix}' found via direct match (multiple, taking first) in channels.csv.")
                    else: # If current_prefix doesn't have .mv, just take the first match without .mv
                        logo = iptv_id_to_logo_map[potential_matches_no_mv[0]]
                        print(f"Logo for '{prefix}' found via direct match (multiple, taking first) in channels.csv.")

            # Attempt 3: Fuzzy match (if no direct match found yet)
            if not logo and iptv_id_to_logo_map: # Only try fuzzy if iptv_channels_csv was loaded
                # Use original_iptv_ids for fuzzy matching to get the full ID back
                best_fuzzy_result = process.extractOne(clean_prefix_no_mv, original_iptv_ids, scorer=fuzz.token_set_ratio)
                
                if best_fuzzy_result and best_fuzzy_result[1] >= FUZZY_MATCH_THRESHOLD:
                    fuzzy_matched_id = best_fuzzy_result[0].lower()
                    logo = iptv_id_to_logo_map.get(fuzzy_matched_id, "")
                    if logo:
                        print(f"Logo for '{prefix}' found via fuzzy match ('{best_fuzzy_result[0]}', score: {best_fuzzy_result[1]}) in channels.csv.")
                else:
                    print(f"Fuzzy match for '{prefix}' not close enough (score: {best_fuzzy_result[1] if best_fuzzy_result else 'N/A'}).")

            # Fallback to original catalog logo if no logo found from iptv-org CSV
            if not logo:
                logo = id_data_map.get(clean_prefix_no_mv, {}).get("logo", "")
                if logo:
                    print(f"Logo for '{prefix}' falling back to original catalog URL.")
                else:
                    print(f"No logo found for '{prefix}' from any source.")

            # --- End Logo Assignment Logic ---

            # Genre lookup (always from original catalog)
            genre = id_data_map.get(clean_prefix_no_mv, {}).get("genre", "General")

            m3u_content += f'#EXTINF:-1 tvg-id="{prefix}" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre}",{name}\n'
            m3u_content += f"{final_url}\n\n"

            print(f"Processed: {name} - Original: {original_url} - Final: {final_url}")
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
