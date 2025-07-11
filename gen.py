import requests
import json
import re

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
    m3u_content = "#EXTM3U\n"
    m3u_content += "# Generated from Hilay TV API\n"
    m3u_content += "# https://hilaytv.vercel.app/\n\n"

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
            final_url = get_final_url(original_url)

            # Lookup logo and genre
            logo = ""
            genre = "General"
            if clean_prefix in id_data_map:
                logo = id_data_map[clean_prefix]["logo"]
                genre = id_data_map[clean_prefix]["genre"]

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
