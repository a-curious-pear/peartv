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

# Massive genre keyword mapping, only keeps Islam related for religion
genre_keywords_map = {
    "news": "News",
    "current affairs": "News",
    "live": "News",
    "breaking": "News",
    "documentary": "Documentary",
    "report": "News",
    "international": "News",
    "middle east": "News",
    "world": "News",

    "sports": "Sports",
    "soccer": "Sports",
    "football": "Sports",
    "cricket": "Sports",
    "tennis": "Sports",
    "billiard": "Sports",
    "match": "Sports",
    "league": "Sports",
    "cup": "Sports",
    "olympic": "Sports",
    "nba": "Sports",
    "fifa": "Sports",
    "rugby": "Sports",
    "golf": "Sports",

    "politics": "Politics",
    "parliament": "Politics",
    "senate": "Politics",
    "congress": "Politics",
    "election": "Politics",

    "government": "Government",
    "state": "Government",
    "official": "Government",

    # Islam only
    "islam": "Religion",
    "muslim": "Religion",
    "quran": "Religion",
    "quruan": "Religion",

    "kids": "Kids",
    "child": "Kids",
    "children": "Kids",
    "cartoon": "Kids",
    "animation": "Kids",
    "learning": "Kids",
    "nursery": "Kids",
    "preschool": "Kids",
    "school": "Kids",

    "entertainment": "Entertainment",
    "culture": "Entertainment",
    "events": "Entertainment",
    "variety": "Entertainment",
    "show": "Entertainment",
    "maliku": "Entertainment",
    "drama": "Entertainment",
    "comedy": "Entertainment",
    "reality": "Entertainment",

    "music": "Music",
    "band": "Music",
    "concert": "Music",
    "song": "Music",
    "hits": "Music",
    "radio": "Radio",
    "fm": "Radio",

    "education": "Education",
    "science": "Education",
    "history": "Education",
    "technology": "Education",
    "tech": "Education",

    "movie": "Movies",
    "cinema": "Movies",
    "film": "Movies",
    "hollywood": "Movies",
    "bollywood": "Movies",

    "audio": "Radio",

    "lifestyle": "Lifestyle",
    "travel": "Lifestyle",
    "food": "Lifestyle",
    "cooking": "Lifestyle",
    "fashion": "Lifestyle",

    "shopping": "Shopping",
    "shop": "Shopping",
    "qvc": "Shopping",
    "hsn": "Shopping",

    "docu": "Documentary",
    "wildlife": "Documentary",
    "nature": "Documentary",

    "info": "General",
    "general": "General",
}

def classify_genre_smart(raw_genre):
    genre_lower = raw_genre.lower()
    for keyword, category in genre_keywords_map.items():
        if keyword in genre_lower:
            return category
    return "General"

def generate_m3u_playlist():
    manifest_url = 'https://hilaytv.vercel.app/manifest.json'
    catalog_url = 'https://hilaytv.vercel.app/catalog/tv/maldives.json'

    # Fetch manifest
    try:
        manifest = requests.get(manifest_url, timeout=10).json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch manifest.json: {e}")
        return

    # Fetch catalog
    try:
        catalog = requests.get(catalog_url, timeout=10).json()
        catalog_metas = catalog.get("metas", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch catalog: {e}")
        return

    # Build id-to-logo+genre lookup
    id_data_map = {}
    for channel in catalog_metas:
        channel_id = channel.get("id", "").lower().replace(".mv", "")
        logo = channel.get("logo", "")
        genres_list = channel.get("genres", [])
        genre = "General"
        if genres_list:
            first_segment = genres_list[0].split("|")[0].strip()
            genre = classify_genre_smart(first_segment)
        id_data_map[channel_id] = {
            "logo": logo,
            "genre": genre
        }

    # Start M3U content
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
        except (requests.exceptions.RequestException, json.JSONDecodeError):
            try:
                response_fallback = requests.get(channel_url_fallback, timeout=10)
                response_fallback.raise_for_status()
                channel_data = response_fallback.json()
            except (requests.exceptions.RequestException, json.JSONDecodeError):
                continue

        if channel_data and channel_data.get('streams'):
            stream = channel_data['streams'][0]
            name = stream.get('name', prefix)
            original_url = stream.get('url', '')
            if not original_url:
                continue

            final_url = get_final_url(original_url)
            logo = ""
            group_title = "General"

            if clean_prefix in id_data_map:
                logo = id_data_map[clean_prefix]["logo"]
                group_title = id_data_map[clean_prefix]["genre"]

            m3u_content += f'#EXTINF:-1 tvg-id="{prefix}" tvg-name="{name}" tvg-logo="{logo}" group-title="{group_title}",{name}\n'
            m3u_content += f"{final_url}\n\n"
            processed_count += 1

    with open('peartv.m3u', 'w', encoding='utf-8') as f:
        f.write(m3u_content)

    print(f"\nM3U playlist generated: peartv.m3u")
    print(f"Total channels processed: {processed_count}/{len(manifest.get('idPrefixes', []))}")

if __name__ == "__main__":
    generate_m3u_playlist()
