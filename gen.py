import requests
import json

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

genre_keywords_map = {
    # Same general structure, excluding non-Islam religions
    "news": "News",
    "current affairs": "News",
    "live": "News",
    "documentary": "Documentary",
    "sports": "Sports",
    "soccer": "Sports",
    "football": "Sports",
    "cricket": "Sports",
    "tennis": "Sports",
    "billiard": "Sports",
    "league": "Sports",
    "politics": "Politics",
    "parliament": "Politics",
    "government": "Government",
    "islam": "Religion",
    "muslim": "Religion",
    "quran": "Religion",
    "quruan": "Religion",
    "kids": "Kids",
    "child": "Kids",
    "learning": "Kids",
    "entertainment": "Entertainment",
    "culture": "Entertainment",
    "events": "Entertainment",
    "music": "Music",
    "radio": "Radio",
    "education": "Education",
    "technology": "Education",
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
    # Always prioritize islamic / quran first
    if any(x in combined for x in ["islam", "muslim", "quran", "quruan"]):
        return "Religion"
    # Then look through other categories
    for keyword, category in genre_keywords_map.items():
        if keyword in combined:
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
        genres_list = []
        if "genres" in channel:
            for g in channel["genres"]:
                genres_list.append(g.split("|")[0].strip())
        genre = classify_genre_smart(genres_list)
        id_data_map[channel_id] = {"logo": logo, "genre": genre}

    # Build M3U
    m3u_content = "#EXTM3U\n"
    processed_count = 0

    for prefix in manifest.get('idPrefixes', []):
        clean_prefix = prefix.lower().replace(".mv", "")
        channel_data = None

        try:
            # Try .mv.json first
            response = requests.get(f"https://hilaytv.vercel.app/stream/tv/{prefix}.mv.json", timeout=10)
            response.raise_for_status()
            channel_data = response.json()
        except:
            try:
                # Try fallback .json
                response = requests.get(f"https://hilaytv.vercel.app/stream/tv/{prefix}.json", timeout=10)
                response.raise_for_status()
                channel_data = response.json()
            except:
                continue

        if channel_data and "streams" in channel_data:
            stream = channel_data["streams"][0]
            name = stream.get("name", prefix)
            original_url = stream.get("url", "")
            final_url = get_final_url(original_url)
            logo = ""
            genre = "General"
            if clean_prefix in id_data_map:
                logo = id_data_map[clean_prefix]["logo"]
                genre = id_data_map[clean_prefix]["genre"]
            m3u_content += f'#EXTINF:-1 tvg-id="{prefix}" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre}",{name}\n'
            m3u_content += f"{final_url}\n\n"
            processed_count += 1

    with open('peartv.m3u', 'w', encoding='utf-8') as f:
        f.write(m3u_content)

    print(f"M3U generated with {processed_count} channels. Saved as peartv.m3u")

if __name__ == "__main__":
    generate_m3u_playlist()
