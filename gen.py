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

def fetch_catalog_metadata():
    catalog_url = 'https://hilaytv.vercel.app/catalog/tv/hilay_catalog.json'
    catalog_map = {}
    try:
        response = requests.get(catalog_url, timeout=10)
        response.raise_for_status()
        catalog_data = response.json()
        for channel in catalog_data.get("metas", []):
            channel_id = channel.get("id", "")
            if channel_id:
                catalog_map[channel_id] = {
                    "name": channel.get("name", channel_id),
                    "logo": channel.get("logo", ""),
                    "genres": "|".join(channel.get("genres", []))
                }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching catalog metadata: {e}")
    return catalog_map

def generate_m3u_playlist():
    catalog_data = fetch_catalog_metadata()

    manifest_url = 'https://hilaytv.vercel.app/manifest.json'
    try:
        response = requests.get(manifest_url, timeout=10)
        response.raise_for_status()
        manifest = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch manifest.json: {e}")
        return

    m3u_content = "#EXTM3U\n"
    m3u_content += "# Generated from Hilay TV API\n"
    m3u_content += "# https://hilaytv.vercel.app/\n\n"

    processed_count = 0
    for prefix in manifest.get('idPrefixes', []):
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
                print(f"Used fallback for {prefix}")
            except (requests.exceptions.RequestException, json.JSONDecodeError):
                print(f"Skipping {prefix}: No valid stream JSON found.")
                continue

        if channel_data and channel_data.get('streams'):
            stream = channel_data['streams'][0]
            original_url = stream.get('url', '')

            if not original_url:
                print(f"Skipping {prefix}: No URL found")
                continue

            final_url = get_final_url(original_url)

            # Lookup catalog metadata by prefix (which matches 'id' in catalog)
            meta = catalog_data.get(prefix, {})
            name = meta.get("name", stream.get('name', prefix))
            logo = meta.get("logo", "")
            genres = meta.get("genres", "")

            m3u_content += f'#EXTINF:-1 tvg-id="{prefix}" tvg-name="{name}" tvg-logo="{logo}" group-title="{genres}",{name}\n'
            m3u_content += f"{final_url}\n\n"

            print(f"Added: {name} ({prefix})")
            processed_count += 1
        else:
            print(f"Skipping {prefix}: No stream data.")

    filename = 'peartv.m3u'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(m3u_content)

    print(f"\nM3U playlist generated: {filename}")
    print(f"Total channels processed: {processed_count}/{len(manifest.get('idPrefixes', []))}")

if __name__ == "__main__":
    generate_m3u_playlist()
