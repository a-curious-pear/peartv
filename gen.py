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

def generate_m3u_playlist():
    # Fetch the manifest.json
    manifest_url = 'https://hilaytv.vercel.app/manifest.json'
    try:
        response = requests.get(manifest_url, timeout=10)
        response.raise_for_status()
        manifest = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch manifest.json: {e}")
        return

    # Prepare M3U header
    m3u_content = "#EXTM3U\n"
    m3u_content += "# Generated from Hilay TV API\n"
    m3u_content += "# https://hilaytv.vercel.app/\n\n"

    # Process each channel prefix
    processed_count = 0
    for prefix in manifest.get('idPrefixes', []):
        # Try the primary channel URL first
        channel_url_primary = f"https://hilaytv.vercel.app/stream/tv/{prefix}.mv.json"
        channel_url_fallback = f"https://hilaytv.vercel.app/stream/tv/{prefix}.json"
        
        channel_data = None
        
        # Attempt to fetch from primary URL
        try:
            response_primary = requests.get(channel_url_primary, timeout=10)
            response_primary.raise_for_status()
            channel_data = response_primary.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Primary URL failed for {prefix} ({channel_url_primary}): {e}. Trying fallback.")
            channel_data = None # Ensure channel_data is reset if primary fails

        # If primary failed or was empty, try fallback URL
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
            
            # Get final URL after redirects
            final_url = get_final_url(original_url)
            
            # Add to M3U
            m3u_content += f"#EXTINF:-1 tvg-id=\"{prefix}\" tvg-name=\"{name}\" group-title=\"Curious Pear\",{name}\n"
            m3u_content += f"{final_url}\n\n"
            
            print(f"Processed: {name} - Original: {original_url} - Final: {final_url}")
            processed_count += 1
        else:
            print(f"Skipping {prefix}: No stream data found in either URL.")


    # Save to file
    filename = 'peartv.m3u'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(m3u_content)

    print(f"\nM3U playlist generated: {filename}")
    print(f"Total channels processed: {processed_count}/{len(manifest.get('idPrefixes', []))}")

if __name__ == "__main__":
    generate_m3u_playlist()
