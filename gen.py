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
        channel_url = f"https://hilaytv.vercel.app/stream/tv/{prefix}.mv.json"
        
        try:
            response = requests.get(channel_url, timeout=10)
            response.raise_for_status()
            channel_data = response.json()
            
            if channel_data and channel_data.get('streams'):
                stream = channel_data['streams'][0]
                name = stream.get('name', prefix)
                original_url = stream.get('url', '')
                
                if not original_url:
                    print(f"Skipping {prefix}: No URL found")
                    continue
                
                # Get final URL after redirects
                final_url = get_final_url(original_url)
                
                # Add to M3U
                m3u_content += f"#EXTINF:-1 tvg-id=\"{prefix}\" tvg-name=\"{name}\" group-title=\"Curious Pear\",{name}\n"
                m3u_content += f"{final_url}\n\n"
                
                print(f"Processed: {name} - Original: {original_url} - Final: {final_url}")
                processed_count += 1
                
        except requests.exceptions.RequestException as e:
            print(f"Error processing {prefix}: {e}")
            continue
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {prefix}")
            continue

    # Save to file
    filename = 'peartv.m3u'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(m3u_content)

    print(f"\nM3U playlist generated: {filename}")
    print(f"Total channels processed: {processed_count}/{len(manifest.get('idPrefixes', []))}")

if __name__ == "__main__":
    generate_m3u_playlist()
