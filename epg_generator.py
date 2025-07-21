import re
import os
import requests
import gzip
import xml.etree.ElementTree as ET
from io import BytesIO

def download_file(url, local_filename):
    """
    Downloads a file from a URL.
    """
    print(f"Downloading {url} to {local_filename}...")
    try:
        with requests.get(url, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Successfully downloaded {local_filename}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during download: {e}")
        return False

def parse_m3u_playlist(file_path):
    """
    Parses an M3U playlist file and extracts channel information.

    Args:
        file_path (str): The path to the M3U playlist file.

    Returns:
        list: A list of dictionaries, where each dictionary represents a channel
              and contains its 'name', 'url', 'tvg_id', and 'tvg_name'.
    """
    channels = []
    current_channel = {}

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#EXTINF:'):
                    current_channel = {}
                    tvg_id_match = re.search(r'tvg-id="([^"]*)"', line)
                    current_channel['tvg_id'] = tvg_id_match.group(1) if tvg_id_match else None

                    tvg_name_match = re.search(r'tvg-name="([^"]*)"', line)
                    current_channel['tvg_name'] = tvg_name_match.group(1) if tvg_name_match else None

                    name_match = re.search(r',(.+)$', line)
                    current_channel['name'] = name_match.group(1).strip() if name_match else 'Unknown Channel'

                elif line and not line.startswith('#'):
                    if current_channel:
                        current_channel['url'] = line
                        channels.append(current_channel)
                        current_channel = {}
    except FileNotFoundError:
        print(f"Error: M3U file not found at {file_path}")
        return []
    except Exception as e:
        print(f"An error occurred while parsing the M3U file: {e}")
        return []

    return channels

def download_and_parse_epg(epg_url):
    """
    Downloads an XMLTV EPG file (potentially gzipped) and parses its content.

    Args:
        epg_url (str): The URL of the XMLTV EPG file.

    Returns:
        xml.etree.ElementTree.Element or None: The root element of the parsed XMLTV, or None if an error occurs.
    """
    print(f"\nAttempting to download EPG from: {epg_url}")
    try:
        response = requests.get(epg_url, stream=True, timeout=300) # Increased timeout for large files
        response.raise_for_status()

        xml_content = response.content
        # Check if content is gzipped by header or file extension
        if response.headers.get('Content-Encoding') == 'gzip' or epg_url.endswith('.gz'):
            print("Content is gzipped, decompressing...")
            xml_content = gzip.decompress(xml_content)
        
        # Parse the XML content
        root = ET.fromstring(xml_content)
        print("EPG XMLTV data downloaded and parsed successfully.")
        return root

    except requests.exceptions.RequestException as e:
        print(f"Error downloading EPG from {epg_url}: {e}")
    except gzip.BadGzipFile:
        print("Error: Could not decompress gzipped EPG file. It might be corrupted or not a valid gzip.")
    except ET.ParseError as e:
        print(f"Error parsing XMLTV content: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during EPG download/parsing: {e}")
    return None

def match_channels_with_epg(m3u_channels, epg_root):
    """
    Matches M3U channels with EPG data and collects relevant EPG elements.

    Args:
        m3u_channels (list): List of dictionaries from M3U parsing.
        epg_root (xml.etree.ElementTree.Element): Root of the parsed XMLTV EPG.

    Returns:
        tuple: A tuple containing two dictionaries:
               - matched_epg_channels: {tvg_id: EPG_channel_element}
               - matched_epg_programs: {tvg_id: [list_of_EPG_program_elements]}
    """
    matched_epg_channels = {}
    matched_epg_programs = {}

    # Create a lookup for EPG channels by their 'id' attribute
    epg_channel_lookup = {channel_elem.get('id'): channel_elem for channel_elem in epg_root.findall('channel')}

    # Group programs by channel ID for efficient lookup
    epg_program_lookup = {}
    for program_elem in epg_root.findall('programme'):
        channel_id = program_elem.get('channel')
        if channel_id not in epg_program_lookup:
            epg_program_lookup[channel_id] = []
        epg_program_lookup[channel_id].append(program_elem)

    print("\nAttempting to match M3U channels with EPG data...")
    matched_count = 0
    skipped_count = 0

    for m3u_channel in m3u_channels:
        m3u_tvg_id = m3u_channel.get('tvg_id')
        if m3u_tvg_id and m3u_tvg_id in epg_channel_lookup:
            matched_epg_channels[m3u_tvg_id] = epg_channel_lookup[m3u_tvg_id]
            matched_epg_programs[m3u_tvg_id] = epg_program_lookup.get(m3u_tvg_id, [])
            matched_count += 1
        else:
            skipped_count += 1

    print(f"\nMatching complete. Found {matched_count} channels with EPG data, skipped {skipped_count} channels.")
    return matched_epg_channels, matched_epg_programs

def generate_custom_xmltv(matched_channels, matched_programs, output_file_name='custom_epg.xml'):
    """
    Generates a new XMLTV file with only the matched EPG data.

    Args:
        matched_channels (dict): Dictionary of matched EPG channel elements.
        matched_programs (dict): Dictionary of matched EPG program elements.
        output_file_name (str): The name of the output XMLTV file.
    """
    print(f"\nGenerating custom XMLTV file: {output_file_name}")
    tv_root = ET.Element('tv', attrib={
        'generator-info-name': 'Custom EPG Generator by Gemini',
        'source-info-name': 'Aggregated EPG Sources'
    })

    # Add channel elements
    for tvg_id, channel_elem in matched_channels.items():
        tv_root.append(channel_elem)

    # Add program elements
    for tvg_id, program_list in matched_programs.items():
        for program_elem in program_list:
            tv_root.append(program_elem)

    # Create an ElementTree object and write to file
    tree = ET.ElementTree(tv_root)
    try:
        # Use a custom XML declaration to ensure it's always utf-8
        # pretty_print is not a standard ElementTree argument, it's from lxml.
        # For standard ET, we can't pretty print directly.
        # We'll write it as is and rely on the file being valid XML.
        tree.write(output_file_name, encoding='utf-8', xml_declaration=True)
        print(f"Custom EPG file '{output_file_name}' created successfully.")
    except Exception as e:
        print(f"Error writing custom EPG file: {e}")

def main():
    # --- Configuration ---
    m3u_url = 'https://raw.githubusercontent.com/a-curious-pear/peartv/refs/heads/main/peartv.m3u'
    m3u_file_name = 'peartv.m3u'
    # This is a large, comprehensive EPG file. It might take a while to download and process.
    # For specific regions, you might find smaller, faster alternatives on sites like bevy.be or epgshare01.online
    epg_url = 'https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz'
    custom_epg_output_file = 'custom_epg.xml'

    # --- Download M3U playlist ---
    if not download_file(m3u_url, m3u_file_name):
        print("Failed to download M3U playlist. Exiting.")
        return

    # --- Parse the M3U file ---
    print(f"\nParsing M3U playlist from: {m3u_file_name}")
    channels_data = parse_m3u_playlist(m3u_file_name)

    if not channels_data:
        print("No channels found in M3U playlist. Exiting.")
        return

    print(f"\nFound {len(channels_data)} channels in your M3U playlist.")

    # --- Download and Parse EPG ---
    epg_root = download_and_parse_epg(epg_url)

    if epg_root is None:
        print("Could not download or parse EPG data. Cannot proceed with matching.")
        return

    # --- Match M3U Channels with EPG ---
    matched_epg_channels, matched_epg_programs = match_channels_with_epg(channels_data, epg_root)

    if not matched_epg_channels:
        print("No channels from your M3U could be matched with the EPG data. The generated EPG file will be empty.")
        return

    # --- Generate Custom XMLTV ---
    generate_custom_xmltv(matched_epg_channels, matched_epg_programs, custom_epg_output_file)

    print("\n--- Automation Complete ---")
    print(f"The '{custom_epg_output_file}' file has been generated.")

if __name__ == "__main__":
    main()
