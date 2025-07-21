import re
import os
import requests
import gzip
import xml.etree.ElementTree as ET
from io import BytesIO
import copy # Needed for deepcopy to retain elements from iterparse

def normalize_string(s):
    """
    Normalizes a string for comparison: converts to lowercase, removes spaces,
    and removes common non-alphanumeric characters.
    """
    if s is None:
        return ""
    s = s.lower()
    s = re.sub(r'\s+', '', s) # Remove all whitespace
    s = re.sub(r'[^a-z0-9]', '', s) # Remove non-alphanumeric characters (keep only letters and numbers)
    return s

def download_file(url, local_filename):
    """
    Downloads a file from a URL.
    """
    print(f"Downloading {url} to {local_filename}...")
    try:
        with requests.get(url, stream=True, timeout=60) as r: # Increased timeout for very large files
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

def process_epg_iteratively(epg_url, m3u_channels):
    """
    Downloads an XMLTV EPG file and processes it iteratively to match channels
    and collect relevant EPG data without loading the entire file into memory.

    Args:
        epg_url (str): The URL of the XMLTV EPG file.
        m3u_channels (list): List of dictionaries from M3U parsing.

    Returns:
        tuple: A tuple containing two dictionaries:
               - matched_epg_channels: {tvg_id: EPG_channel_element}
               - matched_epg_programs: {tvg_id: [list_of_EPG_program_elements]}
    """
    matched_epg_channels = {}
    matched_epg_programs = {}

    # Prepare M3U channel lookups for efficient matching
    m3u_tvg_ids = {c.get('tvg_id') for c in m3u_channels if c.get('tvg_id')}
    m3u_normalized_names = {normalize_string(c.get('tvg_name')) for c in m3u_channels if c.get('tvg_name')}

    print(f"\nAttempting to download and iteratively parse EPG from: {epg_url}")
    try:
        response = requests.get(epg_url, stream=True, timeout=600) # Increased timeout for very large downloads
        response.raise_for_status()

        # Decompress if gzipped
        if response.headers.get('Content-Encoding') == 'gzip' or epg_url.endswith('.gz'):
            print("Content is gzipped, decompressing stream...")
            # Use BytesIO to wrap the decompressed stream for iterparse
            xml_stream = gzip.GzipFile(fileobj=BytesIO(response.content))
        else:
            xml_stream = BytesIO(response.content)

        # Iteratively parse the XML
        # We parse 'end' events for 'channel' and 'programme' to ensure all children are processed
        # before we decide to keep or discard the element.
        print("Starting iterative parsing of EPG data...")
        
        # Keep track of EPG channel IDs that have been matched to avoid re-processing
        matched_epg_ids_set = set()
        
        # Temporary storage for channel elements encountered during parsing
        temp_epg_channels = {}
        
        for event, elem in ET.iterparse(xml_stream, events=('end',)):
            if event == 'end':
                if elem.tag == 'channel':
                    epg_channel_id = elem.get('id')
                    display_name_elem = elem.find('display-name')
                    epg_display_name = display_name_elem.text if display_name_elem is not None else None
                    
                    # Store channel for later program lookup if it matches an M3U channel
                    is_matched_channel = False
                    if epg_channel_id and epg_channel_id in m3u_tvg_ids:
                        is_matched_channel = True
                    elif epg_display_name and normalize_string(epg_display_name) in m3u_normalized_names:
                        is_matched_channel = True

                    if is_matched_channel:
                        # Deep copy the element to ensure it's not garbage collected by iterparse
                        temp_epg_channels[epg_channel_id] = copy.deepcopy(elem)
                        # print(f"  Found potential EPG channel: ID='{epg_channel_id}', Name='{epg_display_name}'")

                elif elem.tag == 'programme':
                    epg_program_channel_id = elem.get('channel')
                    
                    # Only process programs for channels we are interested in
                    if epg_program_channel_id in temp_epg_channels:
                        # Deep copy the element to ensure it's not garbage collected
                        if epg_program_channel_id not in matched_epg_programs:
                            matched_epg_programs[epg_program_channel_id] = []
                        matched_epg_programs[epg_program_channel_id].append(copy.deepcopy(elem))
                        matched_epg_ids_set.add(epg_program_channel_id) # Mark channel as having programs

            # Clear the element from memory after processing to keep memory low
            elem.clear()

        # After parsing all programs, add the channel elements for those that had programs
        for channel_id in matched_epg_ids_set:
            if channel_id in temp_epg_channels:
                matched_epg_channels[channel_id] = temp_epg_channels[channel_id]

        print("Iterative EPG parsing complete.")
        num_epg_channels_matched = len(matched_epg_channels)
        num_epg_programs_collected = sum(len(progs) for progs in matched_epg_programs.values())
        print(f"Collected {num_epg_channels_matched} EPG channels and {num_epg_programs_collected} programs for matching.")

        return matched_epg_channels, matched_epg_programs

    except requests.exceptions.RequestException as e:
        print(f"Error downloading EPG from {epg_url}: {e}")
    except gzip.BadGzipFile:
        print("Error: Could not decompress gzipped EPG file. It might be corrupted or not a valid gzip.")
    except ET.ParseError as e:
        print(f"Error parsing XMLTV content: {e}. This might indicate malformed XML.")
    except Exception as e:
        print(f"An unexpected error occurred during EPG download/parsing: {e}")
    return {}, {} # Return empty dicts on failure

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
    for tvg_id in sorted(matched_channels.keys()): # Sort for consistent output
        tv_root.append(matched_channels[tvg_id])

    # Add program elements
    for tvg_id in sorted(matched_programs.keys()): # Sort for consistent output
        for program_elem in matched_programs[tvg_id]:
            tv_root.append(program_elem)

    # Create an ElementTree object and write to file
    tree = ET.ElementTree(tv_root)
    try:
        tree.write(output_file_name, encoding='utf-8', xml_declaration=True)
        print(f"Custom EPG file '{output_file_name}' created successfully.")
        if not matched_channels and not matched_programs:
            print("WARNING: The generated custom_epg.xml file is empty because no channels were matched or had programs.")
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

    # --- Process EPG Iteratively and Match ---
    matched_epg_channels, matched_epg_programs = process_epg_iteratively(epg_url, channels_data)

    if not matched_epg_channels and not matched_epg_programs:
        print("No channels from your M3U could be matched with the EPG data or no programs were found for matched channels. The generated EPG file will likely be empty.")
        pass # Proceed to generate an empty file as per request to always overwrite.

    # --- Generate Custom XMLTV ---
    generate_custom_xmltv(matched_epg_channels, matched_epg_programs, custom_epg_output_file)

    print("\n--- Automation Complete ---")
    print(f"The '{custom_epg_output_file}' file has been generated.")
    print("\nTo diagnose the content of the file, check the GitHub Actions run logs for messages like 'MATCHED' or 'SKIPPED'.")
    print("Specifically, look at the 'Run EPG Generator Script' step in your workflow run details.")

if __name__ == "__main__":
    main()
