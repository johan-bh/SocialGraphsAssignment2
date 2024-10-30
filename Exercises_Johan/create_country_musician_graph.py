import re
import asyncio
import aiohttp
import networkx as nx
import time
from tqdm import tqdm
from collections import Counter

# Path to the country musician text file
path = r"C:\Users\jbhan\Desktop\socialgraphs2024\data\week4_country_music.txt"

# Regular expression pattern to match all Wikipedia page names in double brackets that are not files or categories
pattern = re.compile(r'\[\[(?!File:|Category:)([^|\]]+)')

# List to store Wikipedia page names
country_musicians = []

# Open and read the text file
with open(path, 'r', encoding='utf-8') as file:
    for line in file:
        matches = pattern.findall(line)
        for match in matches:
            # Replace spaces with underscores
            page_name = match.strip().replace(' ', '_')
            country_musicians.append(page_name)

print(f"Found {len(country_musicians)} Wikipedia page names.")

# Print duplicates in the list
duplicates = [item for item, count in Counter(country_musicians).items() if count > 1]
print(f"Found {len(duplicates)} duplicate Wikipedia page names.")
print(duplicates)

# Convert the list to a set for faster lookup
page_names_set = set(country_musicians)

# Initialize a directed graph
G = nx.DiGraph()

# Base URL for the Wikipedia API
baseurl = "https://en.wikipedia.org/w/api.php"

# User-Agent header
headers = {
    'User-Agent': 'MyWikipediaApp/1.0 (https://example.com; myemail@example.com)'
}

# Semaphore to limit the number of concurrent requests
semaphore = asyncio.Semaphore(10)

def extract_wikitext_links(wikitext):
    raw_links = re.findall(r'\[\[([^\|\]]+)(\|[^\]]+)?\]\]', wikitext)
    cleaned_links = [link[0].replace(' ', '_') for link in raw_links if ':' not in link[0]]
    return cleaned_links

def calculate_word_count(wikitext):
    words = wikitext.split()
    return len(words)

async def fetch_wikitext_and_links(session, title, retries=3, delay=1, timeout=10):
    links = set()
    content_length = 0

    client_timeout = aiohttp.ClientTimeout(total=timeout)

    for attempt in range(retries):
        async with semaphore:
            params = {
                'action': 'query',
                'titles': title,
                'format': 'json',
                'prop': 'revisions',
                'rvprop': 'content',
                'rvslots': 'main',
                'redirects': '1',
            }

            try:
                async with session.get(baseurl, headers=headers, params=params, timeout=client_timeout) as response:
                    response.raise_for_status()
                    page_content = await response.json()

                    page_id = next(iter(page_content['query']['pages']))
                    page_data = page_content['query']['pages'][page_id]

                    if 'revisions' in page_data and 'slots' in page_data['revisions'][0]:
                        wikitext = page_data['revisions'][0]['slots']['main']['*']
                        wikitext_links = extract_wikitext_links(wikitext)
                        content_length = calculate_word_count(wikitext)
                        for link in wikitext_links:
                            if link in page_names_set:
                                links.add(link)
                    else:
                        print(f"No revisions found for {title}")
                        continue

                    return title, links, content_length

            except Exception as e:
                print(f"Error for {title} on attempt {attempt + 1}: {e}")
                await asyncio.sleep(delay)

    print(f"Failed to fetch data for {title} after {retries} attempts.")
    return title, links, content_length

async def main(page_names):
    client_timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=client_timeout) as session:
        tasks = [asyncio.create_task(fetch_wikitext_and_links(session, title)) for title in page_names]

        results = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Processing pages'):
            try:
                result = await f
                results.append(result)
            except Exception as e:
                print(f"Task failed unexpectedly: {e}")

        # Cancel any tasks that are still pending
        pending_tasks = [task for task in tasks if not task.done()]
        if pending_tasks:
            print("Cancelling pending tasks...")
            for pending in pending_tasks:
                pending.cancel()
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        added_nodes = set()
        for title, links, content_length in results:
            # Use the original title directly
            matched_title = title  # Directly use the title passed to the function
            if matched_title and matched_title not in added_nodes:
                G.add_node(matched_title, length_of_content=content_length)
                added_nodes.add(matched_title)

            for link in links:
                matched_link = link  # Directly use the link as it is
                if matched_title and matched_link:
                    G.add_edge(matched_title, matched_link)

if __name__ == "__main__":
    asyncio.run(main(set(country_musicians)))

    # Save the graph to a GraphML file
    nx.write_graphml(G, "data/country_musician_graph.graphml")
