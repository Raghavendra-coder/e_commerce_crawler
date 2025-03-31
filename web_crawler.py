import aiohttp
import asyncio
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import re
import json


def normalize_url(url, base_url):
    absolute_url = urljoin(base_url, url)
    parsed = urlparse(absolute_url)
    parsed = parsed._replace(fragment='')
    return urlunparse(parsed)


def is_same_domain(url, base_domain):
    parsed_url = urlparse(url)
    parsed_base = urlparse(base_domain)
    return parsed_url.netloc == parsed_base.netloc


async def get_robots_txt(session, domain):
    robots_url = f"{domain}/robots.txt"
    rp = RobotFileParser()
    try:
        async with session.get(robots_url) as response:
            if response.status == 200:
                content = await response.text()
                rp.parse(content.splitlines())
            else:
                rp.parse([])
    except Exception as e:
        print(f"Error fetching robots.txt for {domain}: {e}")
        rp.parse([])
    return rp


async def worker(session, domain, rp, queue, visited, product_urls, product_patterns, sem, lock):
    while True:
        url = await queue.get()
        original_normalized = normalize_url(url, domain)

        async with lock:
            if original_normalized in visited:
                queue.task_done()
                continue
            visited.add(original_normalized)

        parsed_url = urlparse(original_normalized)
        if not rp.can_fetch('MyCrawler/1.0', parsed_url.path):
            queue.task_done()
            continue

        async with sem:
            try:
                async with session.get(original_normalized, allow_redirects=True) as response:
                    final_url = str(response.url)
                    final_normalized = normalize_url(final_url, domain)

                    if not is_same_domain(final_normalized, domain):
                        queue.task_done()
                        continue

                    if final_normalized != original_normalized:
                        async with lock:
                            if final_normalized in visited:
                                queue.task_done()
                                continue
                            visited.add(final_normalized)

                    if response.status == 200 and 'html' in response.headers.get('Content-Type', '').lower():
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        links = [a.get('href') for a in soup.find_all('a')]

                        for link in links:
                            if not link:
                                continue
                            absolute_link = normalize_url(link, final_normalized)
                            if not is_same_domain(absolute_link, domain):
                                continue

                            parsed_link = urlparse(absolute_link)
                            if rp.can_fetch('MyCrawler/1.0', parsed_link.path):
                                is_product = any(pattern.search(parsed_link.path) for pattern in product_patterns)
                                async with lock:
                                    if is_product:
                                        if absolute_link not in product_urls:
                                            product_urls.append(absolute_link)
                                    else:
                                        if absolute_link not in visited:
                                            await queue.put(absolute_link)
            except Exception as e:
                print(f"Error fetching {original_normalized}: {e}")
            finally:
                queue.task_done()


async def crawl_domain(session, domain, product_patterns, output_map, concurrency=10, max_workers=5):
    rp = await get_robots_txt(session, domain)
    start_url = domain
    parsed_start = urlparse(start_url)
    if not rp.can_fetch('MyCrawler/1.0', parsed_start.path):
        print(f"Cannot crawl {domain} due to robots.txt")
        return

    queue = asyncio.Queue()
    await queue.put(start_url)
    visited = set()
    product_urls = []
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(concurrency)

    workers = [
        asyncio.create_task(worker(session, domain, rp, queue, visited, product_urls, product_patterns, sem, lock)) for
        _ in range(max_workers)]
    await queue.join()

    for worker_task in workers:
        worker_task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    output_map[domain] = product_urls


async def main(domains, product_patterns, output_file):
    output_map = {}
    connector = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for domain in domains:
            task = asyncio.create_task(crawl_domain(session, domain, product_patterns, output_map))
            tasks.append(task)
        await asyncio.gather(*tasks)

    with open(output_file, 'w') as f:
        json.dump(output_map, f, indent=2)


if __name__ == "__main__":
    required_domains = [
        "https://www.virgio.com/",
        "https://www.tatacliq.com/",
        "https://nykaafashion.com/",
        "https://www.westside.com/"
    ]
    product_regexes = [
        re.compile(r'/product/', re.IGNORECASE),
        re.compile(r'/p/', re.IGNORECASE),
        re.compile(r'/item/', re.IGNORECASE),
        re.compile(r'/prd/', re.IGNORECASE),
        re.compile(r'/prod/', re.IGNORECASE),
        re.compile(r'/shop/', re.IGNORECASE),
        re.compile(r'/buy/', re.IGNORECASE),
    ]
    output_file = 'product_urls.json'
    asyncio.run(main(required_domains, product_regexes, output_file))
