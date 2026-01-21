import asyncio
import json
import os
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy, BrowserConfig, CrawlResult
import csv
import pandas as pd
from typing import Dict, List, Set
from urllib.parse import urljoin
import requests
import urllib3
# Disable only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


async def namibia():

    print("Extracting data from Namibia Statistics Agency (NSA) website.")
    def ensure_base_url(url):
        """Convert relative URLs to absolute URLs and remove trailing '#'"""
        # Convert relative URLs to absolute URLs
        if not url.startswith(("http://", "https://")):
            # Handle different relative path formats
            if url.startswith("./"):
                url = url[2:]
            elif url.startswith("/"):
                url = url[1:]
            return base_url + url
        return url
    base_url = "https://nsa.org.na/"
    pub_url = "https://nsa.org.na/publications/"
    census_url = "https://nsa.org.na/census/"
    nss_url = "https://nsa.org.na/nss/"
    nsdi_url = "https://nsa.org.na/nsdi/"
    nss_menu_names: Set[str] = set()
    nsdi_menu_names: Set[str] = set()
    menu_links = set()
    nss_nav_links = []
    nsdi_nav_links = []
    nss_docs = []
    nsdi_docs = []
    home_docs = []    
    pub_docs = []
    census_docs = []
    menu_links_schema = {
    "name": "menu_links",
    "baseSelector": "section .dt-nav-menu-horizontal a ",
    "type": "list",
    "fields": [
        {
            "name":"title",
            "selector": "span .menu-text",
            "type": "text"
        },
        {
            "name": "url",
            "type": "attribute",
            "attribute": "href"
        }
        ]
    }
    pub_folder_schema = {
        "name": "folders",
        "baseSelector": "li.dlp-folder",
        "type": "list",
        "fields": [
            {
                "name": "id",
                "type": "attribute",
                "attribute": "data-category-id"
            },
            {
                "name": "name",
                "selector": ".dlp-category-name",
                "type": "text"
            }
        ]
    }
    pub_docs_schema = {
        "name": "publication_docs",
        "baseSelector": "tbody tr.post-row",
        "type": "list",
        "fields": [
            {"name": "title", "selector": ".col-title", "type": "text"},
            {"name": "categories", "selector": ".col-doc_categories span", "type": "text"},
            {"name": "date", "selector": ".col-date.sorting_1", "type": "text"},
            {"name": "link", "selector": ".col-link a.dlp-download-link", "type": "attribute", "attribute": "href"},
        ],
    }
    census_main_report_schema = {
            "name": "download report",
            "baseSelector": "li.menu-item-10792",  # Each item container
            "type": "list",
            "fields": [
                {   
                    "name": "link", 
                    "selector": "a[target='_blank']",
                    "type": "attribute",
                    "attribute": "href"  
                }
            ]
        }
    census_docs_schema = {
            "name": "census_docs",
            "baseSelector": ".e-con-inner > .e-child",  # Each item container
            "type": "list",
            "fields": [
                {   
                    "name": "title", 
                    "selector": "h5.qodef-m-title", 
                    "type": "text"
                },
                {   
                    "name": "link", 
                    "selector": "a[href$='.pdf'], a[href$='.xlsx']",  # Only PDF and XLSX links
                    "type": "attribute",
                    "attribute": "href"  
                },
            ]
        }
    nss_homefile_schema = {
        "name": "home files",
        "baseSelector": ".e-con-inner span.elementor-icon-box-title",
        "type": "list",
        "fields": [
            {"name": "title", "selector": "a", "type": "text"},
            {"name": "link", "selector": "a[href$='.pdf']", "type": "attribute", "attribute": "href"},
        ],
    }
    nss_nsdi_nav_schema = {
        "name": "navigation menus",
        "baseSelector": "ul.dt-nav-menu-horizontal > li.menu-item",
        "type": "list",
        "fields": [
            {
                "name": "menu_name",
                "selector": "span.menu-text",
                "type": "text",
            },
            {
                "name": "url",
                "selector": "a",
                "type": "attribute",
                "attribute": "href"
            }
        ]
    }
    nss_nsdi_docs_schema = {
        "name": "nsdi_docs",
        "baseSelector": "tbody tr.post-row",
        "type": "list",
        "fields": [
            {"name": "title", "selector": ".col-title", "type": "text"},
            {"name": "date", "selector": ".col-date.sorting_1", "type": "text"},
            {"name": "link", "selector": ".col-link a.dlp-download-link", "type": "attribute", "attribute": "href"},
        ],
    }

# ------------------------- NSS NAVIGATION LINKS EXTRACTION ------------------------- #

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
        
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for="body section",
            session_id="hn_session",
            extraction_strategy=JsonCssExtractionStrategy(schema=nss_nsdi_nav_schema),
            magic=False,
        )
        
        results: List[CrawlResult] = await crawler.arun(url=nss_url, config=config)
        
        for result in results:
            if result.success and result.extracted_content:
                try:
                    items = json.loads(result.extracted_content)
                    print(f"Extracted {len(items)} raw menu items")
                    
                    for item in items:
                        menu_name = item.get("menu_name", "").strip()
                        menu_url = item.get("url", "").strip()
                        
                        # Skip empty or invalid items
                        if not menu_name or not menu_url:
                            continue
                            
                        # Only add if we haven't seen this menu name before
                        if menu_name not in nss_menu_names:
                            nss_menu_names.add(menu_name)
                            # Create dictionary with menu_name as key and url as value
                            nss_nav_links.append({menu_name: menu_url})
                        
                            
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}")
                    continue

        print(f"After deduplication: {len(nss_nav_links)} unique menu links")

    # Save the unique results
    with open("nss_menu_links.json", "w", encoding="utf-8") as f:
        json.dump(nss_nav_links, f, indent=4)
    
    print(f"Saved {len(nss_nav_links)} unique menu links to nss_menu_links.json")

# -------------------- HOME PAGE DOCUMENTS EXTRACTION -----------------

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for=".e-con-inner .elementor-icon-box-title",
            session_id="hn_session",
            extraction_strategy=JsonCssExtractionStrategy(schema=nss_homefile_schema),
            magic=False,
        )
        results: List[CrawlResult] = await crawler.arun(url=nss_url, config=config)
        for result in results:
            if result.success:
                try:
                    items = json.loads(result.extracted_content)
                except Exception as e:
                    print(f"Error parsing JSON: {e}")
                    items = []

                print(f"Extracted {len(items)} reports from Home page")

                for item in items:
                    title = item.get("title", "").strip()
                    link = item.get("link", "").strip()
                    # Only add if BOTH title and link are not empty
                    if title and link:
                        report = {
                            "title": title,
                            "link": link,
                        }
                        home_docs.append(report)

                print(f"After filtering: {len(home_docs)} items with both title and link")

    with open("home_page_docs.json", "w", encoding="utf-8") as f:
        json.dump(home_docs, f, ensure_ascii=False, indent=4)

    print(f"Saved {len(home_docs)} valid documents to home_docs.json")

# ----------------------- DOCUMENTS EXTRACTION -----------------------

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:

        # Extract the DOCUMENTS URL
        documents_url = None
        for item in nss_nav_links:
            if "DOCUMENTS" in item:
                documents_url = item["DOCUMENTS"]
                break
            
        config=CrawlerRunConfig (
                    cache_mode=CacheMode.BYPASS,
                    scan_full_page=True,
                    wait_for="tbody tr.post-row",  # Wait until banner is gone
                    wait_for_timeout=60000, # 60 seconds
                    session_id="hn_session",
                    extraction_strategy=JsonCssExtractionStrategy(schema = nss_nsdi_docs_schema),
                    magic=False, 
                    page_timeout= 60000, # 60 seconds 
    )
        
        results: List[CrawlResult] = await crawler.arun(url=documents_url, config=config)
        for result in results:
            if result.success:
                try:
                    items = json.loads(result.extracted_content)
                except Exception as e:
                    print(f"Error parsing JSON: {e}")
                    items = []

                print(f"Extracted {len(items)} reports from {documents_url}")

                for item in items:
                    report = {
                        "title": item.get("title", "").strip(),
                        "date": item.get("date", "").strip(),
                        "link": item.get("link", "").strip(),
                    }
                    nss_docs.append(report)

    with open("nss_docs.json", "w", encoding="utf-8") as f:
        json.dump(nss_docs, f, ensure_ascii=False, indent=4)



    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:

        config=CrawlerRunConfig (
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for="body section",   
            session_id="hn_session",
            extraction_strategy=JsonCssExtractionStrategy(schema=nss_nsdi_nav_schema),
            magic=False, 
        )

        results: List[CrawlResult] = await crawler.arun(url=nsdi_url,config=config,)

        for result in results:
            if result.success and result.extracted_content:
                try:
                    items = json.loads(result.extracted_content)
                    print(f"Extracted {len(items)} Links")

                    for item in items:
                        menu_name = item.get("menu_name", "").strip()
                        menu_url = item.get("url", "").strip()

                        # Skip empty or invalid items
                        if not menu_name or not menu_url:
                            continue

                        # Only add if we haven't seen this menu name before
                        if menu_name not in nsdi_menu_names:
                            nsdi_menu_names.add(menu_name)
                            # Create dictionary with menu_name as key and url as value
                            nsdi_nav_links.append({menu_name: menu_url})

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}")
                    continue

        print(f"After deduplication: {len(nsdi_nav_links)} unique menu links")

    # Save the unique results   
    with open("nsdi_menu_links.json", "w", encoding="utf-8") as f:
        json.dump(nsdi_nav_links, f, indent=4)

    print(f"Saved {len(nsdi_nav_links)} unique menu links to nsdi_menu_links.json")

# ----------------------- DOCUMENTS EXTRACTION -----------------------

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
        
        # Extract the DOCUMENTS URL
        documents_url = None
        for item in nsdi_nav_links:
            if "DOCUMENTS" in item:
                documents_url = item["DOCUMENTS"]
                break

        config=CrawlerRunConfig (
                    cache_mode=CacheMode.BYPASS,
                    scan_full_page=True,
                    wait_for="tbody tr.post-row",  # Wait until banner is gone
                    wait_for_timeout=60000, # 60 seconds
                    session_id="hn_session",
                    extraction_strategy=JsonCssExtractionStrategy(schema = nss_nsdi_docs_schema),
                    magic=False, 
                    page_timeout= 60000, # 60 seconds 
    )
        
        results: List[CrawlResult] = await crawler.arun(url=documents_url, config=config)
        for result in results:
            if result.success:
                try:
                    items = json.loads(result.extracted_content)
                except Exception as e:
                    print(f"Error parsing JSON: {e}")
                    items = []

                print(f"Extracted {len(items)} reports from {documents_url}")

                for item in items:
                    report = {
                        "title": item.get("title", "").strip(),
                        "date": item.get("date", "").strip(),
                        "link": item.get("link", "").strip(),
                    }
                    nsdi_docs.append(report)

    with open("nsdi_docs.json", "w", encoding="utf-8") as f:
        json.dump(nsdi_docs, f, ensure_ascii=False, indent=4)


# ------------------------- Publication Menu Links Extract  ------------------------- #

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:

        config=CrawlerRunConfig (
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for="body section",   
            session_id="hn_session",
            extraction_strategy=JsonCssExtractionStrategy(schema = menu_links_schema),
            magic=False, 
        )
        
        results: List[CrawlResult] = await crawler.arun(url=base_url,config=config,)
        
        for result in results:
            if result.success:
                items = json.loads(result.extracted_content)
                print(f"Extracted {len(items)} Links")
                for item in items:
                    menu_url = ensure_base_url(item.get("url", ""))
                    if menu_url != base_url + "#":
                        menu_links.add(menu_url)  # Add URL to the set

    with open("nsa_menu_links.json", "w", encoding="utf-8") as f:
        json.dump(list(menu_links), f, indent=4)


# ------------------------- Publications Folders Extraction ------------------------- #
    
    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:

        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for=".dlp-folder",  
            session_id="nsa_session",
            extraction_strategy=JsonCssExtractionStrategy(schema=pub_folder_schema),
        )

        results = await crawler.arun(url=pub_url, config=config)

        folder_dict = {}  

        for result in results:
            if result.success:
                if result.extracted_content:
                    folders = json.loads(result.extracted_content)
                    # Build dict {id: name}
                    folder_dict = {f["id"]: f["name"] for f in folders if f.get("id")}
                    print("✅ Folder Dict:", json.dumps(folder_dict, indent=2))
                else:
                    print("⚠️ No extracted content. The elements may not be loaded yet.")
            else:
                print("❌ Crawl failed:", result.error_message)

        # Save after processing all results
        if folder_dict:
            with open("folders.json", "w", encoding="utf-8") as f:
                json.dump(folder_dict, f, indent=2, ensure_ascii=False)

# ------------------------- Extract Publications by Folder ------------------------- #  
           
    async with AsyncWebCrawler(config=BrowserConfig(headless=False, verbose=True)) as crawler:

        with open("folders.json", "r", encoding="utf-8") as f:
            id_name_dict = json.load(f)

        id_name_dict = {k: v for k, v in id_name_dict.items() if v} # Remove empty names

        for category_id in id_name_dict.keys():

            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                scan_full_page=True,
                wait_for=".dlp-category-table tbody tr",  
                session_id="hn_session",
                extraction_strategy=JsonCssExtractionStrategy(schema=pub_docs_schema),
                magic=False,
                js_code=f"""
                    function openFolderAndWait(categoryId, callback) {{
                        const folder = document.querySelector(`li.dlp-folder[data-category-id="${{categoryId}}"]`);
                        if (!folder) {{
                            console.log(`❌ Folder ${{categoryId}} not found`);
                            return;
                        }}
                        const clickable = folder.querySelector(".dlp-icon.folder, .dlp-category-name");
                        if (!clickable) {{
                            console.log("⚠️ No clickable element found inside folder.");
                            return;
                        }}
                        console.log("🖱️ Clicking folder:", categoryId);
                        clickable.click();
                        const observer = new MutationObserver((mutations, obs) => {{
                            if (folder.classList.contains("table-loaded")) {{
                                obs.disconnect();
                                const table = folder.querySelector(".dlp-category-table");
                                if (table) {{
                                    console.log("✅ Table loaded for category:", categoryId);
                                    callback(table);
                                }}
                            }}
                        }});

                        observer.observe(folder, {{ attributes: true, attributeFilter: ["class"] }});
                    }}
                    openFolderAndWait("{category_id}", (table) => {{
                        console.log("📄 Table HTML for category {category_id}:", table.innerHTML);
                    }});
                """,
                page_timeout=60000,
            )

            results: List[CrawlResult] = await crawler.arun(url=pub_url, config=config)

            for result in results:
                if result.success:
                    try:
                        items = json.loads(result.extracted_content)
                    except Exception as e:
                        print(f"⚠️ Could not parse JSON for category {category_id}: {e}")
                        items = []

                    print(f"Extracted {len(items)} items for category ID {category_id}")

                    for item in items:
                        report = {
                            "title": item.get("title", "").strip(),
                            "categories": item.get("categories", "").strip(),
                            "date": item.get("date", "").strip(),
                            "link": item.get("link", "").strip(),
                            "category_id": category_id,
                        }
                        pub_docs.append(report)

    with open("pub_docs.json", "w", encoding="utf-8") as f:
        json.dump(pub_docs, f, ensure_ascii=False, indent=4)

# ------------------------- Census Page Extraction ------------------------- #

    async with AsyncWebCrawler(config=BrowserConfig(headless=False, verbose=True)) as crawler:
        
        config0 = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for="li.menu-item-10792 a[target='_blank']",  
            session_id="hn_session",
            extraction_strategy=JsonCssExtractionStrategy(schema=census_main_report_schema),
            magic=False,
            page_timeout=60000,
        )
        results: List[CrawlResult] = await crawler.arun(url=census_url, config=config0)

        main_links = []
        for result in results:
            if result.success:
                items = json.loads(result.extracted_content)
                print(f"✅ Successfully extracted {len(items)} items")
                for item in items:
                    link = item.get("link")
                    if link and link not in main_links: 
                        main_links.append(link)
                        # Extract file name from URL, remove extension, clean
                        file_name = os.path.basename(link)
                        file_name_no_ext = os.path.splitext(file_name)[0]
                        clean_title = file_name_no_ext.replace('-', ' ')
                        census_docs.append({
                            "main_title": "Census 2023 Products",  # Will propagate to all reports
                            "title": clean_title,
                            "link": link
                        })
                        print(f"📄 Found: {clean_title} -> {link}")
            else:
                print(f"❌ Crawl failed: {result.error_message}")


        config1 = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for=".e-con-inner", 
            session_id="hn_session",
            extraction_strategy=JsonCssExtractionStrategy(schema=census_docs_schema),
            magic=False,
            page_timeout=60000,
        )

        results: List[CrawlResult] = await crawler.arun(url=census_url, config=config1)
        
        for result in results:
            if result.success:
                try:
                    items = json.loads(result.extracted_content)
                    print(f"✅ Successfully extracted {len(items)} items")
                except Exception as e:
                    print(f"⚠️ Could not parse JSON: {e}")
                    print(f"Raw content: {result.extracted_content[:500]}...")
                    items = []

                for item in items:
                    # Only include items that have both title and PDF link
                    if item.get("title") and item.get("link"):
                        report = {
                            "main_title": "Census 2023 Products",  # Main title from the page
                            "title": item.get("title", "").strip(),
                            "link": item.get("link", "").strip(),
                        }
                        census_docs.append(report)
                        print(f"📄 Found: {report['title']} -> {report['link']}")
            else:
                print(f"❌ Crawl failed: {result.error_message}")

    # Save results
    with open("census_docs.json", "w", encoding="utf-8") as f:
        json.dump(census_docs, f, ensure_ascii=False, indent=4)

    print(f"💾 Saved {len(census_docs)} files to census_docs.json")

#  ----------------------- MERGE AND SAVE ALL DOCUMENTS ----------------------

    nsa_data = nss_docs + home_docs + nsdi_docs + census_docs + pub_docs
    print(f"Total documents collected: {len(nsa_data)}")
    # Save merged results
    with open("nsa_data.json", "w", encoding="utf-8") as f:
        json.dump(nsa_data, f, ensure_ascii=False, indent=4)

    print(f"Saved {len(nsa_data)} merged documents to nsa_data.json")

    # Load the JSON data from file
    with open('nsa_data.json', 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Extract all unique links
    links = []
    seen_links = set()

    for item in data:
        link = item.get("link")
        if link and link not in seen_links:
            links.append(link)
            seen_links.add(link)

    # Create a text file with all the links
    file_content = "\n".join(links)

    # Save to a text file
    with open("nsa_all_links.txt", "w", encoding='utf-8') as file:
        file.write(file_content)

    print(f"Links have been saved to 'nsa_all_links.txt'")
    print(f"Total unique links extracted: {len(links)}")
    print(f"Total documents in JSON: {len(data)}")

    # ------------------------- Download files into a folder ---------------- #

    download_folder = 'file_downloads'
    os.makedirs(download_folder, exist_ok=True)

    with open('nsa_all_links.txt', 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]

    failed = []

    for url in urls:
        filename = os.path.join(download_folder, url.split('/')[-1])
        try:
            response = requests.get(url, timeout=60, verify=False)
            response.raise_for_status()
            with open(filename, 'wb') as f_out:
                f_out.write(response.content)

        except Exception as e:
            failed.append(url)

    # Save failed URLs for retry
    if failed:
        with open('failed_downloads.txt', 'w', encoding='utf-8') as f:
            for url in failed:
                f.write(url + '\n')
    else:
        if os.path.exists('failed_downloads.txt'):
            os.remove('failed_downloads.txt')


async def main():
    await namibia()

if __name__ == "__main__":
    asyncio.run(main())