import asyncio
import csv
import json
import pandas as pd
from typing import Dict, List
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy, BrowserConfig
import os
import requests
import urllib3

# Disable only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ✅ Import log_message from app.py
try:
    from app import log_message
except ImportError:
    # fallback if run standalone
    def log_message(message, level="info"):
        print(f"[{level.upper()}] {message}")


async def js_interaction():
    """Extract files from all pagination pages"""
    log_message("[INIT].... → KNBS Reports Extraction with Pagination started", "info")
    
    base_url = "https://www.knbs.or.ke/"
    all_menus = set()
    main_article_urls = set()
    more_urls = set()
    all_reports = []
    file_details_dict: Dict[str, set] = {} # Store more article detail URLs 
    load_more_dict: Dict[str, set] = {}
    page_links_dict: Dict[str, set] = {}  # Store pagination URLs per menu

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
    
    # ---------------- Ectraction schemas ----------------

    menu_links_schema = {
    "name": "menu_links",
    "baseSelector": "header .w-nav-list a",
    "type": "list",
    "fields": [
        {
            "name":"title",
            "selector": ".w-nav-title",
            "type": "text"
        },
        {
            "name": "url",
            "type": "attribute",
            "attribute": "href"
        }
        ]
    }
    nav_links= {
        "name": "nav_links",
        "baseSelector": "body main.l-main",
        "fields": [
            {
                "name": "next_page",
                "selector": "nav.pagination.navigation a.next.page-numbers",
                "type": "attribute",
                "attribute": "href"
            },
        ]
    }
    article_schema = {
        "name": "file_links",
        "baseSelector": "article.w-grid-item",
        "fields": [
            {
                "name": "url",
                "selector": "a.w-btn.us-btn-style_7.usg_btn_2.icon_atleft",
                "type": "attribute",
                "attribute": "href"
            },
        ]
    }
    more= {
        "name": "file_links",
        "baseSelector": "article.w-grid-item",
        "fields": [
            {
                "name": "url",
                "selector": "a.w-btn.us-btn-style_7.usg_btn_1.icon_atleft",
                "type": "attribute",
                "attribute": "href"
            },
        ]
    }
    pdf_links = {
        "name": "xlsx_links",
        "description":"Extract all XLSX titles and links",
        "baseSelector": ".l-main .l-section.wpb_row.height_large .w-btn-wrapper",
        "type": "list",
        "fields": [
            {
                "name":"pdf",
                "selector": "a[href$='.pdf']",
                "type": "attribute",
                "attribute": "href",
                "multiple": True
            }
        ]
    }
    xlsx__links = {
        "name": "xlsx_links",
        "description":"Extract all XLSX titles and links",
        "baseSelector": ".l-main .l-section.wpb_row.height_large .wpb_wrapper p",
        "type": "list",
        "fields": [
            {
                "name":"xlsx",
                "selector": "a[href$='.xlsx']",
                "type": "attribute",
                "attribute": "href",
                "multiple": True
            }
        ]
    }
    more_details = {
        "name": "more_details",
        "baseSelector": "body .l-main",
        "type": "list",
        "fields": [
            {"name": "main_report_title","selector": "h1.entry-title","type": "text"},
            {"name": "main_report_url", "selector":"a.w-btn.us-btn-style_6", "type": "attribute", "attribute": "href"},
            {"name": "main_category","selector": ".main_category span","type": "text"},
            {"name": "sub_category","selector": ".sub_category","type": "text"},
            {"name": "post_month","selector": ".month span","type": "text"},
            {"name":"post_year","selector": ".year span","type": "text"},
            {"name": "overview","selector": ".report_short_description p","type": "text"},
        ]
    }

    # ---------------- Extract Menu Links ----------------#
       
    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
        log_message(f"[FETCH]... ↓ {base_url}", "info")
        config=CrawlerRunConfig (
            cache_mode=CacheMode.BYPASS,
            scan_full_page=True,
            wait_for="body",  # Wait until banner is gone
            session_id="hn_session",
            extraction_strategy=JsonCssExtractionStrategy(schema = menu_links_schema),
            magic=True,
            js_code="document.querySelector('.l-cookie button#us-set-cookie.w-btn.us-btn-style_1')?.click();"
        )
        results: List[CrawlResult] = await crawler.arun(url=base_url,config=config,)
        for result in results:
            if result.success:
                items = json.loads(result.extracted_content)
                for item in items:
                    menu_url = ensure_base_url(item.get("url", ""))
                    if menu_url != base_url + "#":
                        all_menus.add(menu_url)  # Add URL to the set
                        log_message(f"[SCRAPE].. ◆ menu: {menu_url}", "info")

    # ---------------- Pagination Loop for each menu links ----------------#
         
    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
        for url in all_menus: # replace all_menus by all_menu_links if you want to use all_menus_links.json file above.
            page_links = set() # initialize set
            current_url = url
            # ✅ Add home page first
            page_links.add(current_url)
            while current_url:
                log_message(f"[FETCH]... ↓ {current_url}", "info")
                config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    scan_full_page=True,
                    wait_for="body .l-main",
                    extraction_strategy=JsonCssExtractionStrategy(schema=nav_links),
                    session_id="hn_session",
                    magic=True,
                    js_code = "document.querySelector('.l-cookie button#us-set-cookie.w-btn.us-btn-style_1')?.click();"
                )
                results: List[CrawlResult] = await crawler.arun(url=current_url, config=config,)
                next_url = None
                for result in results:
                    if result.success:
                        items = json.loads(result.extracted_content)
                        for item in items:
                            next_page = item.get("next_page", "")
                            if next_page:
                                next_url = ensure_base_url(next_page)
                                if next_url not in page_links:
                                    page_links.add(next_url)
                                    log_message(f"[SCRAPE].. ◆ pagination: {next_url}", "info")
                current_url = next_url

            page_links_dict[url] = page_links # Store all pagination URLs for this menu

    # ---------------- Saving page_links_dict to a JSON file  ----------------
    
    with open("knbs_page_links.json", "w", encoding="utf-8") as f:
        json.dump({k: list(v) for k, v in page_links_dict.items()}, f, ensure_ascii=False, indent=4) # Convert sets to lists for JSON compatibility
    log_message("[COMPLETE] Saved pagination links → knbs_page_links.json", "success")
    
    # ---------------- Main article links per page ----------------#

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
        for url in page_links:
            log_message(f"[FETCH]... ↓ {url}", "info")
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                scan_full_page=True,
                wait_for="body main.l-main .w-grid-list article.w-grid-item a.usg_btn_2",
                extraction_strategy=JsonCssExtractionStrategy(schema=article_schema),
                session_id="hn_session",
                magic=True,
                js_code="document.querySelector('.l-cookie button#us-set-cookie.w-btn.us-btn-style_1')?.click();"
            )
            results: List[CrawlResult] = await crawler.arun(url=url,config=config,)
            if not results:
                log_message("🚫 No results, stopping.", "warning")
                break
            extracted_any = False
            for result in results:
                if result.success:
                    items = json.loads(result.extracted_content)
                    for item in items:
                        url = item.get("url", "")
                        if url and url not in main_article_urls:
                            main_article_urls.add(url)
                            extracted_any = True
                            log_message(f"[EXTRACT]. ■ Found article: {url}", "info")

            if not extracted_any:
                log_message("🚫 No new items → last page reached.", "warning")
                break
    
    # ---------------- Main article more button links ----------------#

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:

        with open("knbs_page_links.json", "r", encoding="utf-8") as f:
            page_links_dict = json.load(f)
        page_links_dict = {k: set(v) for k, v in page_links_dict.items()}  # If you want sets again
        # print("✅ Loaded pagination links from file")

        for page_urls in page_links_dict.values(): # loop over the lists of URLs for each menu
            for page_url in page_urls: # iterate each paginated page 
                load_more_btn_urls = [
                    "https://www.knbs.or.ke/statistical-abstracts/",
                    "https://www.knbs.or.ke/economic-surveys/",
                    "https://www.knbs.or.ke/county-statistical-abstracts/",
                    "https://www.knbs.or.ke/general-publications/"
                ]
                skip_urls= [   
                    "https://www.knbs.or.ke/about/",
                    "https://www.knbs.or.ke/macroeconomic-statistics-directorate/",
                    "https://www.knbs.or.ke/about/#vmc",
                    "https://www.knbs.or.ke/videos/",
                    "https://www.knbs.or.ke/board-of-directors/",
                    "https://www.knbs.or.ke/reports/kenya-census-1999/",
                    "https://www.knbs.or.ke/reports/kenya-census-2009/",
                    "https://www.knbs.or.ke/reports/kenya-census-2019/",
                    "https://www.knbs.or.ke/kenstats/",
                    "https://www.knbs.or.ke/partners/",
                    "https://www.knbs.or.ke/statistical-coordination-methods-directorate/",
                    "https://www.knbs.or.ke/photos/",
                    "https://www.knbs.or.ke/tenders/",
                    "https://www.knbs.or.ke/ongoing-surveys/",
                    "https://www.knbs.or.ke/portals/",
                    "https://www.knbs.or.ke/statistical-releases/",
                    "https://www.knbs.or.ke/about/#history",
                    "https://www.knbs.or.ke/top-management/",
                    "https://www.knbs.or.ke/about/#mandate",
                    "https://www.knbs.or.ke/jobs/",
                    "https://www.knbs.or.ke/director-general-office/",
                    "https://www.knbs.or.ke/directorates/",
                    "https://www.knbs.or.ke/knbs-sdgs/",
                    "https://www.knbs.or.ke/service-delivery-charter/",
                    "https://www.knbs.or.ke/quality-policy/",
                    "https://www.knbs.or.ke/about/kenya-statistics-code-of-practice-kescop/",
                    "https://www.knbs.or.ke/population-and-social-statistics-directorate/",
                    "https://www.knbs.or.ke/iso-certification/",
                    "https://www.knbs.or.ke/production-statistics-directorate/",
                    "https://www.knbs.or.ke/internships/",
                    "https://www.knbs.or.ke/strategic-plan/",
                    "https://www.knbs.or.ke/data-revision-policy/",
                    "https://www.knbs.or.ke/corporate-services-directorate/",
                    "https://www.knbs.or.ke/news-and-events/page/4/",
                    "https://www.knbs.or.ke/news-and-events/page/5/",
                    "https://www.knbs.or.ke/news-and-events/page/9/",
                    "https://www.knbs.or.ke/news-and-events/page/3/",
                    "https://www.knbs.or.ke/news-and-events/page/10/",
                    "https://www.knbs.or.ke/news-and-events/page/12/",
                    "https://www.knbs.or.ke/news-and-events/page/15/",
                    "https://www.knbs.or.ke/news-and-events/page/13/",
                    "https://www.knbs.or.ke/news-and-events/page/14/",
                    "https://www.knbs.or.ke/news-and-events/page/11/",
                    "https://www.knbs.or.ke/news-and-events/",
                    "https://www.knbs.or.ke/news-and-events/page/7/",
                    "https://www.knbs.or.ke/news-and-events/page/17/",
                    "https://www.knbs.or.ke/news-and-events/page/2/",
                    "https://www.knbs.or.ke/news-and-events/page/8/",
                    "https://www.knbs.or.ke/news-and-events/page/6/",
                    "https://www.knbs.or.ke/news-and-events/page/16/"
                ]
               
                if page_url in load_more_btn_urls :
                    # 🔹 Applying custom condition for load_more_btn URLs
                    print(f"⚡ Special handling for {page_url}")
                    file_details = set() # ✅ reset once per page
                    # JavaScript code to automatically click "Load More" until all content is loaded
                    js_code = """
                    (async () => {
                        let previousCount = 0;
                        while (true) {
                            const items = document.querySelectorAll(".w-grid-item");
                            if (items.length === previousCount) {
                                console.log("✅ All items loaded.");
                                break; // no new items loaded, stop
                            }
                            previousCount = items.length;

                            const btn = document.querySelector("button.w-btn.us-btn-style_1");
                            if (!btn) {
                                console.log("✅ No Load More button found.");
                                break; // no button, stop
                            }

                            btn.click();
                            console.log("🔄 Clicked Load More, waiting for new items...");
                            await new Promise(r => setTimeout(r, 20000)); // wait 20s for new items
                        }
                    })();
                    """
                    load_more = CrawlerRunConfig(
                        cache_mode=CacheMode.BYPASS,
                        scan_full_page=True,
                        wait_for="document.querySelectorAll('article.w-grid-item').length > 100",
                        extraction_strategy=JsonCssExtractionStrategy(schema=more),
                        session_id="hn_session",
                        magic=True,
                        js_code=js_code,
                        page_timeout= 30000
                    )
                    results: List[CrawlResult] = await crawler.arun(url=page_url,config=load_more,)
                    for result in results:
                        if result.success:
                            items = json.loads(result.extracted_content)
                            for item in items:
                                more_url  = item.get("url", "")
                                if  more_url and more_url not in file_details:
                                    file_details.add(more_url)

                    # ✅ store links for this specific page
                    file_details_dict[page_url] = file_details
                elif page_url in skip_urls:
                    print(f"⏭️ Skipping {page_url} as per skip list.")
                    continue
                else:
                    print(f"✅ Normal handling for {page_url}")
                    more_urls = set()   # 🔹 reset for each page_url
                    config_more = CrawlerRunConfig(
                        cache_mode=CacheMode.BYPASS,
                        scan_full_page=True,
                        wait_for="body main.l-main .w-grid-list article.w-grid-item a.usg_btn_1",
                        extraction_strategy=JsonCssExtractionStrategy(schema=more),
                        session_id="hn_session",
                        magic=True,
                        js_code="document.querySelector('.l-cookie button#us-set-cookie.w-btn.us-btn-style_1')?.click();"
                    )
                    results: List[CrawlResult] = await crawler.arun(url=page_url,config=config_more,)
                    extracted_any = False
                    for result in results:
                        if result.success:
                            items = json.loads(result.extracted_content)
                            for item in items:
                                more_url  = item.get("url", "")
                                if  more_url and more_url not in more_urls:
                                    more_urls.add(more_url)
                                    extracted_any = True

                    if not extracted_any:
                        print("🚫 No new 'more' items → probably last page reached.")
                        break

                    # ✅ store links for this specific page (now only its own)
                    load_more_dict[page_url] = more_urls

    # Save file_details_dict (convert sets → lists)
    with open("more_links_1.json", "w", encoding="utf-8") as f:
        json.dump({k: list(v) for k, v in file_details_dict.items()}, f, ensure_ascii=False, indent=4)
    print(f"\n🎉 Done! Saved {len(file_details_dict)} pages of links into more_links_1.json")

    # Save load_more_dict (convert sets → lists)
    with open("more_links_2.json", "w", encoding="utf-8") as f:
        json.dump({k: list(v) for k, v in load_more_dict.items()}, f, ensure_ascii=False, indent=4)
    print(f"\n🎉 Done! Saved {len(load_more_dict)} pages of links into more_links_2.json")

    # Load both files
    with open("more_links_1.json", "r", encoding="utf-8") as f1:
        data1 = json.load(f1)

    with open("more_links_2.json", "r", encoding="utf-8") as f2:
        data2 = json.load(f2)

    merged = {}

    # Merge file1
    for key, value in data1.items():
        merged.setdefault(key, []).extend(value)

    # Merge file2
    for key, value in data2.items():
        merged.setdefault(key, []).extend(value)

    # Save merged result
    with open("knbs_file_details_links.json", "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=4)

    # ---------------- Extract Unique URLs from knbs file details links ----------------#

    with open("knbs_file_details_links.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # Collect all URLs from all values (ignore keys)
    all_urls = []
    for urls in data.values():
        all_urls.extend(urls)

    # Deduplicate
    unique_urls = list(set(all_urls))

    # Save to file
    with open("unique_knbs_urls.txt", "w", encoding="utf-8") as f:
        for url in unique_urls:
            f.write(url + "\n")

    print(f"Extracted {len(unique_urls)} unique URLs to unique_knbs_urls.txt")

    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
        
        with open("unique_knbs_urls.txt", "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]

        for url in urls:
            # 🔥 Reset per report
            pdf_files = []
            xlsx_files = []
            
            # ---------------- PDF extraction ----------------

            config_pdf = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                scan_full_page=True,
                wait_for=".l-main .l-section.wpb_row.height_large ",
                extraction_strategy=JsonCssExtractionStrategy(schema=pdf_links),
                session_id="hn_session",
                magic=True,
                js_code="document.querySelector('.l-cookie button#us-set-cookie.w-btn.us-btn-style_1')?.click();"
            )
            results: List[CrawlResult] = await crawler.arun(url=url,config=config_pdf,)
            for result in results:
                if result.success:
                    items = json.loads(result.extracted_content)
                    for item in items:
                        pdf_link = item.get("pdf", [])
                        if pdf_link and pdf_link not in pdf_files:
                            pdf_files.append(pdf_link)

            # ---------------- XLSX extraction ----------------

            config_xlsx = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                scan_full_page=True,
                wait_for=".l-main .l-section.wpb_row.height_large ",
                extraction_strategy=JsonCssExtractionStrategy(schema=xlsx__links),
                session_id="hn_session",
                magic=True,
                js_code="document.querySelector('.l-cookie button#us-set-cookie.w-btn.us-btn-style_1')?.click();"
            )
            results: List[CrawlResult] = await crawler.arun(url=url,config=config_xlsx,)
            for result in results:
                if result.success:
                    items = json.loads(result.extracted_content)
                    for item in items:
                        xlsx_link = item.get("xlsx", [])
                        if xlsx_link and xlsx_link not in xlsx_files:
                            xlsx_files.append(xlsx_link)

            # ---------------- Main Report extraction ----------------
            
            config_more = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                scan_full_page=True,
                wait_for="body main.l-main",
                extraction_strategy=JsonCssExtractionStrategy(schema=more_details),
                session_id="hn_session",
                magic=True,
                js_code="document.querySelector('.l-cookie button#us-set-cookie.w-btn.us-btn-style_1')?.click();"
            )
            results: List[CrawlResult] = await crawler.arun(url=url,config=config_more,)
            for result in results:
                if result.success:
                    items = json.loads(result.extracted_content)
                    for item in items:
                        main_url = item.get("main_report_url", "")
                        # Remove main_report_url from pdf_files if present
                        pdf_files_cleaned = [link for link in pdf_files if link != main_url]
                        report = {
                            "main_report_title": item.get("main_report_title", ""),
                            "main_category": item.get("main_category", ""),
                            "sub_category": item.get("sub_category", ""),
                            "post_month": item.get("post_month", ""),
                            "post_year": item.get("post_year", ""),
                            "overview": item.get("overview", ""),
                            "main_report_url": main_url,
                            "pdf_files": pdf_files_cleaned,
                            "xlsx_files": xlsx_files,
                        }
                        all_reports.append(report)

    # ---------------- ✅ Save output as JSON ----------------
    
    with open("knbs_files.json", "w", encoding="utf-8") as f:
        json.dump(all_reports, f, ensure_ascii=False, indent=4)
    log_message(f"[COMPLETE] Extracted {len(all_reports)} reports → knbs_files.json", "success")
    # ---------------- Load JSON data ---------------- #
    with open('knbs_files.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    # ---------------- Collect all URLs ---------------- #
    urls = []
    for entry in data:
        if entry.get('main_report_url'):
            urls.append(entry['main_report_url'])
        urls.extend(entry.get('pdf_files', []))
        urls.extend(entry.get('xlsx_files', []))
    
    # To avoid duplication in urls.txt, convert the urls list to a set before writing to the file. A set automatically removes duplicate values. 
    unique_urls = set(urls)

    # ---------------- Save URLs to a file ---------------- #
    with open('urls.txt', 'w', encoding='utf-8') as out:
        for url in sorted(unique_urls):  # sorting for consistency
            out.write(url + '\n')
    
    # //////////////////////////////////////////////////////////// #
    
    # ---------------- Download files into a folder ---------------- #
    download_folder = 'file_downloads'
    os.makedirs(download_folder, exist_ok=True)

    with open('urls.txt', 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]

    failed = []

    for url in urls:
        filename = os.path.join(download_folder, url.split('/')[-1])
        try:
            response = requests.get(url, timeout=60, verify=False)
            response.raise_for_status()
            with open(filename, 'wb') as f_out:
                f_out.write(response.content)
            log_message(f"[DOWNLOAD] ✓ {filename}", "success")
        except Exception as e:
            log_message(f"[ERROR] Failed to download {url}: {e}", "error")
            failed.append(url)

    # Save failed URLs for retry
    if failed:
        with open('failed_downloads.txt', 'w', encoding='utf-8') as f:
            for url in failed:
                f.write(url + '\n')
        log_message(f"[WARN] {len(failed)} downloads failed. See failed_downloads.txt", "warning")
    else:
        log_message("[COMPLETE] ● All files downloaded successfully", "success")



async def main():
    await js_interaction()

if __name__ == "__main__":
    asyncio.run(main())
