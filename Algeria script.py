import asyncio
import os
import json
import base64
from pathlib import Path
from typing import List
from crawl4ai import ProxyConfig
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import RoundRobinProxyStrategy
from crawl4ai import JsonCssExtractionStrategy, LLMExtractionStrategy
from crawl4ai import LLMConfig
from crawl4ai import PruningContentFilter, BM25ContentFilter
from crawl4ai import DefaultMarkdownGenerator
from crawl4ai import BFSDeepCrawlStrategy, DomainFilter, FilterChain
from crawl4ai import BrowserConfig

__cur_dir__ = Path(__file__).parent

base_url = "https://www.ons.dz/"

def ensure_base_url(url):
    """Convert relative URLs to absolute URLs"""
    if not url.startswith(("http://", "https://")):
        # Handle different relative path formats
        if url.startswith("./"):
            url = url[2:]
        elif url.startswith("/"):
            url = url[1:]
        return base_url + url
    return url

async def js_interaction():
    """Hierarchical menu extraction with 3 levels"""
    print("\n=== ONS Algeria Menu Extraction ===")

    # Define schemas for different menu levels
    main_schema = {
        "name": "news",
        "baseSelector": ".barre-noire .list-inline > li",
        "type": "list",
        "fields": [
            {
                "name": "title",
                "selector": "a:first-child",
                "type": "text", 
            },
            {
                "name": "url",
                "selector": "a:first-child",
                "type": "attribute",
                "attribute": "href",
            }
        ]
    }
    
    submenu_schema = {
        "name": "submenu",
        "baseSelector": ".trait-droite table.title",
        "type": "list",
        "fields": [
            {
                "name": "title",
                "selector": "a",
                "type": "text"
            },
            {
                "name": "url",
                "selector": "a",
                "type": "attribute",
                "attribute": "href"
            }
        ]
    }
    # Selector for child items (level 3)
    child_schema = {
        "name": "childs",
        "baseSelector": ".titres-articles ",
        "type": "list",
        "fields": [
            {
                "name": "title",
                "selector": "a",
                "type": "text",
                "postprocess": "trim",
                "transform": "lambda x: x.split(';')[-1].strip() if ';' in x else x.strip()"
            },
            {
                "name": "url",
                "selector": "a",
                "type": "attribute",
                "attribute": "href"
            }
        ]
    }
    
    pdf_xls_schema = {
        "name": "doc_links",
        "baseSelector": "#documents_joints ul.lister-documents li",
        "type": "list",
        "fields": [
            {
                "name": "url",
                "selector": "a[href][type='application/pdf'], a[href][type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']",
                "type": "attribute",
                "attribute": "href"
            }
        ]
    }
    docs_schema = {
        "name": "documents",
        "baseSelector": "#document_articles ul li",
        "type": "list",
        "fields": [
            {
                "name": "url",
                "selector": ".spip_doc_titre a[type='application/pdf'], .spip_doc_titre a[type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']",
                "type": "attribute",
                "attribute": "href"
            }
        ]

    }
    
    subchild_schema = child_schema

    # A simple page that needs JS to reveal content
    async with AsyncWebCrawler(config=BrowserConfig(headless=False)) as crawler:
         # Level 1: Main Menu
        results: List[CrawlResult] = await crawler.arun(
            url="https://www.ons.dz/",
            config=CrawlerRunConfig(
                session_id="hn_session",
                extraction_strategy=JsonCssExtractionStrategy(schema=main_schema),
                wait_for=".barre-noire .list-inline > li",
                magic=True,
            ),
        )
        
        for result in results:
            if result.success:
                items = json.loads(result.extracted_content)               
                for item in items:
                    if item["title"].lower() == "accueil":
                        continue
                    item["url"] = ensure_base_url(item["url"])
                    print(f"\n[MAIN MENU] {item['title']}: {item['url']}")
                    
                    # Level 2: Submenu
                    submenu_results: List[CrawlResult] = await crawler.arun(
                        url=item["url"],
                        config=CrawlerRunConfig(
                            session_id="hn_session",
                            extraction_strategy=JsonCssExtractionStrategy(schema=submenu_schema),
                        ),
                    )
                    
                    for submenu_result in submenu_results:
                        if submenu_result.success:
                            submenu_items = json.loads(submenu_result.extracted_content)
                            for submenu_item in submenu_items:
                                submenu_item["url"] = ensure_base_url(submenu_item["url"])
                                print(f"  [SUBMENU] {submenu_item['title']}: {submenu_item['url']}")

                                # Level 3: Child Items
                                child_results: List[CrawlResult] = await crawler.arun(
                                    url=submenu_item["url"],
                                    config=CrawlerRunConfig(
                                        session_id="hn_session",
                                        extraction_strategy=JsonCssExtractionStrategy(schema=child_schema),
                                    ),
                                )
                                for child_result in child_results:
                                    if child_result.success:
                                        child_items = json.loads(child_result.extracted_content)
                                        for child_item in child_items:
                                            child_item["url"] = ensure_base_url(child_item["url"])
                                            print(f"    [CHILD] {child_item['title']}: {child_item['url']}")

                                            # Level 4: Subchild Items
                                            subchild_results: List[CrawlResult] = await crawler.arun(
                                                url=child_item["url"],
                                                config=CrawlerRunConfig(
                                                    session_id="hn_session",
                                                    extraction_strategy=JsonCssExtractionStrategy(schema=subchild_schema),
                                                ),
                                            )
                                            for subchild_result in subchild_results:
                                                if subchild_result.success:
                                                    subchild_items = json.loads(subchild_result.extracted_content)
                                                    for subchild_item in subchild_items:
                                                        subchild_item["url"] = ensure_base_url(subchild_item["url"])
                                                        print(f"        [SUBCHILD] {subchild_item['url']}")

                                                        docs_results: List[CrawlResult] = await crawler.arun(
                                                            url=subchild_item["url"],
                                                            config=CrawlerRunConfig(
                                                                session_id="hn_session",
                                                                extraction_strategy=JsonCssExtractionStrategy(schema=docs_schema),
                                                            ),
                                                        )
                                                        for docs_result in docs_results:
                                                            if docs_result.success:
                                                                docs = json.loads(docs_result.extracted_content)
                                                                for doc in docs:
                                                                    doc_url = doc.get("url")
                                                                    if doc_url:
                                                                        print(f"          [DOCUMENT LINK] - {doc_url}")
                                                            else:
                                                                print(f"        [ERROR] Failed to extract document links for {subchild_item['url']}")

                                    else:
                                        print(f"    [ERROR] Failed to extract child items for {submenu_item['url']}")
                                
                                # Extract PDF links if available
                                pdf_xls_results: List[CrawlResult] = await crawler.arun(
                                                url=submenu_item["url"],
                                                config=CrawlerRunConfig(
                                                    session_id="hn_session",
                                                    extraction_strategy=JsonCssExtractionStrategy(schema=pdf_xls_schema),
                                                ),
                                            )
                                for pdf_xls_result in pdf_xls_results:
                                    if pdf_xls_result.success:
                                        docs = json.loads(pdf_xls_result.extracted_content)
                                        for doc in docs:
                                            doc_url = doc.get("url")
                                            if doc_url:
                                                print(f"        [DOCUMENT LINK] - {doc_url}")
                                    else:
                                        print(f"    [ERROR] Failed to extract document links for {submenu_item['url']}")
                                            
                        else:
                             print("  [ERROR] Failed to extract submenu data")
            else:
                print("Failed to extract structured data")

async def main():
    await js_interaction()

if __name__ == "__main__":
    asyncio.run(main())