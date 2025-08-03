# General-purpose Web Scraping Tool

This project is aimed at developing a **general-purpose web scraping tool** designed to extract structured and unstructured data from the **National Statistical Office (NSO)** websites of various member states.

The tool navigates through public statistical web portals and automatically extracts:
- Downloadable files (PDFs, Excel, CSV)
- Dataset titles and metadata
- News articles and reports
  

## 🌍 Project Objective

Statistical data from NSOs is crucial for informed decision-making in development, research, policy formulation, and governance. However, this data is often scattered across different formats and platforms. This tool provides a unified way to collect such data systematically, especially where APIs are not available.

## ⚙️ Features

- 🌐 Scrapes full websites (recursive link traversal)
- 📁 Automatically detects and downloads data files (PDF, XLS, CSV, etc.)
- 🧠 Extracts metadata: titles, dates, descriptions
- 🕸️ Supports both synchronous and asynchronous crawling
- 🔧 Configurable and reusable for multiple country NSO domains
- 🛡️ Proxy and timeout support for robust crawling

