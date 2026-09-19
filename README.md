# Multi Crawler

A collection of modified open-source crawlers and download tools for experimenting with large-scale public web data collection.

This repository combines several existing open-source projects with custom modifications for testing crawling performance, data extraction, output formatting, and automation.

> Note: Each project may originate from a different open-source repository. Please refer to the LICENSE and original project documentation inside each folder for licensing and usage restrictions.

---

## Projects

### 1. MediaCrawler

`MediaCrawler/`

A multi-platform social media crawler.

Main purposes:

- Crawl publicly accessible social media content
- Extract posts and related metadata
- Support browser-based crawling and authenticated sessions
- Test crawling speed and concurrency
- Export collected data for further processing

Custom modifications in this repository are mainly used for crawler testing, login/session handling, and data collection workflows.

---

### 2. Academia Preserver

`academia-preserver/`

A crawler/downloader designed for testing content discovery and document retrieval workflows on Academia-related pages.

Main purposes:

- Discover document entries
- Extract document metadata
- Test pagination and lazy-loading behaviour
- Measure crawling throughput
- Save structured results for later processing

This project is mainly used for experimentation with document-oriented crawling.

---

### 3. Scribd Downloader

`scribd-downloader/`

A modified document retrieval project for Scribd-related content.

Main purposes:

- Process Scribd document URLs
- Extract available document information
- Test document download workflows
- Automate repetitive document processing
- Save results locally for later conversion or analysis

Downloaded files are excluded from this Git repository.

---

### 4. SlideShare Downloader

`slidesharedl-py/`

A Python-based SlideShare processing/downloading tool.

Main purposes:

- Process SlideShare presentation URLs
- Extract presentation information
- Retrieve available slide resources
- Automate presentation processing
- Save downloaded presentation assets locally

Generated and downloaded files are excluded from Git tracking.

---

### 5. twscrape

`twscrape/`

A modified Twitter/X scraping project based on the open-source `twscrape` project.

Main purposes:

- Collect publicly available posts and metadata
- Perform search-based data collection
- Handle larger batches of requests
- Export structured social media data
- Test crawling performance and throughput

The modified version is included here for integration with the wider Multi Crawler workflow.

---

## Repository Structure

```text
Multi_Crawler/
│
├── MediaCrawler/
├── academia-preserver/
├── scribd-downloader/
├── slidesharedl-py/
├── twscrape/
├── Converter/
├── .gitignore
└── README.md