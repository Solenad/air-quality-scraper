#!/usr/bin/env python3
"""Air Quality Data Scraper — CLI entrypoint.

Usage
-----
    python main.py --source openaq
"""
import asyncio
import sys

from aq_scraper.__main__ import main

if __name__ == "__main__":
    asyncio.run(main())
