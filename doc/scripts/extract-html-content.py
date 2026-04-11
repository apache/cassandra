# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Extract main content from HTML files for man page generation.
Strips navigation, headers, footers, and other boilerplate.
"""

import sys
import re
from pathlib import Path

def extract_content(html_content):
    """
    Extract main content from HTML, removing navigation and boilerplate.
    Uses simple regex-based approach to avoid external dependencies.
    """
    # Remove script and style tags
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

    # Try to extract main content area (Antora uses <article> or <main>)
    # Look for common content containers
    patterns = [
        r'<article[^>]*>(.*?)</article>',
        r'<main[^>]*>(.*?)</main>',
        r'<div[^>]*class="[^"]*doc[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
    ]

    for pattern in patterns:
        match = re.search(pattern, html_content, flags=re.DOTALL | re.IGNORECASE)
        if match:
            content = match.group(1)
            # Remove navigation elements within content
            content = re.sub(r'<nav[^>]*>.*?</nav>', '', content, flags=re.DOTALL | re.IGNORECASE)
            content = re.sub(r'<div[^>]*class="[^"]*nav[^"]*"[^>]*>.*?</div>', '', content, flags=re.DOTALL | re.IGNORECASE)
            content = re.sub(r'<aside[^>]*>.*?</aside>', '', content, flags=re.DOTALL | re.IGNORECASE)
            return content

    # Fallback: remove common navigation/header/footer elements
    html_content = re.sub(r'<header[^>]*>.*?</header>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<footer[^>]*>.*?</footer>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<nav[^>]*>.*?</nav>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<div[^>]*class="[^"]*nav[^"]*"[^>]*>.*?</div>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<aside[^>]*>.*?</aside>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

    # Remove body tag but keep content
    html_content = re.sub(r'<body[^>]*>', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'</body>', '', html_content, flags=re.IGNORECASE)

    return html_content

def main():
    if len(sys.argv) != 2:
        print("Usage: extract-html-content.py <input.html>", file=sys.stderr)
        print("Extracts main content from HTML and writes to stdout", file=sys.stderr)
        sys.exit(1)

    input_file = Path(sys.argv[1])

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)

    try:
        html_content = input_file.read_text(encoding='utf-8')
        content = extract_content(html_content)
        print(content, end='')
    except Exception as e:
        print(f"Error processing {input_file}: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
