#!/usr/bin/env python3
"""Generate native-protocol HTML and asciidoc summary from .spec files."""

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

import argparse
import sys
import re
import html
import io
from pathlib import Path
from typing import List

_comment_re = re.compile(r'^#\s?(.*)$')
_empty_re = re.compile(r'^\s*$')
_title_re = re.compile(r'^\s+(.*)\s*$')
_heading_re = re.compile(r'^(?P<indent>\s*)(?P<number>\d+(?:\.\d+)*)\.?\s+(?P<title>[A-Za-z_].+)$')
_toc_entry_re = re.compile(r'^(?P<number>\d+(?:\.\d+)*)\.?\s+(?P<title>.+)$')
_url_re = re.compile(r'(https?://[^\s)]+)')
_URL_TRAILING_PUNCT = '.,;:!?'
_protocol_filename_re = re.compile(r'^native_protocol_v(\d+)\.(?:spec|html)$')


def _skip_blank(lines: List[str], idx: int) -> int:
    while idx < len(lines) and _empty_re.match(lines[idx]):
        idx += 1
    return idx


def parse_spec_file(path: Path) -> dict:
    """Parse a native_protocol_v*.spec file into license, title, TOC, and sections."""
    text = path.read_text(encoding='utf-8')
    lines = text.splitlines()
    idx = 0

    # License
    license_lines = []
    while idx < len(lines):
        m = _comment_re.match(lines[idx])
        if not m:
            break
        license_lines.append(m.group(1))
        idx += 1
    idx = _skip_blank(lines, idx)

    # Titles
    m = _title_re.match(lines[idx]) if idx < len(lines) else None
    if not m:
        sys.exit(f"Parse error: missing or malformed title at line {idx + 1}")
    title = m.group(1)
    idx += 1
    idx = _skip_blank(lines, idx)

    # Table of Contents
    if idx >= len(lines) or lines[idx] != "Table of Contents":
        sys.exit(f"Parse error: expected 'Table of Contents' at line {idx + 1}")
    idx += 1
    idx = _skip_blank(lines, idx)

    # TOC
    toc = []
    while idx < len(lines) and lines[idx].strip():
        line = lines[idx].strip()
        m = _toc_entry_re.match(line)
        if not m:
            sys.exit(f"Parse error: bad TOC entry at line {idx + 1}")
        toc.append({'number': m.group('number'), 'title': m.group('title')})
        idx += 1
    idx = _skip_blank(lines, idx)

    # Sections distinguishing real headings from prose/list-items
    sections = []
    current = None
    for line in lines[idx:]:
        m = _heading_re.match(line)
        if m:
            num = m.group('number')
            sec_title = m.group('title').rstrip()
            if '.' in num or m.group('indent') == '':
                if current:
                    sections.append(current)
                current = {'number': num, 'title': sec_title, 'body': []}
                continue
        if current:
            current['body'].append(line)

    if current:
        sections.append(current)

    return {
        'license': license_lines,
        'title': title,
        'toc': toc,
        'sections': sections,
    }


def build_toc_tree(entries, nums):
    """Build a nested TOC tree from flat entries; mark which numbers exist as sections."""
    root = {'children': []}
    stack = [root]
    for e in entries:
        lvl = e['number'].count('.') + 1
        stack = stack[:lvl]
        parent = stack[-1]
        node = {'entry': e, 'exists': e['number'] in nums, 'children': []}
        parent['children'].append(node)
        stack.append(node)
    return root['children']


_section_multi_re = re.compile(
    r'([sS]ections)(\s+\d+(?:\.\d+)*(?:,?\s+(?:and\s+)?\d+(?:\.\d+)*)+)'
)


_section_single_re = re.compile(r'([sS]ection (\d+(?:\.\d+)*))')
_num_re = re.compile(r'\d+(?:\.\d+)*')


def _linkify_section_refs(text: str) -> str:
    def repl_multi(m):
        return m.group(1) + _num_re.sub(
            lambda n: f'<a href="#s{n.group(0)}">{n.group(0)}</a>', m.group(2))
    text = _section_multi_re.sub(repl_multi, text)
    text = _section_single_re.sub(
        lambda m: f'<a href="#s{m.group(2)}">{m.group(1)}</a>', text)
    return text


def _linkify_url(m):
    url = m.group(1)
    trailing = ''
    while url and url[-1] in _URL_TRAILING_PUNCT:
        trailing = url[-1] + trailing
        url = url[:-1]
    return f'<a href="{url}">{url}</a>{trailing}'


def format_body(lines):
    """Render section body lines as escaped HTML with URL and section-ref linkification."""
    text = "\n".join(lines)
    if text.startswith('\n'):
        text = text[1:]
    escaped = html.escape(text)
    with_urls = _url_re.sub(_linkify_url, escaped)
    linked = _linkify_section_refs(with_urls)
    # Transcode entity names to byte-match the previous (go) tools output.
    linked = linked.replace('&quot;', '&#34;').replace('&#x27;', '&#39;')
    return '<pre>' + linked + '</pre>'


def build_sections(secs):
    """Convert raw parsed sections into render-ready dicts with HTML body and heading level."""
    return [{
        'number': s['number'],
        'title': s['title'],
        'level': s['number'].count('.') + 2,
        'body_html': format_body(s['body'])
    } for s in secs]


def _render_toc(nodes, out, indent):
    pad = '  ' * indent
    for node in nodes:
        num = node['entry']['number']
        node_title = html.escape(node['entry']['title'])
        out.write(f'{pad}<li id="toc{num}">\n')
        out.write(f'{pad}  {num}\n')
        if node['exists']:
            out.write(f'{pad}  <a href="#s{num}">{node_title}</a>\n')
        else:
            out.write(f'{pad}  {node_title}\n')
        if node['children']:
            out.write(f'{pad}  <ol>\n')
            _render_toc(node['children'], out, indent + 2)
            out.write(f'{pad}  </ol>\n')
        out.write(f'{pad}</li>\n')


def render_html(title, license_lines, toc_tree, sections):
    """Render the full HTML document for a single protocol version."""
    out = io.StringIO()
    t_esc = html.escape(title)
    out.write('<!DOCTYPE html>\n')
    out.write('<html>\n')
    out.write('<head>\n')
    out.write('  <meta charset="utf-8">\n')
    out.write(f'  <title>{t_esc}</title>\n')
    out.write('  <style>\n')
    out.write('    nav ol { margin: 0; padding: 0; padding-left: 1em; }\n')
    out.write('    nav li { list-style: none; }\n')
    out.write('    nav.top ul { margin: 0; padding: 0; background: #eee; color: black; }\n')
    out.write('    nav.top ul li { display: inline-block; }\n')
    out.write('  </style>\n')
    out.write('</head>\n')
    out.write('<body>\n')
    for line in license_lines:
        out.write(f'  <!-- {html.escape(line)} -->\n')
    out.write(f'  <h1>{t_esc}</h1>\n')
    out.write('  <h2>Table of Contents</h2>\n')
    out.write('  <nav>\n')
    out.write('    <ol>\n')
    _render_toc(toc_tree, out, 3)
    out.write('    </ol>\n')
    out.write('  </nav>\n')
    for sec in sections:
        lvl = sec['level']
        num = sec['number']
        sec_title = html.escape(sec['title'])
        out.write(f'  <h{lvl} id="s{num}">{num} {sec_title}</h{lvl}>\n')
        out.write(f'  {sec["body_html"]}\n')
    out.write('</body>\n')
    out.write('</html>\n')
    return out.getvalue()


def main():  # pylint: disable=too-many-locals
    """CLI entrypoint: render one HTML page per spec file plus an asciidoc summary."""
    parser = argparse.ArgumentParser(
        description="Generate native-protocol HTML and asciidoc summary from .spec files."
    )
    parser.add_argument(
        '--spec-dir', type=Path, default=Path('.'),
        help="Directory containing native_protocol_v*.spec files (default: cwd)."
    )
    parser.add_argument(
        '--attach-dir', type=Path, default=Path('modules/cassandra/attachments'),
        help="Output directory for per-version HTML files."
    )
    parser.add_argument(
        '--summary-adoc', type=Path,
        default=Path('modules/cassandra/pages/reference/native-protocol.adoc'),
        help="Output path for the generated asciidoc summary."
    )
    args = parser.parse_args()

    spec_dir = args.spec_dir
    attach_dir = args.attach_dir
    summary_adoc = args.summary_adoc

    if not spec_dir.is_dir():
        sys.exit(f"Spec directory does not exist: {spec_dir.resolve()}")

    attach_dir.mkdir(parents=True, exist_ok=True)
    summary_adoc.parent.mkdir(parents=True, exist_ok=True)

    specs = sorted(
        (p for p in spec_dir.glob('native_protocol_v*.spec')
         if _protocol_filename_re.match(p.name)),
        key=lambda p: int(_protocol_filename_re.match(p.name).group(1)),
        reverse=True,
    )
    if not specs:
        sys.exit(f"No native_protocol_v*.spec files found in {spec_dir.resolve()}")

    for sp in specs:
        version = _protocol_filename_re.match(sp.name).group(1)
        hp = attach_dir / f'native_protocol_v{version}.html'
        doc = parse_spec_file(sp)
        toc_tree = build_toc_tree(doc['toc'], {s['number'] for s in doc['sections']})
        sections = build_sections(doc['sections'])
        rendered = render_html(doc['title'], doc['license'], toc_tree, sections)
        hp.write_text(rendered, encoding='utf-8')
        print(f"-> {hp}")

    nav_js = """[source, js]
++++
<script>
    function setNavigation() {
        var containers = document.querySelectorAll('.sect1');

        containers.forEach(function (container) {
            var preElements = container.querySelectorAll('pre');
            preElements.forEach(function(preElement) {
                if (!preElement.textContent.trim()) {
                    preElement.remove();
                }
            });
            var h1Elements = container.querySelectorAll('h1');
            h1Elements.forEach(function(h1Element) {
                h1Element.remove();
            });

            var navLinks = container.querySelectorAll('nav a, pre a');

            navLinks.forEach(function (link) {
                link.addEventListener('click', function (event) {

                    event.preventDefault();
                    var section = link.getAttribute('href').replace("#", '');

                    var targetSection = container.querySelector('h2[id="' + section + '"]')
                        || container.querySelector('h3[id="' + section + '"]')
                        || container.querySelector('h4[id="' + section + '"]')
                        || container.querySelector('h5[id="' + section + '"]');

                    if (targetSection) {
                        targetSection.scrollIntoView({ behavior: 'smooth' });
                    }
                });
            });
        });
    }

    window.onload = function() {
        setNavigation()
    }
</script>
++++
"""

    html_files = sorted(
        (p for p in attach_dir.glob('native_protocol_v*.html')
         if _protocol_filename_re.match(p.name)),
        key=lambda p: int(_protocol_filename_re.match(p.name).group(1)),
        reverse=True,
    )
    with summary_adoc.open('w', encoding='utf-8') as f:
        f.write("= Native Protocol Versions\n")
        f.write(":page-layout: default\n\n")
        for file in html_files:
            ver = _protocol_filename_re.match(file.name).group(1)
            f.write(f"== Native Protocol Version {ver}\n\n")
            f.write("[source, html]\n++++\n")
            f.write(f"include::cassandra:attachment${file.name}[Version {ver}]\n")
            f.write("++++\n\n")
        f.write(nav_js)
    print(f"-> {summary_adoc}")


if __name__ == '__main__':
    main()
