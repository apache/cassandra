#!/usr/bin/env python3
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
"""
Git Heatmap — visualize code change frequency and recency.

Heat formula:
  decay(days)        = 2^(-days / HALF_LIFE_DAYS)          # half-life = 60 days
  size_bonus(lines)  = 1 + 1/(1 + lines/SMALL_CHANGE_SCALE) # small edits → bonus up to 2x
  commit_heat(d, l)  = decay(d) * size_bonus(l)

Repo-level:  file_heat = Σ commit_heat(days_ago, added+deleted) per commit
Line-level:  line_heat = Σ commit_heat(days_ago, churn) per commit that touched that line,
             with line positions tracked through diffs via VersionMap.
"""

import argparse
import colorsys
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

HALF_LIFE_DAYS = 60
SMALL_CHANGE_SCALE = 10

# Extensions excluded from repo-level scans by default (non-code files).
DEFAULT_EXCLUDED_EXTENSIONS = frozenset({
    '.txt', '.md', '.rst', '.adoc',
    '.json', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf', '.properties',
    '.xml', '.csv', '.tsv',
    '.lock', '.sum',
    '.png', '.jpg', '.jpeg', '.gif', '.ico', '.svg', '.webp',
    '.pdf', '.doc', '.docx',
    '.zip', '.tar', '.gz', '.bz2', '.xz',
    '.woff', '.woff2', '.ttf', '.eot',
    '.min.js', '.min.css',
    '.map',
    '.license', '.licence',
})

# ═══════════════════════════════════════════════════════════════════════════════
# Core heat functions
# ═══════════════════════════════════════════════════════════════════════════════

def decay(days_ago: float) -> float:
    """Exponential decay: 1.0 at day 0, 0.5 at HALF_LIFE_DAYS."""
    return 2.0 ** (-days_ago / HALF_LIFE_DAYS)


def size_bonus(lines_changed: int) -> float:
    """Bonus multiplier for small changes.

    Returns value in [1.0, 2.0):
      1 line   → ~1.91
      10 lines → 1.50
      100 lines→ ~1.09
    """
    if lines_changed <= 0:
        return 1.0
    return 1.0 + 1.0 / (1.0 + lines_changed / SMALL_CHANGE_SCALE)


def commit_heat(days_ago: float, lines_changed: int) -> float:
    """Heat contributed by one commit = decay × size_bonus."""
    return decay(days_ago) * size_bonus(lines_changed)


# ═══════════════════════════════════════════════════════════════════════════════
# VersionMap — maps line numbers from old file version to new file version
# ═══════════════════════════════════════════════════════════════════════════════

class VersionMap:
    """Maps line numbers from parent (old) to child (new) file version.

    Built from unified-diff hunk headers.  For each hunk @@ -a,b +c,d @@:
      • min(b,d) old lines map 1:1 to new lines (modification)
      • extra old lines (b>d) are marked deleted  → map_line returns -1
      • extra new lines (d>b) are pure insertions  → no old line maps to them
      • lines outside hunks shift by the cumulative delta
    """

    def __init__(self, hunks: List[Tuple[int, int, int, int]]):
        self.regions: List[Tuple[str, int, float, int]] = []
        cum_delta = 0
        prev_old_end = 0

        for os_, oc, ns, nc in hunks:
            if oc > 0:
                # Unchanged lines before this hunk
                if os_ > prev_old_end + 1:
                    self.regions.append(('shift', prev_old_end + 1, os_ - 1, cum_delta))
                # Modified lines (1:1 mapping)
                min_c = min(oc, nc)
                if min_c > 0:
                    self.regions.append(('modify', os_, os_ + min_c - 1, cum_delta))
                # Deleted old lines
                if oc > nc:
                    self.regions.append(('deleted', os_ + nc, os_ + oc - 1, 0))
                prev_old_end = os_ + oc - 1
            else:
                # Pure insertion: os_ is the line *before* the insertion point
                if os_ >= prev_old_end + 1:
                    self.regions.append(('shift', prev_old_end + 1, os_, cum_delta))
                    prev_old_end = os_
            cum_delta += nc - oc

        # Trailing region
        self.regions.append(('shift', prev_old_end + 1, float('inf'), cum_delta))

    def map_line(self, old_line: int) -> int:
        """Map old line → new line.  Returns -1 if deleted."""
        for kind, start, end, delta in self.regions:
            if start <= old_line <= end:
                return -1 if kind == 'deleted' else old_line + delta
        return old_line


def map_through_versions(line: int, version_maps: List[VersionMap]) -> int:
    """Chain multiple VersionMaps.  Returns -1 if deleted at any step."""
    for vm in version_maps:
        line = vm.map_line(line)
        if line == -1:
            return -1
    return line


# ═══════════════════════════════════════════════════════════════════════════════
# Git helpers
# ═══════════════════════════════════════════════════════════════════════════════

def git(repo_path: str, *args) -> str:
    result = subprocess.run(
        ['git', '-C', repo_path] + list(args),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git failed: {' '.join(args)}\n{result.stderr[:500]}")
    return result.stdout


HUNK_RE = re.compile(r'^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@')


def resolve_rename(filename: str) -> str:
    """Resolve git numstat rename notation  {old => new}."""
    m = re.search(r'\{([^}]*) => ([^}]*)\}', filename)
    if m:
        return filename[:m.start()] + m.group(2) + filename[m.end():]
    return filename


# ═══════════════════════════════════════════════════════════════════════════════
# Repo-level parsing
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class FileCommit:
    hash: str
    timestamp: int
    added: int
    deleted: int


def parse_repo_log(repo_path: str, since: Optional[str] = None) -> Dict[str, List[FileCommit]]:
    """Return {filename: [FileCommit, ...]} from git log --numstat."""
    args = ['log', '--format=commit %H %at', '--numstat', '-m', '--first-parent']
    if since:
        args.append(f'--since={since}')
    output = git(repo_path, *args)

    files: Dict[str, List[FileCommit]] = {}
    cur_hash = cur_ts = None

    for line in output.splitlines():
        if line.startswith('commit '):
            parts = line.split()
            cur_hash, cur_ts = parts[1], int(parts[2])
        elif line and cur_hash and '\t' in line:
            parts = line.split('\t')
            if len(parts) < 3 or parts[0] == '-':
                continue
            added, deleted = int(parts[0]), int(parts[1])
            fname = resolve_rename(parts[2])
            files.setdefault(fname, []).append(
                FileCommit(cur_hash, cur_ts, added, deleted))
    return files


# ═══════════════════════════════════════════════════════════════════════════════
# File-level (line) parsing
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class FileDiffCommit:
    hash: str
    timestamp: int
    hunks: List[Tuple[int, int, int, int]]
    added_lines: List[int]   # new-side line numbers with '+' prefix
    total_churn: int         # added + deleted line count


def parse_file_log(repo_path: str, file_path: str,
                   since: Optional[str] = None) -> List[FileDiffCommit]:
    """Parse git log -p -U0 for one file.  Returns list newest-first."""
    args = ['log', '--format=commit %H %at', '-p', '-U0',
            '--follow', '-m', '--first-parent']
    if since:
        args.append(f'--since={since}')
    args += ['--', file_path]
    output = git(repo_path, *args)

    commits: List[FileDiffCommit] = []
    cur_hash = cur_ts = None
    hunks: List[Tuple[int,int,int,int]] = []
    added_lines: List[int] = []
    add_cnt = del_cnt = 0
    new_counter = 0
    in_hunk = False

    def flush():
        nonlocal hunks, added_lines, add_cnt, del_cnt, in_hunk
        if cur_hash and (hunks or added_lines):
            commits.append(FileDiffCommit(
                cur_hash, cur_ts, list(hunks), list(added_lines),
                add_cnt + del_cnt))
        hunks, added_lines = [], []
        add_cnt = del_cnt = 0
        in_hunk = False

    for raw_line in output.splitlines():
        if raw_line.startswith('commit '):
            flush()
            parts = raw_line.split()
            if len(parts) >= 3:
                cur_hash, cur_ts = parts[1], int(parts[2])
        elif raw_line.startswith('@@'):
            m = HUNK_RE.match(raw_line)
            if m:
                os_ = int(m.group(1))
                oc  = int(m.group(2)) if m.group(2) else 1
                ns  = int(m.group(3))
                nc  = int(m.group(4)) if m.group(4) else 1
                hunks.append((os_, oc, ns, nc))
                new_counter = ns
                in_hunk = True
        elif in_hunk:
            if raw_line.startswith('+'):
                added_lines.append(new_counter)
                new_counter += 1
                add_cnt += 1
            elif raw_line.startswith('-'):
                del_cnt += 1
            elif raw_line.startswith('\\'):
                pass
            else:
                new_counter += 1  # context (shouldn't appear with -U0)
    flush()
    return commits


# ═══════════════════════════════════════════════════════════════════════════════
# Heat computation
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class FileHeat:
    path: str
    heat: float
    last_modified_days: float
    num_commits: int


def _normalise_dir(d: str) -> str:
    """Normalise a directory path for prefix matching: strip ./ and ensure trailing /."""
    d = d.replace('\\', '/')
    d = d.lstrip('./')
    if d and not d.endswith('/'):
        d += '/'
    return d


def _is_dir_pattern(pattern: str) -> bool:
    """Heuristic: patterns containing '/' are directory prefixes, not extensions."""
    return '/' in pattern


def _should_exclude(fpath: str, excluded_exts: frozenset,
                    excluded_dirs: Tuple[str, ...] = ()) -> bool:
    """Check whether a file path matches any excluded extension or directory."""
    # Directory prefixes
    for dp in excluded_dirs:
        if fpath.startswith(dp):
            return True
    # Multi-part extensions (e.g. .min.js)
    lower = fpath.lower()
    for ext in excluded_exts:
        if ext.count('.') > 1 and lower.endswith(ext):
            return True
    # Single-part extension
    _, ext = os.path.splitext(lower)
    # Extensionless basename match (e.g. LICENSE → .license)
    basename = os.path.basename(lower)
    return ext in excluded_exts or ('.' + basename) in excluded_exts


def compute_repo_heat(repo_path: str, *, now: Optional[float] = None,
                      since: Optional[str] = None, top: Optional[int] = None,
                      existing_only: bool = True,
                      directory: Optional[str] = None,
                      exclude: Optional[List[str]] = None,
                      no_default_exclude: bool = False) -> List[FileHeat]:
    if now is None:
        now = time.time()
    file_commits = parse_repo_log(repo_path, since=since)

    current_files = None
    if existing_only:
        current_files = set(git(repo_path, 'ls-files').splitlines())

    # Build the set of excluded extensions
    if no_default_exclude:
        excluded_exts: frozenset = frozenset()
    else:
        excluded_exts = DEFAULT_EXCLUDED_EXTENSIONS

    # Split --exclude items into directory prefixes vs. extensions
    excluded_dirs: List[str] = []
    if exclude:
        extra_exts: List[str] = []
        for item in exclude:
            if _is_dir_pattern(item):
                excluded_dirs.append(_normalise_dir(item))
            else:
                extra_exts.append(item if item.startswith('.') else f'.{item}')
        if extra_exts:
            excluded_exts = excluded_exts | frozenset(extra_exts)
    excluded_dirs_t = tuple(excluded_dirs)

    # Normalise --dir prefix
    dir_prefix: Optional[str] = None
    if directory:
        dp = _normalise_dir(directory)
        dir_prefix = dp if dp else None

    results = []
    for fpath, commits in file_commits.items():
        if current_files is not None and fpath not in current_files:
            continue
        if dir_prefix is not None and not fpath.startswith(dir_prefix):
            continue
        if _should_exclude(fpath, excluded_exts, excluded_dirs_t):
            continue
        heat = 0.0
        last_ts = 0
        for fc in commits:
            days = max(0.0, (now - fc.timestamp) / 86400.0)
            heat += commit_heat(days, fc.added + fc.deleted)
            last_ts = max(last_ts, fc.timestamp)
        results.append(FileHeat(
            fpath, heat,
            (now - last_ts) / 86400.0 if last_ts else float('inf'),
            len(commits)))

    results.sort(key=lambda r: r.heat, reverse=True)
    return results[:top] if top else results


@dataclass
class LineHeat:
    line_number: int
    heat: float
    content: str


def compute_line_heat(repo_path: str, file_path: str, *,
                      now: Optional[float] = None,
                      since: Optional[str] = None,
                      max_commits: int = 500) -> List[LineHeat]:
    if now is None:
        now = time.time()

    full = os.path.join(repo_path, file_path)
    with open(full, 'r', errors='replace') as f:
        lines = f.readlines()
    n = len(lines)
    if n == 0:
        return []

    diffs = parse_file_log(repo_path, file_path, since=since)
    if max_commits:
        diffs = diffs[:max_commits]

    heat = [0.0] * (n + 1)  # 1-indexed

    # version_maps[j] = VersionMap for diffs[j]  (maps V_{j+1} → V_j)
    # V_0 = HEAD
    version_maps: List[VersionMap] = []

    for i, cd in enumerate(diffs):
        days = max(0.0, (now - cd.timestamp) / 86400.0)
        h = commit_heat(days, cd.total_churn)

        for new_line in cd.added_lines:
            head_line = new_line
            # Map V_i → V_0 through version_maps[i-1] … version_maps[0]
            for j in range(i - 1, -1, -1):
                head_line = version_maps[j].map_line(head_line)
                if head_line == -1:
                    break
            if head_line != -1 and 1 <= head_line <= n:
                heat[head_line] += h

        version_maps.append(VersionMap(cd.hunks))

    return [LineHeat(i + 1, heat[i + 1], lines[i].rstrip('\r\n'))
            for i in range(n)]


# ═══════════════════════════════════════════════════════════════════════════════
# Output formatting
# ═══════════════════════════════════════════════════════════════════════════════

def heat_to_rgb(normalised: float) -> Tuple[int, int, int]:
    hue = (1.0 - normalised) * 0.667   # blue 240° → red 0°
    r, g, b = colorsys.hsv_to_rgb(hue, 0.8, 0.95)
    return int(r * 255), int(g * 255), int(b * 255)


def _bg(r, g, b):
    return f"\033[48;2;{r};{g};{b}m"

_RST = "\033[0m"


def format_repo_heat(results: List[FileHeat], color: bool = True) -> str:
    if not results:
        return "No files found."
    mx = max(r.heat for r in results)
    out = []
    out.append(f"{'#':>5}  {'Heat':>10}  {'Cmts':>6}  {'Last modified':>14}  File")
    out.append("─" * 90)
    for i, r in enumerate(results):
        norm = r.heat / mx if mx else 0
        hs = f"{r.heat:10.2f}"
        ds = (f"{r.last_modified_days:.0f}d ago" if r.last_modified_days < 365
              else f"{r.last_modified_days/365:.1f}y ago")
        if color:
            cr, cg, cb = heat_to_rgb(norm)
            hs = f"{_bg(cr,cg,cb)} {hs} {_RST}"
        out.append(f"{i+1:>5}  {hs}  {r.num_commits:>6}  {ds:>14}  {r.path}")
    return "\n".join(out)


def format_line_heat(results: List[LineHeat], color: bool = True,
                     show_all: bool = False, context: int = 2) -> str:
    """Format line-level heat.

    By default only lines with heat > 0 are shown, grouped into snippets
    with ``context`` surrounding lines and ``...`` separators.
    Pass ``show_all=True`` to display every line.
    """
    if not results:
        return "Empty file."
    mx = max(r.heat for r in results) or 1.0
    w = len(str(len(results)))

    def fmt(r: LineHeat) -> str:
        norm = r.heat / mx
        pct = norm * 100
        if color:
            cr, cg, cb = heat_to_rgb(norm)
            bar = f"{_bg(cr,cg,cb)} {pct:5.1f}% {_RST}"
        else:
            bar = f"{pct:6.1f}%"
        return f"{r.line_number:>{w}} {bar} {r.content}"

    if show_all:
        return "\n".join(fmt(r) for r in results)

    # Build set of line indices (0-based) to display: hot lines + context
    hot_indices = {i for i, r in enumerate(results) if r.heat > 0}
    if not hot_indices:
        return "No lines with heat > 0."

    visible: set = set()
    for i in hot_indices:
        for j in range(max(0, i - context), min(len(results), i + context + 1)):
            visible.add(j)

    out: list = []
    prev_i = -2  # sentinel
    for i in sorted(visible):
        if i > prev_i + 1:
            out.append(f"{'':>{w}}   {'...'}")
        out.append(fmt(results[i]))
        prev_i = i
    # trailing ellipsis if we cut off before end
    if max(visible) < len(results) - 1:
        out.append(f"{'':>{w}}   {'...'}")
    return "\n".join(out)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="Git Heatmap — code change frequency × recency",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  python heatmap.py repo --top 20 ./kafka
  python heatmap.py repo --dir src/main --top 20 ./kafka
  python heatmap.py repo --exclude xml,proto,src/generated/ ./kafka
  python heatmap.py repo --no-default-exclude ./kafka
  python heatmap.py file clients/src/main/java/org/apache/kafka/common/utils/Utils.java ./kafka
  python heatmap.py file --all --context 5 some/file.py .
""")
    sub = ap.add_subparsers(dest='cmd')

    rp = sub.add_parser('repo', help='file-level heatmap')
    rp.add_argument('repo_path', nargs='?', default='.')
    rp.add_argument('--top', type=int, default=30)
    rp.add_argument('--since', default='2 years ago')
    rp.add_argument('--dir', default=None,
                    help='limit scan to files under this directory (e.g. src/main)')
    rp.add_argument('--exclude', default=None,
                    help='comma-separated extensions or directory paths to exclude '
                         '(e.g. xml,proto,src/java/org/apache/cassandra/config/)')
    rp.add_argument('--no-default-exclude', action='store_true',
                    help='disable default exclusion of non-code files (txt, md, json, …)')
    rp.add_argument('--no-color', action='store_true')
    rp.add_argument('--json', action='store_true')

    fp = sub.add_parser('file', help='line-level heatmap')
    fp.add_argument('file_path')
    fp.add_argument('repo_path', nargs='?', default='.')
    fp.add_argument('--since', default='2 years ago')
    fp.add_argument('--max-commits', type=int, default=500)
    fp.add_argument('--all', action='store_true', dest='show_all',
                    help='show all lines including 0%% heat (default: snippets only)')
    fp.add_argument('--context', type=int, default=2,
                    help='lines of context around hot lines in snippet mode (default: 2)')
    fp.add_argument('--no-color', action='store_true')
    fp.add_argument('--json', action='store_true')

    args = ap.parse_args()
    if not args.cmd:
        ap.print_help()
        sys.exit(1)

    if args.cmd == 'repo':
        exclude_ext = args.exclude.split(',') if args.exclude else None
        res = compute_repo_heat(args.repo_path, since=args.since, top=args.top,
                                directory=args.dir,
                                exclude=exclude_ext,
                                no_default_exclude=args.no_default_exclude)
        if args.json:
            import json
            print(json.dumps([{'path': r.path, 'heat': r.heat,
                               'commits': r.num_commits,
                               'last_modified_days': round(r.last_modified_days, 1)}
                              for r in res], indent=2))
        else:
            print(format_repo_heat(res, color=not args.no_color))

    elif args.cmd == 'file':
        res = compute_line_heat(args.repo_path, args.file_path,
                                since=args.since, max_commits=args.max_commits)
        if args.json:
            import json
            print(json.dumps([{'line': r.line_number, 'heat': round(r.heat, 4),
                               'content': r.content} for r in res], indent=2))
        else:
            print(format_line_heat(res, color=not args.no_color,
                                   show_all=args.show_all,
                                   context=args.context))


if __name__ == '__main__':
    main()
