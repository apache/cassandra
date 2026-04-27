#!/usr/bin/env python3
"""
Line-level delta debugging minimizer.

Given an input file and a test command, reduces the file to a minimal
set of lines that still causes the test command to exit with a non-zero
status (or match a specific pattern).

Based on Zeller's ddmin algorithm.

Usage:
    python shrink_text.py INPUT_FILE TEST_COMMAND [--pattern REGEX]

    INPUT_FILE:   the file to minimize
    TEST_COMMAND: command that returns 0 when the input "still fails"
                  (i.e., the bug is still present). The command receives
                  the candidate file path as its last argument.
    --pattern:    optional regex that must appear in the command's
                  stdout/stderr for the failure to count as "same bug"

Example:
    python shrink_text.py crash_input.sql "./check_bug.sh" --pattern "IndexOutOfBounds"
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile


def read_lines(path):
    with open(path, 'r') as f:
        return f.readlines()


def write_lines(path, lines):
    with open(path, 'w') as f:
        f.writelines(lines)


def is_interesting(lines, test_cmd, pattern, tmpdir):
    """Returns True if the reduced input still triggers the bug."""
    candidate = os.path.join(tmpdir, 'candidate')
    write_lines(candidate, lines)

    try:
        result = subprocess.run(
            test_cmd + [candidate],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return False

    if result.returncode == 0:
        return False

    if pattern:
        combined = result.stdout + result.stderr
        if not re.search(pattern, combined):
            return False

    return True


def ddmin(lines, test_fn):
    """Delta debugging minimization at line granularity."""
    n = 2
    while len(lines) >= 2:
        chunk_size = max(len(lines) // n, 1)
        some_progress = False

        for i in range(0, len(lines), chunk_size):
            complement = lines[:i] + lines[i + chunk_size:]
            if not complement:
                continue
            if test_fn(complement):
                lines = complement
                n = max(n - 1, 2)
                some_progress = True
                print(f"  reduced to {len(lines)} lines", file=sys.stderr)
                break

        if not some_progress:
            if n >= len(lines):
                break
            n = min(n * 2, len(lines))

    return lines


def main():
    parser = argparse.ArgumentParser(description='Line-level delta debugging minimizer')
    parser.add_argument('input_file', help='File to minimize')
    parser.add_argument('test_command', nargs='+', help='Command that returns 0 when bug is present')
    parser.add_argument('--pattern', help='Regex that must appear in output for same-bug check')
    args = parser.parse_args()

    original_lines = read_lines(args.input_file)
    print(f"Original: {len(original_lines)} lines", file=sys.stderr)

    pattern = re.compile(args.pattern) if args.pattern else None

    with tempfile.TemporaryDirectory() as tmpdir:
        def test_fn(lines):
            return is_interesting(lines, args.test_command, pattern, tmpdir)

        if not test_fn(original_lines):
            print("ERROR: original input does not trigger the bug with the given command",
                  file=sys.stderr)
            sys.exit(1)

        reduced = ddmin(original_lines, test_fn)

    output_path = args.input_file + '.reduced'
    write_lines(output_path, reduced)
    print(f"Reduced: {len(reduced)} lines (from {len(original_lines)})", file=sys.stderr)
    print(f"Written to: {output_path}", file=sys.stderr)


if __name__ == '__main__':
    main()
