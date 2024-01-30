#!/usr/bin/env python3

import argparse

def parse_args():
  parser = argparse.ArgumentParser(description='Finds first line of diff for files')
  parser.add_argument('first', type=str, help='first file')
  parser.add_argument('second', type=str, help='second file')
  return parser.parse_args()

def pad_string(string, length, char):
    return string.rjust(length, char)

def find_first_diff(file1, file2):
    with open(file1, 'r') as f1, open(file2, 'r') as f2:
        last_match = None
        for i, (line1, line2) in enumerate(zip(f1, f2)):
            if line1 != line2:
                for j, (char1, char2) in enumerate(zip(line1, line2)):
                    if char1 != char2:
                        pointer = pad_string('^', j + 1, ' ')
                        return f"Line {i+1}, position {j+1}, is the first line where the two files differ.\nFirst: {line1}\nFirst: {pointer}\nSecond: {line2}\nSecond: {pointer}\nLast Match: {last_match}"
                return f"Line {i+1} is the first line where the two files differ."
            last_match = line1
    return "The two files are identical."

def main():
  args = parse_args()
  print(find_first_diff(args.first, args.second))

if __name__ == "__main__":
    main()
