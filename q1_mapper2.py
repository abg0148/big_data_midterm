#!/usr/bin/env python3
import sys

SEP = "\t"

def main():
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split(SEP, 1)  # tolerate values with tabs
        if len(parts) != 2:
            continue
        word, count = parts
        try:
            n = int(count)
        except ValueError:
            continue
        # Emit length with the *count* (token-weighted)
        print(f"{len(word)}{SEP}{n}")

if __name__ == "__main__":
    main()
