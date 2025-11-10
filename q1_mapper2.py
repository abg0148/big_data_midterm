#!/usr/bin/env python3
import sys, os

def main(sep='\t'):
    for line in sys.stdin:
        word, occ = line.split(sep)
        # KEY = len(word), VALUE = 1
        print(f"{len(word)}{sep}1")

if __name__ == "__main__":
    main()