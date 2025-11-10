#!/usr/bin/env python3
import sys
from collections import Counter

counts = Counter()
for line in sys.stdin:
    try:
        w, c = line.rstrip("\n").split("\t", 1)
        counts[w] += int(c)
    except:
        continue

print("Word Frequencies (Top 20):")
for w, n in counts.most_common(20):
    print(f"{w}: {n}")

print("\nStatistics:")
print(f"Total words: {sum(counts.values())}")
print(f"Unique words: {len(counts)}")
