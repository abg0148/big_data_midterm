#!/usr/bin/env python3
import sys

SEP = "\t"
current_key = None
total = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    parts = line.split(SEP, 1)   # tolerate extra tabs in value
    if len(parts) != 2:
        continue
    key, val = parts
    try:
        n = int(val)
    except ValueError:
        continue

    if key != current_key and current_key is not None:
        sys.stdout.write(f"{current_key}{SEP}{total}\n")
        total = 0
    current_key = key
    total += n

if current_key is not None:
    sys.stdout.write(f"{current_key}{SEP}{total}\n")