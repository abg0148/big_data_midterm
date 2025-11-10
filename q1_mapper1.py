#!/usr/bin/env python3
import sys, re, os

stop_words = {
"the", "a", "an", "is", "are"
}

def tokenizer(stream):
    # gets rid of punctuations, converts everything to lowercase and gets rid of stopwords
    # then yields it
    TOKEN_RE = re.compile(r'[a-z0-9]+')
    for line in stream:
        toks = TOKEN_RE.findall(line.lower())
        toks_filtered = [tk for tk in toks if tk not in stop_words]
        if toks_filtered:
            yield toks_filtered

def main(sep='\t'):
    for toks in tokenizer(sys.stdin):
        for word in toks:
            # KEY = word, VALUE = 1
            print(f"{word}{sep}1")

if __name__ == "__main__":
    main()