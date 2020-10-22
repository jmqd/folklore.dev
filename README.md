# folklore.dev

A search engine, but only from trusted engineering resources. (Blogs, papers,
git commits, talks, etc.)

## Why?

Google kind of sucks. Increasingly, I use `site: example.com` in my queries,
because I get better results that way. So I've decided to build my own little
search engine that only searches sites that I trust and have a desire to read.

What you read is important. I don't want to read crap. Small blogs written by
smart people have lost discoverability lately. They don't make money, so there's
no incentive for firms to show them to you. This is very bad.

I want to build a domain-specific search engine based on a trusted allow list of
websites. Search results that you want to consume exhaustively to the last page,
not just the first.

## TODO

1. Add selected git repos as sources for indexing.
2. Wrap this in an HTTP server, then deploy it to https://folklore.dev
3. Support exact text matches for ngrams greater than trigrams.
4. Explore real indexing algorithms and data structures, instead of my
   hacked-together stuff. (Patricia tree might be good.)

## Future roadmap items?

1. Make indexing be an online thing, not just 'statically' at deployment.
2. Add code sources?
3. Support more advanced queries.
