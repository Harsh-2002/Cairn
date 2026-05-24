---
title: "Hello, world"
date: 2026-05-18T12:00:00Z
slug: hello-world
tags: [meta, getting-started]
summary: The obligatory first post.
---

This blog is built with [Cairn](https://github.com/anthropics/cairn).

# What you're looking at

A single Rust binary turned a directory of markdown into a static site. No CMS, no database, no application server.

Everything that's *authoritative* — the post you're reading, the templates, the configuration — lives in a git repository. Everything that's *derived* — the HTML, the responsive image variants, the feed — lives behind a CDN with no compute.

# How to read along

The reference blog under `examples/blog/` exists so the build pipeline has something to build. Replace these files with your own and you're done.
