# Vendored browser libraries

Files here are copied into the repo on purpose, not loaded from a CDN: the
review UI must work on a machine without internet, the version must not
change under us, and the checksum below is what was reviewed.

| File | Library | Version | Released | Source | SHA-256 |
|---|---|---|---|---|---|
| `purify.min.js` | DOMPurify (CHG-002, HTML display sanitiser) | 3.4.17 | 2026-10-12 | npm registry tarball `dompurify-3.4.17.tgz`, integrity `sha512-B/pSlYVHQ58kqISD/yDjuJ1yfzI34m1gF4lAod2bwSRhiN4K5Cl3H8Af7hqB2PmTL911tdEudnnXn5FnhRoz0w==` verified before extraction | `cd37898f9782adf54820c413191b5691803f0f137c799d6edcfd38523b4c2c31` |

`DOMPURIFY-LICENSE` is the library's licence file (MPL-2.0 OR Apache-2.0).

## Updating

1. Pick a release at least two weeks old from
   `https://registry.npmjs.org/dompurify`.
2. Download the tarball named in `dist.tarball`, check it against
   `dist.integrity`, and extract `package/dist/purify.min.js` and
   `package/LICENSE`.
3. Replace the two files, update the row above with the new version and
   SHA-256, and bump the `?v=` query on the script tag in `web/index.html`.
4. Run `tests/test_html_display.py`, which checks the SHA-256 here against
   the file on disk.
