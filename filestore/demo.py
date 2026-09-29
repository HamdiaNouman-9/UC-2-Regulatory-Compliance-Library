"""N articles all link the same PDF, and show it is fetched once.

    python -m filestore.demo "<pdf url>" --articles 5
"""
import argparse
import logging

from dotenv import load_dotenv

from filestore import FileStore


def main():
    load_dotenv(override=True)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--articles", type=int, default=5)
    args = ap.parse_args()

    store = FileStore()
    try:
        for i in range(1, args.articles + 1):
            ref = store.fetch(args.url, f"demo-article-{i}")
            print(f"article {i}: source={ref.source:<12} new={ref.is_new!s:<5} sha={ref.sha256[:12]}")

        texts = store.get_texts_for_document("demo-article-1")
        if texts:
            t = texts[0]
            print(f"\ntext: method={t.method} pages={t.page_count} chars={len(t.text)}")
            print(t.text[:300])
    finally:
        store.close()


if __name__ == "__main__":
    main()
