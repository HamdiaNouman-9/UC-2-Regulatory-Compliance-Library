# utils/lang_detector.py

import re
from lingua import LanguageDetectorBuilder

# ================= Lingua Detector =================
_detector = (
    LanguageDetectorBuilder
    .from_all_languages()
    .with_minimum_relative_distance(0.1)
    .build()
)

# ================= Arabic Heuristic =================
ARABIC_REGEX = re.compile(r"[\u0600-\u06FF]")

def contains_arabic(text: str) -> bool:
    if not text:
        return False
    return bool(ARABIC_REGEX.search(text))


def detect_language(text: str) -> str:
    """
    Returns ISO-639-1 lowercase string:
    'en', 'ar', 'fr', etc.
    """

    if not text or len(text.strip()) < 20:
        return "unknown"

    # HARD override: any Arabic → Arabic
    if contains_arabic(text):
        return "ar"

    detected = _detector.detect_language_of(text)
    if detected is None or detected.iso_code_639_1 is None:
        return "unknown"

    return detected.iso_code_639_1.name.lower()
