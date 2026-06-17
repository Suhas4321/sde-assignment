from typing import List

HOT_KEYWORDS = [
    "confirmed",
    "booked",
    "scheduled",
    "appointment",
    "manager",
    "escalate",
    "complaint",
    "urgent",
    "demo",
    "meeting",
    "canceled",
    "refund",
]


def classify_priority(transcript_text: str, custom_keywords: List[str] = None) -> str:
    """
    Classify the priority lane of a call transcript based on keywords.
    Returns:
      "skip" if transcript is short (< 4 turns)
      "hot" if transcript contains hot keywords (urgent)
      "cold" otherwise (standard processing)
    """
    # Short transcripts skip LLM
    # Note: caller should check turns, but fallback check is here
    if not transcript_text:
        return "skip"

    keywords_to_check = custom_keywords if custom_keywords is not None else HOT_KEYWORDS

    # Lowercase transcript to match keywords case-insensitively
    text_lower = transcript_text.lower()
    for keyword in keywords_to_check:
        if keyword in text_lower:
            return "hot"

    return "cold"
