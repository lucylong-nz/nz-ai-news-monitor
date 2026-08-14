"""
news_fetcher.py

Fetch relevant New Zealand AI industry news from NewsAPI and NewsData.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from config import (
    NEWSAPI_API_KEY,
    NEWSDATA_API_KEY,
    LOOKBACK_DAYS,
    NEWSAPI_QUERIES,
    NEWSDATA_QUERIES,
)

from utils import (
    make_article_key,
    current_batch_id,
    current_timestamp,
    normalize_url,
)

NEWSAPI_URL = "https://newsapi.org/v2/everything"
NEWSDATA_URL = "https://newsdata.io/api/1/latest"

# Post-fetch title filters for improving article relevance.
# Keywords within each pattern are OR conditions.
# Multiple patterns assigned to one query are AND conditions.
AI_TITLE_PATTERN = (
    r"generative ai|genai|\bllm\b|large language model|"
    r"artificial intelligence|\bai\b|chatbot|chatgpt|"
    r"copilot|gemini|claude|anthropic|openai|automation"
)

GLOBAL_VENDOR_PATTERN = (
    r"openai|microsoft|google|deepmind|anthropic|gemini|"
    r"copilot|chatgpt|claude|databricks|snowflake|\baws\b|"
    r"amazon web services|azure|langchain|llamaindex|n8n|"
    r"dspy|hugging face|power bi"
)

NZ_GEO_PATTERN = (
    r"new zealand|\bnz\b|auckland|wellington|"
    r"christchurch|kiwi"
)

LOCAL_VENDOR_PATTERN = (
    r"datacom|cdc data centres|theta|soul machines|"
    r"straker|xero|callaghan innovation|ai forum|"
    r"spark|one nz|datagrid|heartlab"
)

WORKFORCE_PATTERN = (
    r"jobs?|workforce|redundant|job cuts|reskilling|"
    r"training|survey|sentiment|trust|adoption|ethics|"
    r"wellbeing|mental health|skills|regulation|privacy|"
    r"governance|policy"
)

ADOPTION_PATTERN = (
    r"adoption|deployment|implementation|partnership|"
    r"investment|productivity|healthcare|agriculture|"
    r"banking|education|government|manufacturing|"
    r"retail|logistics"
)


# Title filters by query type.
# Rules are independent; only the matching query rule is applied.
# Within a pattern = OR; between patterns = AND.
TITLE_RULES = {
    "nz_ai_core": (
        AI_TITLE_PATTERN,
    ),
    "nz_genai_core": (
        AI_TITLE_PATTERN,
    ),
    "nz_ai_global_vendors": (
        GLOBAL_VENDOR_PATTERN,
        NZ_GEO_PATTERN,
    ),
    "nz_local_vendors": (
        LOCAL_VENDOR_PATTERN,
    ),
    "nz_cloud_vendors": (
        GLOBAL_VENDOR_PATTERN,
        NZ_GEO_PATTERN,
    ),
    "nz_ai_adoption_use_cases": (
        AI_TITLE_PATTERN,
        ADOPTION_PATTERN,
    ),
    "nz_ai_workforce_policy": (
        AI_TITLE_PATTERN,
        WORKFORCE_PATTERN,
    ),
}

NEGATIVE_TITLE_PATTERN = (
    r"market close|sharemarket|share market|"
    r"rugby|cricket|football|warriors v|fifa|"
    r"horoscope|movie review|film review|"
    r"letters:"
)

def safe_text(value):
    if value is None:
        return None
    if isinstance(value, list):
        value = ", ".join(str(item) for item in value)
    value = str(value).strip()
    return value or None


def request_with_retry(url, params, max_retries=3, sleep_seconds=2):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                return response
            print(
                f"[WARN] HTTP {response.status_code} | "
                f"attempt={attempt} | query={params.get('q')}"
            )
            try:
                print(f"[WARN] Response: {response.json()}")
            except Exception:
                print(f"[WARN] Response: {response.text[:500]}")
        except requests.RequestException as error:
            print(f"[WARN] Request failed on attempt {attempt}: {error}")

        if attempt < max_retries:
            time.sleep(sleep_seconds * attempt)

    return None


class NewsFetcher:
    def __init__(
        self,
        from_date=None,
        to_date=None,
        batch_prefix="INITIAL",
    ):
        self.batch_id = current_batch_id(batch_prefix)
        self.ingested_at = current_timestamp()
        self.today_utc = datetime.now(timezone.utc)

        if to_date is None:
            to_date = self.today_utc.date()

        if from_date is None:
            from_date = to_date - timedelta(days=LOOKBACK_DAYS)

        self.from_date = self._normalize_date(from_date)
        self.to_date = self._normalize_date(to_date)

        if self.from_date > self.to_date:
            raise ValueError(
                f"from_date {self.from_date} cannot be later "
                f"than to_date {self.to_date}"
            )

    @staticmethod
    def _normalize_date(value):
        """
        Convert a date, datetime or ISO date string to YYYY-MM-DD.
        """

        if isinstance(value, datetime):
            return value.date().isoformat()

        if hasattr(value, "isoformat") and not isinstance(value, str):
            return value.isoformat()

        if isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            return parsed.date().isoformat()

        raise TypeError(
            "Date value must be a date, datetime, or ISO date string."
        )

    def fetch_newsapi(self):
        rows = []

        for query in NEWSAPI_QUERIES:
            query_name = query["query_name"]

            for page in range(1, 3):
                params = {
                    "q": query["query_text"],
                    "from": self.from_date,
                    "to": self.to_date,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 100,
                    "page": page,
                    "apiKey": NEWSAPI_API_KEY,
                }

                if query.get("domains"):
                    params["domains"] = query["domains"]
                elif query.get("exclude_domains"):
                    params["excludeDomains"] = query["exclude_domains"]

                response = request_with_retry(NEWSAPI_URL, params)

                if response is None:
                    break

                payload = response.json()
                articles = payload.get("articles", [])

                print(
                    f"[INFO] NewsAPI | {query_name} | "
                    f"page={page} | rows={len(articles)}"
                )

                if not articles:
                    break

                for article in articles:
                    rows.append(
                        self._newsapi_row(
                            article=article,
                            query_name=query_name,
                            query_text=query["query_text"],
                        )
                    )

                if len(articles) < 100:
                    break

                time.sleep(1)

        return pd.DataFrame(rows)

    def fetch_newsdata(self):
        rows = []

        for query in NEWSDATA_QUERIES:
            query_name = query["query_name"]
            next_page = None
            page_counter = 0

            while page_counter < 2:
                params = {
                    "apikey": NEWSDATA_API_KEY,
                    "q": query["q"],
                    "country": query["country"],
                    "category": query["category"],
                    "language": query["language"],
                }

                if next_page:
                    params["page"] = next_page

                response = request_with_retry(NEWSDATA_URL, params)

                if response is None:
                    break

                payload = response.json()

                if payload.get("status") not in ("success", "ok"):
                    print(
                        f"[WARN] NewsData failed | "
                        f"{query_name} | {payload}"
                    )
                    break

                articles = payload.get("results", [])

                print(
                    f"[INFO] NewsData | {query_name} | "
                    f"page={page_counter + 1} | rows={len(articles)}"
                )

                if not articles:
                    break

                for article in articles:
                    rows.append(
                        self._newsdata_row(
                            article=article,
                            query_name=query_name,
                            query_text=query["q"],
                        )
                    )

                next_page = payload.get("nextPage")
                page_counter += 1

                if not next_page:
                    break

                time.sleep(1)

        return pd.DataFrame(rows)

    def fetch_all(self):
        newsapi_df = self.fetch_newsapi()
        newsdata_df = self.fetch_newsdata()

        available_frames = [
            frame
            for frame in (newsapi_df, newsdata_df)
            if not frame.empty
        ]

        if not available_frames:
            return pd.DataFrame()

        df = pd.concat(available_frames, ignore_index=True)

        print(f"[INFO] Candidate articles before filtering: {len(df)}")

        df = self._filter_date_range(df)
        df = self._filter_title_relevance(df)
        df = self._drop_noise_titles(df)
        df = self._deduplicate_articles(df)

        df = df.reset_index(drop=True)

        print(f"[INFO] Relevant unique articles: {len(df)}")

        return df

# Standardise fields from NewsAPI and NewsData into a common schema
# for consistent downstream processing and storage.
    def _newsapi_row(self, article, query_name, query_text):
        source = article.get("source") or {}
        title = safe_text(article.get("title"))
        source_name = safe_text(source.get("name"))
        published_at = safe_text(article.get("publishedAt"))
        article_url = safe_text(article.get("url"))

        return {
            "article_key": make_article_key(
                article_url,
                title,
                source_name,
                published_at,
            ),
            "provider": "newsapi",
            "query_name": query_name,
            "query_text": query_text,
            "source_name": source_name,
            "source_id": safe_text(source.get("id")),
            "author": safe_text(article.get("author")),
            "title": title,
            "description": safe_text(article.get("description")),
            "article_url": article_url,
            "image_url": safe_text(article.get("urlToImage")),
            "published_at": published_at,
            "language": "en",
            "country": "new zealand",
            "category": None,
            "batch_id": self.batch_id,
            "ingested_at": self.ingested_at,
            "raw_json": json.dumps(article, ensure_ascii=False),
        }

    def _newsdata_row(self, article, query_name, query_text):
        title = safe_text(article.get("title"))
        source_name = safe_text(
            article.get("source_name") or article.get("source_id")
        )
        published_at = safe_text(article.get("pubDate"))
        article_url = safe_text(article.get("link"))

        return {
            "article_key": make_article_key(
                article_url,
                title,
                source_name,
                published_at,
            ),
            "provider": "newsdata",
            "query_name": query_name,
            "query_text": query_text,
            "source_name": source_name,
            "source_id": safe_text(article.get("source_id")),
            "author": safe_text(article.get("creator")),
            "title": title,
            "description": safe_text(article.get("description")),
            "article_url": article_url,
            "image_url": safe_text(article.get("image_url")),
            "published_at": published_at,
            "language": safe_text(article.get("language")) or "en",
            "country": safe_text(article.get("country")) or "new zealand",
            "category": safe_text(article.get("category")),
            "batch_id": self.batch_id,
            "ingested_at": self.ingested_at,
            "raw_json": json.dumps(article, ensure_ascii=False),
        }

# Filter articles to the required date range.
# Standardise published time to UTC and remove articles older than from_date.
    def _filter_date_range(self, df):
        df = df.copy()

        df["published_at_ts"] = pd.to_datetime(
            df["published_at"],
            errors="coerce",
            utc=True,
        )

        minimum_date = pd.Timestamp(self.from_date, tz="UTC")

        return df[
            df["published_at_ts"].isna()
            | (df["published_at_ts"] >= minimum_date)
        ].copy()

    def _filter_title_relevance(self, df):
        df = df.copy()
        keep_mask = pd.Series(True, index=df.index)
        titles = df["title"].fillna("")

        for query_name, required_patterns in TITLE_RULES.items():
            query_rows = df["query_name"] == query_name

            if not query_rows.any():
                continue

            matches = pd.Series(True, index=df.index)

            for pattern in required_patterns:
                matches = matches & titles.str.contains(
                    pattern,
                    case=False,
                    regex=True,
                    na=False,
                )

            keep_mask = keep_mask & (~query_rows | matches)

        removed = int((~keep_mask).sum())

        print(f"[INFO] Title relevance filter removed {removed} articles")

        return df[keep_mask].copy()

    def _drop_noise_titles(self, df):
        df = df.copy()

        noise_mask = df["title"].fillna("").str.contains(
            NEGATIVE_TITLE_PATTERN,
            case=False,
            regex=True,
            na=False,
        )

        removed = int(noise_mask.sum())

        print(f"[INFO] Noise-title filter removed {removed} articles")

        return df[~noise_mask].copy()

    def _deduplicate_articles(self, df):
        df = df.copy()

        #Create a normalized URL for deduplication
        df["normalized_url"] = df["article_url"].apply(normalize_url)
        
        #Create a normalized title
        df["normalized_title"] = (
            df["title"]
            .fillna("")
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        #Create a simplified story title for deduplication
        df["story_title"] = (
            df["normalized_title"]
            .str.replace(r"[^a-z0-9\s]", " ", regex=True)
            .str.replace(
                r"\b(reportedly|report|says|said|almost|nearly|model|tool)\b",
                " ",
                regex=True,
            )
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        # Deduplicate by story_title
        df = df.drop_duplicates(
            subset=["story_title"],
            keep="first",
        )

        # Sort remaining articles
        df = df.sort_values(
            ["published_at", "provider"],
            ascending=[False, True],
        )

        # Separate articles with/without URLs
        has_url = df["normalized_url"].fillna("") != ""

        # Articles WITH URL → deduplicate by URL
        with_url = df[has_url].drop_duplicates(
            subset=["normalized_url"],
            keep="first",
        )

        #Articles WITHOUT URL → deduplicate by article_key
        without_url = df[~has_url].drop_duplicates(
            subset=["article_key"],
            keep="first",
        )

        # Put them back together
        df = pd.concat(
            [with_url, without_url],
            ignore_index=True,
        )

        df = df.drop_duplicates(
            subset=["normalized_title"],
            keep="first",
        )

        return df.drop(
            columns=[
                "normalized_url",
                "normalized_title",
                "story_title",
                "published_at_ts",
            ],
            errors="ignore",
        )
        