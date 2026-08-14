"""
enrich_news_groq.py

Reads clean articles from the dbt intermediate model,
enriches them with Groq, and writes results to Snowflake.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from groq import Groq
from pydantic import BaseModel, Field, ValidationError
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "llama-3.3-70b-versatile",
)

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

DBT_SCHEMA = os.getenv("DBT_SCHEMA", "DBT_DEV")
SOURCE_MODEL = "INT_NEWS_READY_FOR_LLM"
TARGET_TABLE = "ARTICLE_ENRICHMENT"

PROMPT_VERSION = "v1"
SECONDS_BETWEEN_CALLS = 2.5
MAX_RETRIES = 3

if not GROQ_API_KEY:
    raise ValueError("Missing GROQ_API_KEY in .env")

# Defines and validates the expected LLM output: relevance, category, vendor, use case, sentiment, summary, etc.
class ArticleEnrichment(BaseModel):
    is_relevant: bool
    relevance_score: float = Field(ge=0, le=100)
    relevance_reason: str
    category: str
    industry: str
    vendor: str
    ai_tool: str
    use_case: str
    business_impact: str
    sentiment_label: str
    sentiment_score: float = Field(ge=-1, le=1)
    summary: str


def connect_snowflake():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

# Reads articles from INT_NEWS_READY_FOR_LLM that do not yet exist in ARTICLE_ENRICHMENT. 
# This prevents paying to enrich the same article repeatedly.
def fetch_pending_articles(conn) -> pd.DataFrame:
    sql = f"""
        SELECT
            source.ARTICLE_KEY,
            source.TITLE,
            source.DESCRIPTION,
            source.TEXT_FOR_LLM,
            source.SOURCE_NAME,
            source.PUBLISHED_DATE
        FROM {SNOWFLAKE_DATABASE}.{DBT_SCHEMA}.{SOURCE_MODEL} AS source
        LEFT JOIN {SNOWFLAKE_DATABASE}.RAW.{TARGET_TABLE} AS enriched
            ON source.ARTICLE_KEY = enriched.ARTICLE_KEY
        WHERE enriched.ARTICLE_KEY IS NULL
        ORDER BY source.PUBLISHED_DATE DESC
    """

    cursor = conn.cursor()

    try:
        cursor.execute(sql)

        rows = cursor.fetchall()
        columns = [column[0].lower() for column in cursor.description]

        return pd.DataFrame(rows, columns=columns)

    finally:
        cursor.close()

# Builds the instructions sent to Groq: what counts as relevant NZ AI news and what fields to return.
def build_prompt(row: pd.Series) -> str:
    return f"""
You are analysing news for a New Zealand AI industry intelligence dashboard.

Determine whether this article provides a meaningful signal about New Zealand's
AI, technology, cloud, data, automation or digital industry.

An article is relevant when it concerns at least one of these:
- AI or GenAI adoption affecting New Zealand
- New Zealand AI companies, vendors, startups or partnerships
- AI tools or platforms being used in New Zealand
- AI investment, infrastructure, cloud or data centres affecting New Zealand
- AI workforce, skills, employment, governance, safety or regulation in New Zealand
- A practical AI use case involving a New Zealand organisation or industry
- A global development with a clear and direct impact on New Zealand's AI industry

An article is not relevant when:
- it only mentions a vendor such as Xero, Spark or Microsoft without an AI connection
- it is mainly sports, entertainment, lifestyle or unrelated general news
- it is global AI news with no clear New Zealand impact
- it is generic promotional or marketing content without meaningful industry insight

Article:
{row.get("text_for_llm") or ""}

Return only one valid JSON object using exactly this structure:

{{
  "is_relevant": true,
  "relevance_score": 0,
  "relevance_reason": "",
  "category": "",
  "industry": "",
  "vendor": "",
  "ai_tool": "",
  "use_case": "",
  "business_impact": "",
  "sentiment_label": "",
  "sentiment_score": 0,
  "summary": ""
}}

Rules:
- relevance_score must be between 0 and 100.
- sentiment_score must be between -1 and 1.
- sentiment_label must be Positive, Neutral or Negative.
- category must be one of:
  Generative AI,
  AI Adoption,
  Cloud & Infrastructure,
  Vendor Activity,
  Workforce & Skills,
  Governance & Regulation,
  Startup & Investment,
  Cybersecurity,
  General Technology,
  Not Relevant.
- industry should identify the main affected sector, such as:
  Technology, Government, Healthcare, Financial Services, Agriculture,
  Education, Retail, Manufacturing, Telecommunications or Cross-industry.
- vendor should contain the primary vendor or organisation.
- ai_tool should contain the specific tool or platform, if identified.
- use_case should describe the practical use of AI.
- business_impact should be High, Medium, Low or None.
- summary should be one short sentence.
- Use "None" when a value cannot be identified.
"""

# Sends one article to Groq, gets JSON back, validates it with ArticleEnrichment, and retries if necessary.
def enrich_article(
    client: Groq,
    row: pd.Series,
) -> ArticleEnrichment:
    prompt = build_prompt(row)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": prompt,
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )

            response_text = response.choices[0].message.content
            response_json = json.loads(response_text)

            return ArticleEnrichment(**response_json)

        except (
            json.JSONDecodeError,
            ValidationError,
            Exception,
        ) as error:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Groq enrichment failed after "
                    f"{MAX_RETRIES} attempts: {error}"
                ) from error

            wait_seconds = SECONDS_BETWEEN_CALLS * attempt * 2

            print(
                f"[WARN] Attempt {attempt} failed. "
                f"Retrying in {wait_seconds} seconds."
            )

            time.sleep(wait_seconds)

    raise RuntimeError("Unexpected enrichment failure.")

# Loops through all pending articles, calls enrich_article() for each, and builds the enrichment DataFrame.
def enrich_pending_articles(df: pd.DataFrame) -> pd.DataFrame:
    client = Groq(api_key=GROQ_API_KEY)
    results = []
    total = len(df)

    for position, (_, row) in enumerate(df.iterrows(), start=1):
        article_key = row["article_key"]
        title = row.get("title") or ""

        print(
            f"[INFO] Enriching {position}/{total}: "
            f"{title[:80]}"
        )

        try:
            enrichment = enrich_article(client, row)

            results.append(
                {
                    "ARTICLE_KEY": article_key,
                    "IS_RELEVANT": enrichment.is_relevant,
                    "RELEVANCE_SCORE": enrichment.relevance_score,
                    "RELEVANCE_REASON": enrichment.relevance_reason,
                    "CATEGORY": enrichment.category,
                    "INDUSTRY": enrichment.industry,
                    "VENDOR": enrichment.vendor,
                    "AI_TOOL": enrichment.ai_tool,
                    "USE_CASE": enrichment.use_case,
                    "BUSINESS_IMPACT": enrichment.business_impact,
                    "SENTIMENT_LABEL": enrichment.sentiment_label,
                    "SENTIMENT_SCORE": enrichment.sentiment_score,
                    "SUMMARY": enrichment.summary,
                    "MODEL_NAME": GROQ_MODEL,
                    "PROMPT_VERSION": PROMPT_VERSION,
                    "ENRICHED_AT": datetime.now(timezone.utc).replace(
                        tzinfo=None
                    ),
                }
            )

        except Exception as error:
            print(
                f"[ERROR] Failed to enrich article "
                f"{article_key}: {error}"
            )

        time.sleep(SECONDS_BETWEEN_CALLS)

    return pd.DataFrame(results)

# Appends the enrichment results to Snowflake RAW.ARTICLE_ENRICHMENT
def upload_enrichment(
    conn,
    enrichment_df: pd.DataFrame,
) -> int:
    if enrichment_df.empty:
        return 0

    success, _, row_count, _ = write_pandas(
        conn=conn,
        df=enrichment_df,
        table_name=TARGET_TABLE,
        database=SNOWFLAKE_DATABASE,
        schema="RAW",
        auto_create_table=False,
        overwrite=False,
        use_logical_type=True,
    )

    if not success:
        raise RuntimeError("Failed to upload enrichment data.")

    return row_count


def main():
    print("=" * 60)
    print("AI NEWS MONITOR - GROQ ENRICHMENT")
    print("=" * 60)

    conn = connect_snowflake()

    try:
        pending_df = fetch_pending_articles(conn)

        print(
            f"[INFO] Articles waiting for enrichment: "
            f"{len(pending_df)}"
        )

        if pending_df.empty:
            print("[INFO] No new articles require enrichment.")
            return

        enrichment_df = enrich_pending_articles(pending_df)

        print(
            f"[INFO] Successfully enriched: "
            f"{len(enrichment_df)}"
        )

        uploaded_rows = upload_enrichment(
            conn,
            enrichment_df,
        )

        print(
            f"[INFO] Uploaded {uploaded_rows} enrichment rows."
        )

    finally:
        conn.close()
        print("[INFO] Snowflake connection closed.")


if __name__ == "__main__":
    main()