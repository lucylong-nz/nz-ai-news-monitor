"""
config.py
Project configuration

"""
import os
from dotenv import load_dotenv

load_dotenv()


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)

    if not value:
        raise ValueError(f"Environment variable '{name}' is missing.")

    return value

# ===========================
# API Keys
# ===========================

NEWSDATA_API_KEY = get_env("NEWSDATA_API_KEY")
NEWSAPI_API_KEY = get_env("NEWSAPI_API_KEY")

# ===========================
# Snowflake
# ===========================

SNOWFLAKE_ACCOUNT = get_env("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = get_env("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = get_env("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = get_env("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = get_env("SNOWFLAKE_DATABASE")
SNOWFLAKE_RAW_SCHEMA = get_env(
    "SNOWFLAKE_RAW_SCHEMA",
    "RAW",
)

DBT_SCHEMA = get_env(
    "DBT_SCHEMA",
    "DBT_DEV",
)

# ===========================
# Project Settings
# ===========================

LOOKBACK_DAYS = 30
COUNTRY = "nz"
LANGUAGE = "en"

NZ_TRUSTED_DOMAINS = (
    "nzherald.co.nz,rnz.co.nz,stuff.co.nz,interest.co.nz,businessdesk.co.nz,"
    "newsroom.co.nz,1news.co.nz,itbrief.co.nz,securitybrief.co.nz,"
    "techday.com,nzcity.co.nz,theconversation.com,idealog.co.nz,"
    "stoppress.co.nz,cio.co.nz,reseller.co.nz,computerworld.co.nz"
)

NEWSAPI_NOISE_DOMAINS = (
    "covers.com,ozbargain.com.au,thegadgeteer.com,onefootball.com,"
    "nakedcapitalism.com,securelist.com,helpnetsecurity.com,"
    "timesofindia.indiatimes.com,news.un.org,financialpost.com,"
    "tomshardware.com,comicbook.com,pypi.org,thenextweb.com,"
    "globenewswire.com,crypto.news,cryptobriefing.com"
)

# API search queries define what news to retrieve.
# Each query targets a specific AI topic; results are further filtered in news_fetcher.py.
NEWSAPI_QUERIES = [
    {
        "query_name": "nz_ai_core",
        "query_text": (
            '("generative AI" OR GenAI OR LLM OR "artificial intelligence" '
            'OR chatbot OR ChatGPT OR Copilot OR Gemini OR Claude) '
            'AND ("New Zealand" OR Auckland OR Wellington OR Christchurch OR Kiwi)'
        ),
        "domains": NZ_TRUSTED_DOMAINS
    },
    {
        "query_name": "nz_ai_global_vendors",
        "query_text": (
            '(OpenAI OR Anthropic OR Gemini OR Copilot OR ChatGPT OR Claude '
            'OR Databricks OR Snowflake OR AWS OR Azure OR "Google Cloud") '
            'AND ("New Zealand" OR Auckland OR Wellington OR Christchurch OR Kiwi)'
        ),
        "exclude_domains": NEWSAPI_NOISE_DOMAINS
    },
    {
        "query_name": "nz_local_vendors",
        "query_text": (
            '(Datacom OR "CDC Data Centres" OR Theta OR "Soul Machines" '
            'OR Straker OR Xero OR "Callaghan Innovation" OR "AI Forum" '
            'OR Spark OR "One NZ")'
        ),
        "domains": NZ_TRUSTED_DOMAINS
    },
    {
        "query_name": "nz_ai_adoption_use_cases",
        "query_text": (
            '(AI OR "artificial intelligence" OR GenAI OR automation) '
            'AND (adoption OR deployment OR implementation OR partnership '
            'OR investment OR productivity OR customer OR healthcare '
            'OR agriculture OR banking OR education OR government) '
            'AND ("New Zealand" OR NZ OR Auckland OR Wellington OR Kiwi)'
        ),
        "domains": NZ_TRUSTED_DOMAINS
    },
    {
        "query_name": "nz_ai_workforce_policy",
        "query_text": (
            '(AI OR "artificial intelligence" OR automation) '
            'AND (jobs OR workforce OR skills OR training OR regulation '
            'OR privacy OR governance OR ethics OR policy) '
            'AND ("New Zealand" OR NZ OR Auckland OR Wellington OR Kiwi)'
        ),
        "domains": NZ_TRUSTED_DOMAINS
    }
]

NEWSDATA_QUERIES = [
    {
        "query_name": "nz_ai_core",
        "q": '(AI OR "artificial intelligence") AND ("New Zealand" OR Kiwi)',
        "country": "nz",
        "category": "business,technology",
        "language": "en"
    },
    {
        "query_name": "nz_genai_core",
        "q": '("generative AI" OR GenAI OR LLM OR chatbot) AND "New Zealand"',
        "country": "nz",
        "category": "business,technology",
        "language": "en"
    },
    {
        "query_name": "nz_local_vendors",
        "q": '(Datacom OR Theta OR "Soul Machines" OR Straker OR Xero)',
        "country": "nz",
        "category": "business,technology",
        "language": "en"
    },
    {
        "query_name": "nz_cloud_vendors",
        "q": '(AWS OR Azure OR Snowflake OR Databricks) AND "New Zealand"',
        "country": "nz",
        "category": "business,technology",
        "language": "en"
    }
]