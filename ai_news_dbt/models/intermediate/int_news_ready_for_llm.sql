with staged as (

    select *
    from {{ ref('stg_news_articles') }}

),

prepared as (

    select
        article_key,
        provider,
        source_name,
        author,
        title,
        description,
        article_url,
        published_at,
        published_date,
        news_age_days,
        news_age_bucket,
        language,
        country,
        source_category,
        batch_id,
        ingested_at,

        trim(
            'Title: ' || coalesce(title, '')
            || '\nDescription: ' || coalesce(description, '')
            || '\nSource: ' || coalesce(source_name, '')
            || '\nPublished date: ' || coalesce(published_date::varchar, '')
        ) as text_for_llm

    from staged

    where title is not null
      and article_key is not null

)

select *
from prepared