{{ config(
    materialized='incremental',
    unique_key=['season', 'round'],
    incremental_strategy='merge',
    merge_update_columns=[
        'race_name',
        'race_date',
        'circuit_id',
        'ingestion_timestamp',
        'source_file',
        'batch_id',
        'updated_timestamp'
    ]
) }}

WITH bronze AS (

    SELECT
        SEASON,
        ROUND,
        RACENAME,
        DATE,
        CIRCUITID,
        INGESTION_TIMESTAMP,
        SOURCE_FILE,
        BATCH_ID

    FROM {{ source('bronze', 'races') }}

),

transformed AS (

    SELECT
        SEASON AS season,

        ROUND AS round,

        INITCAP(RACENAME) AS race_name,

        DATE AS race_date,

        CIRCUITID AS circuit_id,

        INGESTION_TIMESTAMP AS ingestion_timestamp,

        SOURCE_FILE AS source_file,

        BATCH_ID AS batch_id

    FROM bronze

),

deduplicated AS (

    SELECT
        season,
        round,
        race_name,
        race_date,
        circuit_id,
        ingestion_timestamp,
        source_file,
        batch_id

    FROM transformed

    WHERE season IS NOT NULL
      AND round IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY season, round
        ORDER BY batch_id DESC, ingestion_timestamp DESC
    ) = 1

)

SELECT
    season,
    round,
    race_name,
    race_date,
    circuit_id,
    ingestion_timestamp,
    source_file,
    batch_id,

    CURRENT_TIMESTAMP() AS created_timestamp,

    CURRENT_TIMESTAMP() AS updated_timestamp

FROM deduplicated

{% if is_incremental() %}

WHERE batch_id >= (
    SELECT COALESCE(MAX(batch_id), '')
    FROM {{ this }}
)

{% endif %}