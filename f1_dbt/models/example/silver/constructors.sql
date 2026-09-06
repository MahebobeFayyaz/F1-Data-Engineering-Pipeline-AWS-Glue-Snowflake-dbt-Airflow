{{ config(
    materialized='incremental',
    unique_key='constructor_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'constructor_name',
        'nationality',
        'ingestion_timestamp',
        'source_file',
        'batch_id',
        'updated_timestamp'
    ]
) }}

WITH bronze AS (

    SELECT
        CONSTRUCTORID,
        NAME,
        NATIONALITY,
        INGESTION_TIMESTAMP,
        SOURCE_FILE,
        BATCH_ID

    FROM {{ source('bronze', 'constructors') }}

),

transformed AS (

    SELECT
        CONSTRUCTORID AS constructor_id,

        INITCAP(NAME) AS constructor_name,

        INITCAP(NATIONALITY) AS nationality,

        INGESTION_TIMESTAMP AS ingestion_timestamp,

        SOURCE_FILE AS source_file,

        BATCH_ID AS batch_id

    FROM bronze

    WHERE CONSTRUCTORID IS NOT NULL

),

deduplicated AS (

    SELECT
        constructor_id,
        constructor_name,
        nationality,
        ingestion_timestamp,
        source_file,
        batch_id

    FROM transformed

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY constructor_id
        ORDER BY batch_id DESC, ingestion_timestamp DESC
    ) = 1

)

SELECT
    constructor_id,
    constructor_name,
    nationality,
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