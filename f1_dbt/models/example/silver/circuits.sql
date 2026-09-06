{{ config(
    materialized='incremental',
    unique_key='circuit_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'circuit_name',
        'latitude',
        'longitude',
        'locality',
        'country',
        'ingestion_timestamp',
        'source_file',
        'batch_id',
        'updated_timestamp'
    ]
) }}

WITH bronze AS (

    SELECT
        CIRCUITID,
        CIRCUITNAME,
        LAT,
        LONG,
        LOCALITY,
        COUNTRY,
        INGESTION_TIMESTAMP,
        SOURCE_FILE,
        BATCH_ID

    FROM {{ source('bronze', 'circuits') }}

),

transformed AS (

    SELECT
        CIRCUITID AS circuit_id,

        INITCAP(CIRCUITNAME) AS circuit_name,

        LAT AS latitude,

        LONG AS longitude,

        INITCAP(LOCALITY) AS locality,

        COUNTRY AS country,

        INGESTION_TIMESTAMP AS ingestion_timestamp,

        SOURCE_FILE AS source_file,

        BATCH_ID AS batch_id

    FROM bronze

    WHERE CIRCUITID IS NOT NULL

),

deduplicated AS (

    SELECT
        circuit_id,
        circuit_name,
        latitude,
        longitude,
        locality,
        country,
        ingestion_timestamp,
        source_file,
        batch_id

    FROM transformed

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY circuit_id
        ORDER BY batch_id DESC, ingestion_timestamp DESC
    ) = 1

)

SELECT
    circuit_id,
    circuit_name,
    latitude,
    longitude,
    locality,
    country,
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