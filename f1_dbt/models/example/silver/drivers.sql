{{ config(
    materialized='incremental',
    unique_key='driver_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'driver_name',
        'date_of_birth',
        'nationality',
        'ingestion_timestamp',
        'source_file',
        'batch_id',
        'updated_timestamp'
    ]
) }}

WITH bronze AS (

    SELECT
        DRIVERID,
        GIVENNAME,
        FAMILYNAME,
        DATEOFBIRTH,
        NATIONALITY,
        INGESTION_TIMESTAMP,
        SOURCE_FILE,
        BATCH_ID

    FROM {{ source('bronze', 'drivers') }}

),

transformed AS (

    SELECT
        DRIVERID AS driver_id,

        INITCAP(
            GIVENNAME || ' ' || FAMILYNAME
        ) AS driver_name,

        DATEOFBIRTH AS date_of_birth,

        INITCAP(NATIONALITY) AS nationality,

        INGESTION_TIMESTAMP AS ingestion_timestamp,

        SOURCE_FILE AS source_file,

        BATCH_ID AS batch_id

    FROM bronze

    WHERE DRIVERID IS NOT NULL

),

deduplicated AS (

    SELECT
        driver_id,
        driver_name,
        date_of_birth,
        nationality,
        ingestion_timestamp,
        source_file,
        batch_id

    FROM transformed

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY driver_id
        ORDER BY batch_id DESC, ingestion_timestamp DESC
    ) = 1

)

SELECT
    driver_id,
    driver_name,
    date_of_birth,
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