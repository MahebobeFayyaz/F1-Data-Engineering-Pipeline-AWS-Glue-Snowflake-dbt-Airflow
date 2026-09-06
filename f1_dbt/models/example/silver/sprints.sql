{{ config(
    materialized='incremental',
    unique_key=['season', 'round', 'constructor_id', 'driver_id'],
    incremental_strategy='merge',
    merge_update_columns=[
        'race_name',
        'race_date',
        'grid_position',
        'completed_laps',
        'car_number',
        'points',
        'final_position',
        'final_position_text',
        'status',
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
        CONSTRUCTORID,
        DRIVERID,
        DATE,
        RACENAME,
        GRID,
        LAPS,
        NUMBER,
        POINTS,
        POSITION,
        POSITIONTEXT,
        STATUS,
        INGESTION_TIMESTAMP,
        SOURCE_FILE,
        BATCH_ID

    FROM {{ source('bronze', 'sprints') }}

),

transformed AS (

    SELECT
        SEASON AS season,

        ROUND AS round,

        CONSTRUCTORID AS constructor_id,

        DRIVERID AS driver_id,

        DATE AS race_date,

        INITCAP(RACENAME) AS race_name,

        GRID AS grid_position,

        LAPS AS completed_laps,

        NUMBER AS car_number,

        POINTS AS points,

        POSITION AS final_position,

        POSITIONTEXT AS final_position_text,

        STATUS AS status,

        INGESTION_TIMESTAMP AS ingestion_timestamp,

        SOURCE_FILE AS source_file,

        BATCH_ID AS batch_id

    FROM bronze

    WHERE SEASON IS NOT NULL
      AND ROUND IS NOT NULL
      AND CONSTRUCTORID IS NOT NULL
      AND DRIVERID IS NOT NULL

),

deduplicated AS (

    SELECT
        season,
        round,
        constructor_id,
        driver_id,
        race_date,
        race_name,
        grid_position,
        completed_laps,
        car_number,
        points,
        final_position,
        final_position_text,
        status,
        ingestion_timestamp,
        source_file,
        batch_id

    FROM transformed

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            season,
            round,
            constructor_id,
            driver_id

        ORDER BY
            batch_id DESC,
            ingestion_timestamp DESC
    ) = 1

)

SELECT
    season,
    round,
    constructor_id,
    driver_id,
    race_date,
    race_name,
    grid_position,
    completed_laps,
    car_number,
    points,
    final_position,
    final_position_text,
    status,
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