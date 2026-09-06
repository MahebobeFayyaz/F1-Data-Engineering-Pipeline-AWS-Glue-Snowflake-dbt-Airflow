{{ config(
    materialized='incremental',
    unique_key=['season', 'round', 'constructor_id', 'driver_id', 'session_type'],
    incremental_strategy='merge'
) }}

WITH results AS (

    SELECT
        season,
        round,
        constructor_id,
        driver_id,
        grid_position,
        completed_laps,
        car_number,
        points,
        final_position,
        final_position_text,
        status,
        batch_id,

        'RACE' AS session_type

    FROM {{ ref('results') }}

    {% if is_incremental() %}

    WHERE batch_id > (
        SELECT COALESCE(MAX(batch_id), '0000-00')
        FROM {{ this }}
    )

    {% endif %}

),

sprints AS (

    SELECT
        season,
        round,
        constructor_id,
        driver_id,
        grid_position,
        completed_laps,
        car_number,
        points,
        final_position,
        final_position_text,
        status,
        batch_id,

        'SPRINT' AS session_type

    FROM {{ ref('sprints') }}

    {% if is_incremental() %}

    WHERE batch_id > (
        SELECT COALESCE(MAX(batch_id), '0000-00')
        FROM {{ this }}
    )

    {% endif %}

),

combined AS (

    SELECT * FROM results

    UNION ALL

    SELECT * FROM sprints

),

final AS (

    SELECT
        season,
        round,
        constructor_id,
        driver_id,
        session_type,
        grid_position,
        completed_laps,
        car_number,
        points,
        final_position,
        final_position_text,
        status,

        CASE
            WHEN final_position = 1 THEN TRUE
            ELSE FALSE
        END AS is_win,

        CASE
            WHEN final_position BETWEEN 1 AND 3 THEN TRUE
            ELSE FALSE
        END AS is_podium,

        CASE
            WHEN points > 0 THEN TRUE
            ELSE FALSE
        END AS has_points,

        batch_id,

        CURRENT_TIMESTAMP() AS created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp

    FROM combined

)

SELECT *
FROM final