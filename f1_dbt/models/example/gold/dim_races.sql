{{ config(
    materialized='incremental',
    unique_key=['season', 'round'],
    incremental_strategy='merge'
) }}

WITH races AS (

    SELECT
        season,
        round,
        race_name,
        race_date,
        circuit_id,
        batch_id

    FROM {{ ref('races') }}

    {% if is_incremental() %}

    WHERE batch_id > (
        SELECT COALESCE(MAX(batch_id), '0000-00')
        FROM {{ this }}
    )

    {% endif %}

),

circuits AS (

    SELECT
        circuit_id,
        circuit_name,
        locality,
        country

    FROM {{ ref('circuits') }}

),

final AS (

    SELECT
        r.season,
        r.round,
        r.race_name,
        r.race_date,
        c.circuit_name,
        c.locality,
        c.country,
        r.batch_id,

        CURRENT_TIMESTAMP() AS created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp

    FROM races r

    INNER JOIN circuits c
        ON r.circuit_id = c.circuit_id

)

SELECT *
FROM final