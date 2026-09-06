{{ config(
    materialized='incremental',
    unique_key='driver_id',
    incremental_strategy='merge'
) }}

WITH drivers AS (

    SELECT
        driver_id,
        driver_name,
        date_of_birth,
        nationality,
        batch_id

    FROM {{ ref('drivers') }}

    {% if is_incremental() %}

    WHERE batch_id > (
        SELECT COALESCE(MAX(batch_id), '0000-00')
        FROM {{ this }}
    )

    {% endif %}

),

nationality_region AS (

    SELECT
        nationality,
        region AS nationality_region

    FROM {{ ref('ref_nationality_region') }}

),

final AS (

    SELECT
        d.driver_id,
        d.driver_name,
        d.date_of_birth,
        d.nationality,
        n.nationality_region,
        d.batch_id,

        CURRENT_TIMESTAMP() AS created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp

    FROM drivers d

    LEFT JOIN nationality_region n
        ON d.nationality = n.nationality

)

SELECT *
FROM final