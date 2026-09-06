{{ config(
    materialized='incremental',
    unique_key='constructor_id',
    incremental_strategy='merge'
) }}

WITH constructors AS (

    SELECT
        constructor_id,
        constructor_name,
        nationality,
        batch_id

    FROM {{ ref('constructors') }}

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
        c.constructor_id,
        c.constructor_name,
        c.nationality,
        n.nationality_region,
        c.batch_id,

        CURRENT_TIMESTAMP() AS created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp

    FROM constructors c

    LEFT JOIN nationality_region n
        ON c.nationality = n.nationality

)

SELECT *
FROM final