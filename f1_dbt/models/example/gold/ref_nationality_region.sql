{{ config(
    materialized='table'
) }}

WITH nationality_region AS (

    SELECT * FROM VALUES

        -- Europe
        ('British', 'Europe'),
        ('Italian', 'Europe'),
        ('French', 'Europe'),
        ('German', 'Europe'),
        ('Swiss', 'Europe'),
        ('Dutch', 'Europe'),
        ('Belgium', 'Europe'),
        ('Belgian', 'Europe'),
        ('Irish', 'Europe'),
        ('Spanish', 'Europe'),
        ('Austrian', 'Europe'),
        ('East German', 'Europe'),
        ('Russian', 'Europe'),
        ('Finnish', 'Europe'),
        ('Polish', 'Europe'),
        ('Portuguese', 'Europe'),
        ('Hungarian', 'Europe'),
        ('Danish', 'Europe'),
        ('Czech', 'Europe'),
        ('Liechtensteiner', 'Europe'),
        ('Monegasque', 'Europe'),
        ('Swedish', 'Europe'),
        ('Argentine-italian', 'Europe'),
        ('American-italian', 'Europe'),

        -- North America
        ('American', 'North America'),
        ('Canadian', 'North America'),
        ('Mexican', 'North America'),

        -- South America
        ('Brazilian', 'South America'),
        ('Chilean', 'South America'),
        ('Argentine', 'South America'),
        ('Uruguayan', 'South America'),
        ('Venezuelan', 'South America'),
        ('Colombian', 'South America'),

        -- Africa
        ('South African', 'Africa'),
        ('Rhodesian', 'Africa'),

        -- Asia
        ('Indian', 'Asia'),
        ('Chinese', 'Asia'),
        ('Japanese', 'Asia'),
        ('Malaysian', 'Asia'),
        ('Hong Kong', 'Asia'),
        ('Indonesian', 'Asia'),
        ('Thai', 'Asia'),

        -- Oceania
        ('Australian', 'Oceania'),
        ('New Zealand', 'Oceania'),
        ('New Zealander', 'Oceania')

    AS t(nationality, region)

)

SELECT
    nationality,
    region
FROM nationality_region