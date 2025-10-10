package com.erebelo.springdataaws.query;

import lombok.experimental.UtilityClass;

@UtilityClass
public class AddressQueryTemplate {

    public static final String LEGACY_ADDRESSES_QUERY_TEMPLATE = """
                WITH legacy_addresses_dedup AS (
                    -- Removes duplicated records from table legacy_addresses by id and address_type
                    SELECT *
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY TRIM(id), TRIM(address_type) ORDER BY id
                               ) AS row_num
                        FROM ${db}.${legacyAddresses}
                    )
                    WHERE row_num = 1
                ),
                legacy_addresses_grouped AS (
                    SELECT
                        TRIM(id) AS address_id,
                        -- address_type HOME
                        MAX(CASE WHEN TRIM(address_type) = 'HOME' THEN TRIM(address_type) END) AS home_address_type,
                        MAX(CASE WHEN TRIM(address_type) = 'HOME' THEN TRIM(address_line_1) END) AS home_address_line_1,
                        MAX(CASE WHEN TRIM(address_type) = 'HOME' THEN TRIM(address_line_2) END) AS home_address_line_2,
                        MAX(CASE WHEN TRIM(address_type) = 'HOME' THEN TRIM(city) END) AS home_city,
                        MAX(CASE WHEN TRIM(address_type) = 'HOME' THEN TRIM(state) END) AS home_state,
                        MAX(CASE WHEN TRIM(address_type) = 'HOME' THEN TRIM(zip_code) END) AS home_zip_code,
                        MAX(CASE WHEN TRIM(address_type) = 'HOME' THEN TRIM(country) END) AS home_country,
                        -- address_type WORK
                        MAX(CASE WHEN TRIM(address_type) = 'WORK' THEN TRIM(address_type) END) AS work_address_type,
                        MAX(CASE WHEN TRIM(address_type) = 'WORK' THEN TRIM(address_line_1) END) AS work_address_line_1,
                        MAX(CASE WHEN TRIM(address_type) = 'WORK' THEN TRIM(address_line_2) END) AS work_address_line_2,
                        MAX(CASE WHEN TRIM(address_type) = 'WORK' THEN TRIM(city) END) AS work_city,
                        MAX(CASE WHEN TRIM(address_type) = 'WORK' THEN TRIM(state) END) AS work_state,
                        MAX(CASE WHEN TRIM(address_type) = 'WORK' THEN TRIM(zip_code) END) AS work_zip_code,
                        MAX(CASE WHEN TRIM(address_type) = 'WORK' THEN TRIM(country) END) AS work_country,
                        -- address_type OTHER
                        MAX(CASE WHEN TRIM(address_type) = 'OTHER' THEN TRIM(address_type) END) AS other_address_type,
                        MAX(CASE WHEN TRIM(address_type) = 'OTHER' THEN TRIM(address_line_1) END) AS other_address_line_1,
                        MAX(CASE WHEN TRIM(address_type) = 'OTHER' THEN TRIM(address_line_2) END) AS other_address_line_2,
                        MAX(CASE WHEN TRIM(address_type) = 'OTHER' THEN TRIM(city) END) AS other_city,
                        MAX(CASE WHEN TRIM(address_type) = 'OTHER' THEN TRIM(state) END) AS other_state,
                        MAX(CASE WHEN TRIM(address_type) = 'OTHER' THEN TRIM(zip_code) END) AS other_zip_code,
                        MAX(CASE WHEN TRIM(address_type) = 'OTHER' THEN TRIM(country) END) AS other_country
                    FROM legacy_addresses_dedup
                    GROUP BY TRIM(id)
                )
                SELECT ROW_NUMBER() OVER () AS record_id, *
                FROM legacy_addresses_grouped
                ORDER BY record_id ASC;
            """;
}
