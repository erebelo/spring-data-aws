package com.erebelo.springdataaws.query;

import lombok.experimental.UtilityClass;

@UtilityClass
public class AddressQuery {

    public static final String ADDRESS_QUERY = """
            WITH legacy_addresses_dedup AS (
                -- Removes duplicated records from table legacy_addresses by id and addresstype
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY TRIM(id), TRIM(addresstype) ORDER BY id
                           ) AS row_num
                    FROM "spring_data_aws_db"."legacy_addresses"
                ) t
                WHERE row_num = 1
            ),
            legacy_addresses_grouped AS (
                SELECT
                    TRIM(id) AS addressid,
                    -- addresstype HOME
                    MAX(CASE WHEN TRIM(addresstype) = 'HOME' THEN TRIM(addresstype) END) AS home_addresstype,
                    MAX(CASE WHEN TRIM(addresstype) = 'HOME' THEN TRIM(addressline1) END) AS home_addressline1,
            		MAX(CASE WHEN TRIM(addresstype) = 'HOME' THEN TRIM(addressline2) END) AS home_addressline2,
                    MAX(CASE WHEN TRIM(addresstype) = 'HOME' THEN TRIM(city) END) AS home_city,
                    MAX(CASE WHEN TRIM(addresstype) = 'HOME' THEN TRIM(state) END) AS home_state,
                    MAX(CASE WHEN TRIM(addresstype) = 'HOME' THEN TRIM(country) END) AS home_country,
                    MAX(CASE WHEN TRIM(addresstype) = 'HOME' THEN TRIM(zipcode) END) AS home_zipcode,
                    -- addresstype WORK
                    MAX(CASE WHEN TRIM(addresstype) = 'WORK' THEN TRIM(addresstype) END) AS work_addresstype,
                    MAX(CASE WHEN TRIM(addresstype) = 'WORK' THEN TRIM(addressline1) END) AS work_addressline1,
            		MAX(CASE WHEN TRIM(addresstype) = 'WORK' THEN TRIM(addressline2) END) AS work_addressline2,
                    MAX(CASE WHEN TRIM(addresstype) = 'WORK' THEN TRIM(city) END) AS work_city,
                    MAX(CASE WHEN TRIM(addresstype) = 'WORK' THEN TRIM(state) END) AS work_state,
                    MAX(CASE WHEN TRIM(addresstype) = 'WORK' THEN TRIM(country) END) AS work_country,
                    MAX(CASE WHEN TRIM(addresstype) = 'WORK' THEN TRIM(zipcode) END) AS work_zipcode,
            		-- addresstype OTHER
                    MAX(CASE WHEN TRIM(addresstype) = 'OTHER' THEN TRIM(addresstype) END) AS other_addresstype,
                    MAX(CASE WHEN TRIM(addresstype) = 'OTHER' THEN TRIM(addressline1) END) AS other_addressline1,
            		MAX(CASE WHEN TRIM(addresstype) = 'OTHER' THEN TRIM(addressline2) END) AS other_addressline2,
                    MAX(CASE WHEN TRIM(addresstype) = 'OTHER' THEN TRIM(city) END) AS other_city,
                    MAX(CASE WHEN TRIM(addresstype) = 'OTHER' THEN TRIM(state) END) AS other_state,
                    MAX(CASE WHEN TRIM(addresstype) = 'OTHER' THEN TRIM(country) END) AS other_country,
                    MAX(CASE WHEN TRIM(addresstype) = 'OTHER' THEN TRIM(zipcode) END) AS other_zipcode
                FROM legacy_addresses_dedup
                GROUP BY TRIM(id)
            )
            SELECT ROW_NUMBER() OVER () AS recordid, *
            FROM legacy_addresses_grouped
            ORDER BY recordid ASC
            """;
}
