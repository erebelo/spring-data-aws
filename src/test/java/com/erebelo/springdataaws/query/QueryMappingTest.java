package com.erebelo.springdataaws.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.PropertyPlaceholderHelper;

class QueryMappingTest {

    private QueryMapping queryMapping;

    private static final String DATABASE = "db_test";
    private static final String LEGACY_ADDRESSES = "legacy_addresses";

    @BeforeEach
    void setUp() {
        queryMapping = new QueryMapping();
        queryMapping.setDatabase(DATABASE);
        queryMapping.setLegacyAddresses(LEGACY_ADDRESSES);

        // Manually call init since @PostConstruct won’t be called in plain unit test
        queryMapping.init();
    }

    @Test
    void testQueryTemplateInitialized() {
        String template = queryMapping.getQueryByName("addresses_query");

        String expectedPrefix = "WITH legacy_addresses_dedup AS (";
        String expectedDbPlaceholderReplaced = "FROM " + DATABASE + "." + LEGACY_ADDRESSES;
        assertTrue(template.contains(expectedPrefix));
        assertTrue(template.contains(expectedDbPlaceholderReplaced));
    }

    @Test
    void testGetQueryByNameReturnsCorrectQuery() {
        String query = queryMapping.getQueryByName("addresses_query");

        assertTrue(query.contains(DATABASE));
        assertTrue(query.contains(LEGACY_ADDRESSES));
    }

    @Test
    void testGetQueryByNameThrowsExceptionIfNotFound() {
        assertThrows(NullPointerException.class, () -> queryMapping.getQueryByName("non_existing_query"));
    }

    @Test
    void testQueryTablePropertiesUnmodifiable() {
        Map<String, String> props = queryMapping.getQueryTableProperties().get("addresses");
        assertThrows(NullPointerException.class, () -> props.put("db", "new_db"));
    }

    @Test
    void testQueryTemplateUnmodifiable() {
        Map<String, String> queryTemplate = queryMapping.getQueryTemplate();
        assertThrows(UnsupportedOperationException.class, () -> queryTemplate.put("new", "query"));
    }

    @Test
    void testPlaceholderHelperWorks() {
        PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");
        String template = "SELECT * FROM ${table}";
        String replaced = helper.replacePlaceholders(template, Map.of("table", "my_table")::get);
        assertEquals("SELECT * FROM my_table", replaced);
    }
}
