package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

class MongoBeanConfigurationTest {

    @Test
    void testTransactionManagerBean() {
        MongoTransactionManager transactionManager = new MongoBeanConfiguration()
                .transactionManager(mock(MongoDatabaseFactory.class));

        assertNotNull(transactionManager);
    }
}
