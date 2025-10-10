package com.erebelo.springdataaws.constant;

import lombok.experimental.UtilityClass;

@UtilityClass
public class AddressConstant {

    public static final String ADDRESS_QUERY_NAME = "addresses_query";
    public static final int ADDRESS_ATHENA_BATCH_SIZE = 2_000;
    public static final String ADDRESS_CSV_FILE_NAME = "addresses.csv";
    public static final String ADDRESS_S3_KEY_NAME = "addresses/" + ADDRESS_CSV_FILE_NAME;
    public static final String ADDRESS_S3_METADATA_TITLE = "Addresses CSV";
    public static final String ADDRESS_S3_CONTENT_TYPE = "text/csv";

}
