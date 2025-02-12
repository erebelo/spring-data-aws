package com.erebelo.springdataaws.service;

public interface S3Service {

    void singlePartUpload(String keyName, String metadataTitle, String contentType, byte[] fileData);

    void multipartUpload(String keyName, String metadataTitle, String contentType, byte[] fileData);

}
