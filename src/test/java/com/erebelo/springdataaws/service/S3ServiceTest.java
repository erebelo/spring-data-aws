package com.erebelo.springdataaws.service;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import com.erebelo.springdataaws.service.impl.S3ServiceImpl;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@ExtendWith(MockitoExtension.class)
class S3ServiceTest {

    @InjectMocks
    private S3ServiceImpl service;

    @Mock
    private S3Client s3Client;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "bucketName", "test-bucket");
    }

    @Test
    void testSinglePartUploadSuccessful() {
        String keyName = "test-key";
        String metadataTitle = "test-title";
        String contentType = "test-content-type";
        byte[] fileData = "test-data".getBytes();

        assertDoesNotThrow(() -> service.singlePartUpload(keyName, metadataTitle, contentType, fileData));

        verify(s3Client, atLeastOnce()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testSinglePartUploadThrowsS3Exception() {
        String keyName = "test-key";
        String metadataTitle = "test-title";
        String contentType = "test-content-type";
        byte[] fileData = "test-data".getBytes();

        given(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .willThrow(S3Exception.builder().message("Test S3 exception").build());

        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(() -> service.singlePartUpload(keyName, metadataTitle, contentType, fileData))
                .withMessage("Test S3 exception");

        verify(s3Client, atLeastOnce()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testMultipartUploadSuccessful() {
        String bucketName = "test-bucket";
        String keyName = "test-key";
        String metadataTitle = "test-title";
        String contentType = "test-content-type";
        byte[] fileData = "test-data".getBytes();

        CreateMultipartUploadResponse createMultipartUploadResponse = CreateMultipartUploadResponse.builder()
                .uploadId("test-upload-id").build();
        given(s3Client.createMultipartUpload(Mockito.<Consumer<CreateMultipartUploadRequest.Builder>>any()))
                .willReturn(createMultipartUploadResponse);

        UploadPartResponse uploadPartResponse = UploadPartResponse.builder().eTag("test-etag").build();
        given(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).willReturn(uploadPartResponse);

        assertDoesNotThrow(() -> service.multipartUpload(keyName, metadataTitle, contentType, fileData));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<CreateMultipartUploadRequest.Builder>> createMultipartCaptor = ArgumentCaptor
                .forClass(Consumer.class);
        verify(s3Client).createMultipartUpload(createMultipartCaptor.capture());

        CreateMultipartUploadRequest.Builder capturedBuilder = CreateMultipartUploadRequest.builder();
        createMultipartCaptor.getValue().accept(capturedBuilder);
        CreateMultipartUploadRequest request = capturedBuilder.build();

        assertEquals(bucketName, request.bucket());
        assertEquals(keyName, request.key());
        assertEquals(Map.of("title", metadataTitle), request.metadata());
        assertEquals(contentType, request.contentType());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<CompleteMultipartUploadRequest.Builder>> completeMultipartCaptor = ArgumentCaptor
                .forClass((Class<Consumer<CompleteMultipartUploadRequest.Builder>>) (Class<?>) Consumer.class);
        verify(s3Client).completeMultipartUpload(completeMultipartCaptor.capture());

        CompleteMultipartUploadRequest.Builder completeBuilder = CompleteMultipartUploadRequest.builder();
        completeMultipartCaptor.getValue().accept(completeBuilder);
        CompleteMultipartUploadRequest completeRequest = completeBuilder.build();

        assertEquals(bucketName, completeRequest.bucket());
        assertEquals(keyName, completeRequest.key());
        assertNotNull(completeRequest.multipartUpload());
        assertFalse(completeRequest.multipartUpload().parts().isEmpty());
    }

    @Test
    void testMultipartUploadThrowsS3Exception() {
        String keyName = "test-key";
        String metadataTitle = "test-title";
        String contentType = "test-content-type";
        byte[] fileData = "test-data".getBytes();

        given(s3Client.createMultipartUpload(Mockito.<Consumer<CreateMultipartUploadRequest.Builder>>any()))
                .willThrow(S3Exception.builder().message("Test S3 exception").build());

        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(() -> service.multipartUpload(keyName, metadataTitle, contentType, fileData))
                .withMessage("Test S3 exception");

        verify(s3Client, atLeastOnce())
                .createMultipartUpload(Mockito.<Consumer<CreateMultipartUploadRequest.Builder>>any());
    }

    @Test
    void testMultipartUploadThrowsUncheckedIOException() {
        String keyName = "test-key";
        String metadataTitle = "test-title";
        String contentType = "test-content-type";
        byte[] fileData = "test-data".getBytes();

        CreateMultipartUploadResponse createMultipartUploadResponse = CreateMultipartUploadResponse.builder()
                .uploadId("test-upload-id").build();
        given(s3Client.createMultipartUpload(Mockito.<Consumer<CreateMultipartUploadRequest.Builder>>any()))
                .willReturn(createMultipartUploadResponse);

        given(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).willAnswer(invocation -> {
            throw new IOException("Test IO exception");
        });

        assertThatExceptionOfType(UncheckedIOException.class)
                .isThrownBy(() -> service.multipartUpload(keyName, metadataTitle, contentType, fileData))
                .withMessage("Error reading file data to upload to bucket");

        verify(s3Client, atLeastOnce())
                .createMultipartUpload(Mockito.<Consumer<CreateMultipartUploadRequest.Builder>>any());
        verify(s3Client, atLeastOnce()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
    }
}
