package com.erebelo.springdataaws.service.impl;

import com.erebelo.springdataaws.service.S3Service;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@Slf4j
@Service
@RequiredArgsConstructor
public class S3ServiceImpl implements S3Service {

    private final S3Client s3Client;

    @Value("${s3.bucket.name}")
    private String bucketName;

    @Override
    public void singlePartUpload(String keyName, String metadataTitle, String contentType, byte[] fileData) {
        try {
            // Create an input stream from the byte array
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fileData);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(keyName)
                    .metadata(Map.of("title", metadataTitle)).contentType(contentType).build();

            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(byteArrayInputStream, fileData.length));
            log.info("file {} uploaded successfully to bucket {}", keyName, bucketName);
        } catch (S3Exception e) {
            log.error("S3 error occurred during upload: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void multipartUpload(String keyName, String metadataTitle, String contentType, byte[] fileData) {
        try {
            // Initiate the multipart upload
            CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(b -> b
                    .bucket(bucketName).key(keyName).metadata(Map.of("title", metadataTitle)).contentType(contentType));
            String uploadId = createMultipartUploadResponse.uploadId();

            // Upload the parts of the file
            int partNumber = 1;
            List<CompletedPart> completedParts = new ArrayList<>();
            ByteBuffer bb = ByteBuffer.allocate(5 * 1024 * 1024); // 5 MB buffer

            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fileData)) {
                byte[] buffer = new byte[5 * 1024 * 1024]; // 5 MB buffer
                int bytesRead;

                while ((bytesRead = byteArrayInputStream.read(buffer)) > 0) {
                    bb.clear();
                    bb.put(buffer, 0, bytesRead);
                    bb.flip();

                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(bucketName).key(keyName)
                            .uploadId(uploadId).partNumber(partNumber).build();

                    UploadPartResponse partResponse = s3Client.uploadPart(uploadPartRequest,
                            RequestBody.fromByteBuffer(bb));

                    CompletedPart part = CompletedPart.builder().partNumber(partNumber).eTag(partResponse.eTag())
                            .build();
                    completedParts.add(part);

                    partNumber++;
                }
            } catch (IOException e) {
                log.error("Error reading file data to upload to bucket: " + e.getMessage());
                throw new UncheckedIOException("Error reading file data to upload to bucket", e);
            }

            // Complete the multipart upload
            s3Client.completeMultipartUpload(b -> b.bucket(bucketName).key(keyName).uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()));
            log.info("File {} uploaded successfully to bucket {}", keyName, bucketName);
        } catch (S3Exception e) {
            log.error("S3 error occurred during multipart upload: " + e.getMessage());
            throw e;
        }
    }
}
