package com.erebelo.springdataaws.service;

import static com.erebelo.springdataaws.mock.AddressMock.EXECUTION_ID;
import static com.erebelo.springdataaws.mock.AddressMock.LEGACY_ADDRESSES_QUERY_TEMPLATE;
import static com.erebelo.springdataaws.mock.AddressMock.getResponsePair;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willCallRealMethod;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.erebelo.springdataaws.domain.dto.AddressContextDto;
import com.erebelo.springdataaws.domain.dto.AddressDto;
import com.erebelo.springdataaws.query.QueryMapping;
import com.erebelo.springdataaws.service.impl.AddressServiceImpl;
import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.slf4j.MDC;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
class AddressServiceTest {

    @InjectMocks
    private AddressServiceImpl service;

    @Mock
    private QueryMapping queryMapping;

    @Mock
    private S3Service s3Service;

    @Mock
    private Executor asyncTaskExecutor;

    @Spy
    private AthenaServiceImpl athenaService = new AthenaServiceImpl(mock(AthenaClient.class), "db_test",
            "s3://test-output-bucket", "test_wg");

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "s3AddressesPath", "test/addresses");
    }

    @Test
    void testTriggerAddressFeedWith2KRecordsSuccessful() {
        given(queryMapping.getQueryByName(anyString())).willReturn(LEGACY_ADDRESSES_QUERY_TEMPLATE);

        // stubbing athenaService spy method response
        willReturn(getResponsePair()).given(athenaService).fetchDataFromAthena(anyString());

        willAnswer((Answer<Void>) invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).given(asyncTaskExecutor).execute(any(Runnable.class));

        // using athenaService spy to call the real method implementation
        willCallRealMethod().given(athenaService).processAndSkipHeaderOnce(anyList(), any(AddressContextDto.class));

        // using athenaService spy to call the real method implementation
        willCallRealMethod().given(athenaService).mapRowsToClass(any(String[].class), anyList(), eq(AddressDto.class));

        willDoNothing().given(s3Service).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));

        Map<String, String> loggingContext = Map.of("key", "value");

        try (MockedStatic<MDC> mockedMdc = mockStatic(MDC.class)) {
            mockedMdc.when(MDC::getCopyOfContextMap).thenReturn(loggingContext);

            String response = service.triggerAddressFeed();

            assertEquals(EXECUTION_ID, response);

            verify(queryMapping).getQueryByName(anyString());
            verify(athenaService).fetchDataFromAthena(anyString());
            verify(asyncTaskExecutor).execute(any(Runnable.class));
            verify(athenaService, atLeastOnce()).processAndSkipHeaderOnce(anyList(), any(AddressContextDto.class));
            verify(athenaService, atLeastOnce()).mapRowsToClass(any(String[].class), anyList(), eq(AddressDto.class));

            ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
            verify(s3Service).multipartUpload(startsWith("test/addresses/"), anyString(), anyString(),
                    bytesCaptor.capture());

            String csv = new String(bytesCaptor.getValue(), StandardCharsets.UTF_8);

            // Verify CSV content
            assertEquals(2000, csv.lines().count(), "Header + 2000 records should be present");
            assertFalse(csv.contains("record_id"), "CSV should not contain record_id column");
            assertTrue(csv.contains("address_id"), "CSV should contain header 'address_id'");
            assertTrue(csv.contains("New York"), "CSV should contain city data from mock");
            assertTrue(bytesCaptor.getValue().length > 100, "Uploaded CSV should not be empty");

            mockedMdc.verify(() -> MDC.setContextMap(loggingContext));
            mockedMdc.verify(MDC::clear);
        }
    }

    @Test
    void testTriggerAddressFeedWithEmptyResultsSuccessful() {
        given(queryMapping.getQueryByName(anyString())).willReturn(LEGACY_ADDRESSES_QUERY_TEMPLATE);

        // stubbing athenaService spy method response
        willReturn(Pair.of(EXECUTION_ID, Collections.emptyList())).given(athenaService)
                .fetchDataFromAthena(anyString());

        willAnswer((Answer<Void>) invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).given(asyncTaskExecutor).execute(any(Runnable.class));

        Map<String, String> loggingContext = Map.of("key", "value");

        try (MockedStatic<MDC> mockedMdc = mockStatic(MDC.class)) {
            mockedMdc.when(MDC::getCopyOfContextMap).thenReturn(loggingContext);

            String response = service.triggerAddressFeed();

            assertEquals(EXECUTION_ID, response);

            verify(queryMapping).getQueryByName(anyString());
            verify(athenaService).fetchDataFromAthena(anyString());
            verify(asyncTaskExecutor).execute(any(Runnable.class));
            verify(athenaService, never()).processAndSkipHeaderOnce(anyList(), any(AddressContextDto.class));
            verify(athenaService, never()).mapRowsToClass(any(String[].class), anyList(), eq(AddressDto.class));
            verify(s3Service, never()).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));

            mockedMdc.verify(() -> MDC.setContextMap(loggingContext));
            mockedMdc.verify(MDC::clear);
        }
    }

    @Test
    void testTriggerAddressFeedFailure() {
        given(queryMapping.getQueryByName(anyString())).willReturn(LEGACY_ADDRESSES_QUERY_TEMPLATE);

        // stubbing athenaService spy method response
        willReturn(getResponsePair()).given(athenaService).fetchDataFromAthena(anyString());

        willAnswer((Answer<Void>) invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).given(asyncTaskExecutor).execute(any(Runnable.class));

        // stubbing athenaService spy method response
        willAnswer(invocation -> {
            throw new RuntimeException("Simulated failure");
        }).given(athenaService).processAndSkipHeaderOnce(anyList(), any(AddressContextDto.class));

        Map<String, String> loggingContext = Map.of("key", "value");

        try (MockedStatic<MDC> mockedMdc = mockStatic(MDC.class)) {
            mockedMdc.when(MDC::getCopyOfContextMap).thenReturn(loggingContext);

            String response = service.triggerAddressFeed();

            assertEquals(EXECUTION_ID, response);

            verify(queryMapping).getQueryByName(anyString());
            verify(athenaService).fetchDataFromAthena(anyString());
            verify(asyncTaskExecutor).execute(any(Runnable.class));
            verify(athenaService).processAndSkipHeaderOnce(anyList(), any(AddressContextDto.class));
            verify(athenaService, never()).mapRowsToClass(any(String[].class), anyList(), eq(AddressDto.class));
            verify(s3Service, never()).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));

            mockedMdc.verify(() -> MDC.setContextMap(loggingContext));
            mockedMdc.verify(MDC::clear);
        }
    }

    @Test
    void testWriteMapListToCsvFailure() throws NoSuchMethodException {
        List<Map<String, String>> mapList = List.of(Map.of("field1", "value1", "field2", "value2"));

        ByteArrayOutputStream outputStreamMock = mock(ByteArrayOutputStream.class);
        willThrow(new IOException("Failed to write")).given(outputStreamMock).write(any(byte[].class), anyInt(),
                anyInt());

        AddressContextDto context = AddressContextDto.builder().headerProcessed(false)
                .athenaColumnOrder(new String[]{"field1", "field2"}).executionId(EXECUTION_ID).headerWritten(false)
                .processedRecords(0).byteArrayOutputStream(new ByteArrayOutputStream()).build();
        context.setByteArrayOutputStream(outputStreamMock);

        // Use reflection to access the private method
        Method writeMapListToCsvMethod = AddressServiceImpl.class.getDeclaredMethod("writeMapListToCsv", List.class,
                AddressContextDto.class);
        writeMapListToCsvMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class,
                () -> writeMapListToCsvMethod.invoke(service, mapList, context));

        assertThat(exception).isInstanceOf(InvocationTargetException.class);
        Throwable rootCause = ((InvocationTargetException) exception).getTargetException();
        assertThat(rootCause).isInstanceOf(IllegalStateException.class);
        assertEquals("Error: 'Failed to write data to file'. Execution ID: '" + EXECUTION_ID
                + "'. Root Cause: 'Failed to write'.", rootCause.getMessage());
    }

    @Test
    void testS3UploadFailure() throws NoSuchMethodException, IOException {
        willThrow(S3Exception.builder().message("S3 upload failed").build()).given(s3Service)
                .multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));

        // Use reflection to access the private method
        Method uploadFileToS3 = AddressServiceImpl.class.getDeclaredMethod("uploadFileToS3", AddressContextDto.class);
        uploadFileToS3.setAccessible(true);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("some test data".getBytes(StandardCharsets.UTF_8));

        AddressContextDto context = AddressContextDto.builder().headerProcessed(true)
                .athenaColumnOrder(new String[]{"field1", "field2"}).executionId(EXECUTION_ID).headerWritten(true)
                .processedRecords(2).byteArrayOutputStream(baos).build();

        Exception exception = assertThrows(Exception.class, () -> uploadFileToS3.invoke(service, context));

        assertThat(exception).isInstanceOf(InvocationTargetException.class);
        Throwable rootCause = ((InvocationTargetException) exception).getTargetException();
        assertThat(rootCause).isInstanceOf(S3Exception.class);
        assertEquals("S3 upload failed", rootCause.getMessage());
    }
}
