package com.erebelo.springdataaws.service;

import static com.erebelo.springdataaws.mock.AddressMock.EXECUTION_ID;
import static com.erebelo.springdataaws.mock.AddressMock.getRowsChunk1;
import static com.erebelo.springdataaws.mock.AddressMock.getRowsChunk2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.erebelo.springdataaws.domain.dto.AddressContextDto;
import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import com.erebelo.springdataaws.exception.model.BadRequestException;
import com.erebelo.springdataaws.service.impl.AddressServiceImpl;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.slf4j.MDC;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.ResultSet;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
class AddressServiceImplTest {

    @InjectMocks
    private AddressServiceImpl addressService;

    @Mock
    private AthenaService athenaService;

    @Mock
    private S3Service s3Service;

    @Mock
    private Executor asyncTaskExecutor;

    @Test
    void testAddressFeedTriggerWith2KRecordsSuccessful() throws InterruptedException {
        when(athenaService.submitAthenaQuery(anyString())).thenReturn(AthenaQueryDto.builder().executionId(EXECUTION_ID).build());
        doNothing().when(athenaService).waitForQueryToComplete(anyString());
        when(athenaService.getQueryResults(anyString()))
                .thenReturn(List.of(
                        GetQueryResultsResponse.builder()
                                .resultSet(ResultSet.builder().rows(getRowsChunk1()).build())
                                .build(),
                        GetQueryResultsResponse.builder()
                                .resultSet(ResultSet.builder().rows(getRowsChunk2()).build())
                                .build()
                ));

        doAnswer((Answer<Void>) invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(asyncTaskExecutor).execute(any(Runnable.class));

        doNothing().when(s3Service).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));

        Map<String, String> loggingContext = Map.of("key", "value");
        try (MockedStatic<MDC> mockedMdc = mockStatic(MDC.class)) {
            mockedMdc.when(MDC::getCopyOfContextMap).thenReturn(loggingContext);

            String result = addressService.addressFeedTrigger();

            assertThat(result).isEqualTo(EXECUTION_ID);

            mockedMdc.verify(() -> MDC.setContextMap(loggingContext));
            mockedMdc.verify(MDC::clear);
            verify(athenaService).submitAthenaQuery(anyString());
            verify(athenaService).waitForQueryToComplete(anyString());
            verify(athenaService).getQueryResults(anyString());
            verify(asyncTaskExecutor).execute(any(Runnable.class));
            verify(s3Service).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));
        }
    }

    @Test
    void testAddressFeedTriggerSubmitAthenaQueryFailure() throws InterruptedException {
        when(athenaService.submitAthenaQuery(anyString())).thenThrow(new AthenaQueryException("Athena query failed"));

        BadRequestException exception = assertThrows(BadRequestException.class,
                () -> addressService.addressFeedTrigger());
        assertThat(exception.getMessage()).isEqualTo("Error: 'Failed to trigger address feed'. " +
                "Execution ID: 'null'. Root Cause: 'Athena query failed'.");

        verify(athenaService).submitAthenaQuery(anyString());
        verify(athenaService, never()).waitForQueryToComplete(anyString());
        verify(athenaService, never()).getQueryResults(anyString());
        verify(asyncTaskExecutor, never()).execute(any(Runnable.class));
        verify(s3Service, never()).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));
    }

    @Test
    void testAddressFeedTriggerWaitForQueryFailure() throws InterruptedException {
        when(athenaService.submitAthenaQuery(anyString())).thenReturn(AthenaQueryDto.builder().executionId(EXECUTION_ID).build());
        doThrow(new AthenaQueryException("Athena query failed")).when(athenaService).waitForQueryToComplete(anyString());

        BadRequestException exception = assertThrows(BadRequestException.class,
                () -> addressService.addressFeedTrigger());
        assertThat(exception.getMessage()).isEqualTo("Error: 'Failed to trigger address feed'. " +
                "Execution ID: '" + EXECUTION_ID + "'. Root Cause: 'Athena query failed'.");

        verify(athenaService).submitAthenaQuery(anyString());
        verify(athenaService).waitForQueryToComplete(anyString());
        verify(athenaService, never()).getQueryResults(anyString());
        verify(asyncTaskExecutor, never()).execute(any(Runnable.class));
        verify(s3Service, never()).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));
    }

    @Test
    void testAddressFeedTriggerGetQueryResultsFailure() throws InterruptedException {
        when(athenaService.submitAthenaQuery(anyString())).thenReturn(AthenaQueryDto.builder().executionId(EXECUTION_ID).build());
        doNothing().when(athenaService).waitForQueryToComplete(anyString());
        when(athenaService.getQueryResults(anyString()))
                .thenThrow(new AthenaQueryException("Failed to get query results"));

        BadRequestException exception = assertThrows(BadRequestException.class,
                () -> addressService.addressFeedTrigger());
        assertThat(exception.getMessage()).isEqualTo("Error: 'Failed to trigger address feed'. " +
                "Execution ID: '" + EXECUTION_ID + "'. Root Cause: 'Failed to get query results'.");

        verify(athenaService).submitAthenaQuery(anyString());
        verify(athenaService).waitForQueryToComplete(anyString());
        verify(athenaService).getQueryResults(anyString());
        verify(asyncTaskExecutor, never()).execute(any(Runnable.class));
        verify(s3Service, never()).multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));
    }

    @Test
    void testBuildAddressMapFromRowFailure() throws Exception {
        Row rowMock = mock(Row.class);

        // Mock the first Datum object to return a valid recordId
        Datum recordIdDatumMock = mock(Datum.class);
        when(recordIdDatumMock.varCharValue()).thenReturn("1");

        when(rowMock.data()).thenThrow(new RuntimeException("Simulated exception")) // First call: throw exception
                .thenReturn(List.of(recordIdDatumMock)); // Second call: return valid data

        // Use reflection to access the private method
        Method buildAddressMapFromRowMethod = AddressServiceImpl.class.getDeclaredMethod("buildAddressMapFromRow",
                Row.class, AddressContextDto.class);
        buildAddressMapFromRowMethod.setAccessible(true);

        Object result = buildAddressMapFromRowMethod.invoke(addressService, rowMock,
                new AddressContextDto(EXECUTION_ID, 0, new ByteArrayOutputStream()));

        assertThat(result).isInstanceOf(Map.class).asInstanceOf(InstanceOfAssertFactories.MAP)
                .isInstanceOf(LinkedHashMap.class).isEmpty();
    }

    @Test
    void testBuildAddressMapFromRowCatchFailure() throws Exception {
        Row rowMock = mock(Row.class);
        when(rowMock.data()).thenThrow(new RuntimeException("Simulated exception"));

        // Use reflection to access the private method
        Method buildAddressMapFromRowMethod = AddressServiceImpl.class.getDeclaredMethod("buildAddressMapFromRow",
                Row.class, AddressContextDto.class);
        buildAddressMapFromRowMethod.setAccessible(true);

        Object result = buildAddressMapFromRowMethod.invoke(addressService, rowMock,
                new AddressContextDto(EXECUTION_ID, 0, new ByteArrayOutputStream()));

        assertThat(result).isInstanceOf(Map.class).asInstanceOf(InstanceOfAssertFactories.MAP)
                .isInstanceOf(LinkedHashMap.class).isEmpty();
    }

    @Test
    void testWriteAddressesToCsvFailure() throws NoSuchMethodException {
        List<Map<String, String>> addressMapList = List.of(Map.of("field1", "value1", "field2", "value2"));

        ByteArrayOutputStream outputStreamMock = mock(ByteArrayOutputStream.class);
        doThrow(new IOException("Failed to write")).when(outputStreamMock).write(any(byte[].class), anyInt(), anyInt());

        AddressContextDto context = new AddressContextDto(EXECUTION_ID, 0, new ByteArrayOutputStream());
        context.setByteArrayOutputStream(outputStreamMock);

        // Use reflection to access the private method
        Method writeAddressesToCsvMethod = AddressServiceImpl.class.getDeclaredMethod("writeAddressesToCsv", List.class,
                AddressContextDto.class);
        writeAddressesToCsvMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class,
                () -> writeAddressesToCsvMethod.invoke(addressService, addressMapList, context));

        assertThat(exception).isInstanceOf(InvocationTargetException.class);
        Throwable rootCause = ((InvocationTargetException) exception).getTargetException();
        assertThat(rootCause).isInstanceOf(BadRequestException.class);
        assertThat(rootCause.getMessage()).isEqualTo("Error: 'Failed to write address data to file'. "
                + "Execution ID: '" + EXECUTION_ID + "'. Root Cause: 'Failed to write'.");
    }

    @Test
    void testS3UploadFailure() throws NoSuchMethodException {
        doThrow(S3Exception.builder().message("S3 upload failed").build()).when(s3Service).multipartUpload(anyString(),
                anyString(), anyString(), any(byte[].class));

        // Use reflection to access the private method
        Method uploadFileToS3 = AddressServiceImpl.class.getDeclaredMethod("uploadFileToS3", AddressContextDto.class);
        uploadFileToS3.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () -> uploadFileToS3.invoke(addressService,
                new AddressContextDto(EXECUTION_ID, 0, new ByteArrayOutputStream())));

        assertThat(exception).isInstanceOf(InvocationTargetException.class);
        Throwable rootCause = ((InvocationTargetException) exception).getTargetException();
        assertThat(rootCause).isInstanceOf(BadRequestException.class);
        assertThat(rootCause.getMessage()).isEqualTo("Error: 'Failed to upload in-memory address file to S3'. "
                + "Execution ID: '" + EXECUTION_ID + "'. Root Cause: 'S3 upload failed'.");
    }
}
