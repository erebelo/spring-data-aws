// package com.erebelo.springdataaws.service;
//
// import static com.erebelo.springdataaws.mock.AddressMock.EXECUTION_ID;
// import static com.erebelo.springdataaws.mock.AddressMock.getRowsChunk1;
// import static com.erebelo.springdataaws.mock.AddressMock.getRowsChunk2;
// import static org.assertj.core.api.Assertions.assertThat;
// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertThrows;
// import static org.mockito.ArgumentMatchers.any;
// import static org.mockito.ArgumentMatchers.anyInt;
// import static org.mockito.ArgumentMatchers.anyString;
// import static org.mockito.BDDMockito.given;
// import static org.mockito.BDDMockito.willAnswer;
// import static org.mockito.BDDMockito.willDoNothing;
// import static org.mockito.BDDMockito.willThrow;
// import static org.mockito.Mockito.mock;
// import static org.mockito.Mockito.mockStatic;
// import static org.mockito.Mockito.never;
// import static org.mockito.Mockito.verify;
//
// import com.erebelo.springdataaws.domain.dto.AddressContextDto;
// import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
// import com.erebelo.springdataaws.exception.model.AthenaQueryException;
// import com.erebelo.springdataaws.exception.model.BadRequestException;
// import com.erebelo.springdataaws.service.impl.AddressServiceImpl;
// import java.io.ByteArrayOutputStream;
// import java.io.IOException;
// import java.lang.reflect.InvocationTargetException;
// import java.lang.reflect.Method;
// import java.util.LinkedHashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.Executor;
// import org.assertj.core.api.InstanceOfAssertFactories;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.mockito.InjectMocks;
// import org.mockito.Mock;
// import org.mockito.MockedStatic;
// import org.mockito.junit.jupiter.MockitoExtension;
// import org.mockito.stubbing.Answer;
// import org.slf4j.MDC;
// import software.amazon.awssdk.services.athena.model.Datum;
// import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
// import software.amazon.awssdk.services.athena.model.ResultSet;
// import software.amazon.awssdk.services.athena.model.Row;
// import software.amazon.awssdk.services.s3.model.S3Exception;
//
// @ExtendWith(MockitoExtension.class)
// class AddressServiceTest {
//
// @InjectMocks
// private AddressServiceImpl addressService;
//
// @Mock
// private AthenaService athenaService;
//
// @Mock
// private S3Service s3Service;
//
// @Mock
// private Executor asyncTaskExecutor;
//
// @Test
// void testAddressFeedTriggerWith2KRecordsSuccessful() throws
// InterruptedException {
// given(athenaService.submitAthenaQuery(anyString()))
// .willReturn(AthenaQueryDto.builder().executionId(EXECUTION_ID).build());
// willDoNothing().given(athenaService).waitForQueryToComplete(anyString());
// given(athenaService.getQueryResults(anyString())).willReturn(List.of(
// GetQueryResultsResponse.builder().resultSet(ResultSet.builder().rows(getRowsChunk1()).build()).build(),
// GetQueryResultsResponse.builder().resultSet(ResultSet.builder().rows(getRowsChunk2()).build())
// .build()));
//
// willAnswer((Answer<Void>) invocation -> {
// ((Runnable) invocation.getArgument(0)).run();
// return null;
// }).given(asyncTaskExecutor).execute(any(Runnable.class));
//
// willDoNothing().given(s3Service).multipartUpload(anyString(), anyString(),
// anyString(), any(byte[].class));
//
// Map<String, String> loggingContext = Map.of("key", "value");
// try (MockedStatic<MDC> mockedMdc = mockStatic(MDC.class)) {
// mockedMdc.when(MDC::getCopyOfContextMap).thenReturn(loggingContext);
//
// String response = addressService.addressFeedTrigger();
//
// assertEquals(EXECUTION_ID, response);
//
// mockedMdc.verify(() -> MDC.setContextMap(loggingContext));
// mockedMdc.verify(MDC::clear);
// verify(athenaService).submitAthenaQuery(anyString());
// verify(athenaService).waitForQueryToComplete(anyString());
// verify(athenaService).getQueryResults(anyString());
// verify(asyncTaskExecutor).execute(any(Runnable.class));
// verify(s3Service).multipartUpload(anyString(), anyString(), anyString(),
// any(byte[].class));
// }
// }
//
// @Test
// void testAddressFeedTriggerSubmitAthenaQueryFailure() throws
// InterruptedException {
// given(athenaService.submitAthenaQuery(anyString())).willThrow(new
// AthenaQueryException("Athena query failed"));
//
// BadRequestException exception = assertThrows(BadRequestException.class,
// () -> addressService.addressFeedTrigger());
//
// assertEquals("Error: 'Failed to trigger address feed'. Execution ID: 'null'.
// Root Cause: 'Athena query "
// + "failed'.", exception.getMessage());
//
// verify(athenaService).submitAthenaQuery(anyString());
// verify(athenaService, never()).waitForQueryToComplete(anyString());
// verify(athenaService, never()).getQueryResults(anyString());
// verify(asyncTaskExecutor, never()).execute(any(Runnable.class));
// verify(s3Service, never()).multipartUpload(anyString(), anyString(),
// anyString(), any(byte[].class));
// }
//
// @Test
// void testAddressFeedTriggerWaitForQueryFailure() throws InterruptedException
// {
// given(athenaService.submitAthenaQuery(anyString()))
// .willReturn(AthenaQueryDto.builder().executionId(EXECUTION_ID).build());
// willThrow(new InterruptedException("Thread
// interrupted")).given(athenaService)
// .waitForQueryToComplete(anyString());
//
// BadRequestException exception = assertThrows(BadRequestException.class,
// () -> addressService.addressFeedTrigger());
//
// assertEquals("Error: 'Thread was interrupted while triggering address feed'.
// Execution ID: '" + EXECUTION_ID
// + "'. Root Cause: 'Thread interrupted'.", exception.getMessage());
//
// assertThat(Thread.currentThread().isInterrupted()).isTrue();
//
// verify(athenaService).submitAthenaQuery(anyString());
// verify(athenaService).waitForQueryToComplete(anyString());
// verify(athenaService, never()).getQueryResults(anyString());
// verify(asyncTaskExecutor, never()).execute(any(Runnable.class));
// verify(s3Service, never()).multipartUpload(anyString(), anyString(),
// anyString(), any(byte[].class));
// }
//
// @Test
// void testAddressFeedTriggerGetQueryResultsFailure() throws
// InterruptedException {
// given(athenaService.submitAthenaQuery(anyString()))
// .willReturn(AthenaQueryDto.builder().executionId(EXECUTION_ID).build());
// willDoNothing().given(athenaService).waitForQueryToComplete(anyString());
// given(athenaService.getQueryResults(anyString()))
// .willThrow(new AthenaQueryException("Failed to get query results"));
//
// BadRequestException exception = assertThrows(BadRequestException.class,
// () -> addressService.addressFeedTrigger());
//
// assertEquals("Error: 'Failed to trigger address feed'. " + "Execution ID: '"
// + EXECUTION_ID
// + "'. Root Cause: 'Failed to get query results'.", exception.getMessage());
//
// verify(athenaService).submitAthenaQuery(anyString());
// verify(athenaService).waitForQueryToComplete(anyString());
// verify(athenaService).getQueryResults(anyString());
// verify(asyncTaskExecutor, never()).execute(any(Runnable.class));
// verify(s3Service, never()).multipartUpload(anyString(), anyString(),
// anyString(), any(byte[].class));
// }
//
// @Test
// void testBuildAddressMapFromRowFailure() throws Exception {
// Row rowMock = mock(Row.class);
//
// // Mock the first Datum object to return a valid recordId
// Datum recordIdDatumMock = mock(Datum.class);
// given(recordIdDatumMock.varCharValue()).willReturn("1");
//
// given(rowMock.data()).willThrow(new RuntimeException("Simulated exception"))
// // First call: throw exception
// .willReturn(List.of(recordIdDatumMock)); // Second call: return valid data
//
// // Use reflection to access the private method
// Method buildAddressMapFromRowMethod =
// AddressServiceImpl.class.getDeclaredMethod("buildAddressMapFromRow",
// Row.class, AddressContextDto.class);
// buildAddressMapFromRowMethod.setAccessible(true);
//
// Object response = buildAddressMapFromRowMethod.invoke(addressService,
// rowMock,
// new AddressContextDto(EXECUTION_ID, 0, new ByteArrayOutputStream()));
//
// assertThat(response).isInstanceOf(Map.class).asInstanceOf(InstanceOfAssertFactories.MAP)
// .isInstanceOf(LinkedHashMap.class).isEmpty();
// }
//
// @Test
// void testBuildAddressMapFromRowCatchFailure() throws Exception {
// Row rowMock = mock(Row.class);
// given(rowMock.data()).willThrow(new RuntimeException("Simulated exception"));
//
// // Use reflection to access the private method
// Method buildAddressMapFromRowMethod =
// AddressServiceImpl.class.getDeclaredMethod("buildAddressMapFromRow",
// Row.class, AddressContextDto.class);
// buildAddressMapFromRowMethod.setAccessible(true);
//
// Object response = buildAddressMapFromRowMethod.invoke(addressService,
// rowMock,
// new AddressContextDto(EXECUTION_ID, 0, new ByteArrayOutputStream()));
//
// assertThat(response).isInstanceOf(Map.class).asInstanceOf(InstanceOfAssertFactories.MAP)
// .isInstanceOf(LinkedHashMap.class).isEmpty();
// }
//
// @Test
// void testWriteAddressesToCsvFailure() throws NoSuchMethodException {
// List<Map<String, String>> addressMapList = List.of(Map.of("field1", "value1",
// "field2", "value2"));
//
// ByteArrayOutputStream outputStreamMock = mock(ByteArrayOutputStream.class);
// willThrow(new IOException("Failed to
// write")).given(outputStreamMock).write(any(byte[].class), anyInt(),
// anyInt());
//
// AddressContextDto context = new AddressContextDto(EXECUTION_ID, 0, new
// ByteArrayOutputStream());
// context.setByteArrayOutputStream(outputStreamMock);
//
// // Use reflection to access the private method
// Method writeAddressesToCsvMethod =
// AddressServiceImpl.class.getDeclaredMethod("writeAddressesToCsv", List.class,
// AddressContextDto.class);
// writeAddressesToCsvMethod.setAccessible(true);
//
// Exception exception = assertThrows(Exception.class,
// () -> writeAddressesToCsvMethod.invoke(addressService, addressMapList,
// context));
//
// assertThat(exception).isInstanceOf(InvocationTargetException.class);
// Throwable rootCause = ((InvocationTargetException)
// exception).getTargetException();
// assertThat(rootCause).isInstanceOf(BadRequestException.class);
// assertEquals("Error: 'Failed to write address data to file'. Execution ID: '"
// + EXECUTION_ID
// + "'. Root Cause: 'Failed to write'.", rootCause.getMessage());
// }
//
// @Test
// void testS3UploadFailure() throws NoSuchMethodException {
// willThrow(S3Exception.builder().message("S3 upload
// failed").build()).given(s3Service)
// .multipartUpload(anyString(), anyString(), anyString(), any(byte[].class));
//
// // Use reflection to access the private method
// Method uploadFileToS3 =
// AddressServiceImpl.class.getDeclaredMethod("uploadFileToS3",
// AddressContextDto.class);
// uploadFileToS3.setAccessible(true);
//
// Exception exception = assertThrows(Exception.class, () ->
// uploadFileToS3.invoke(addressService,
// new AddressContextDto(EXECUTION_ID, 0, new ByteArrayOutputStream())));
//
// assertThat(exception).isInstanceOf(InvocationTargetException.class);
// Throwable rootCause = ((InvocationTargetException)
// exception).getTargetException();
// assertThat(rootCause).isInstanceOf(BadRequestException.class);
// assertEquals("Error: 'Failed to upload in-memory address file to S3'. " +
// "Execution ID: '" + EXECUTION_ID
// + "'. Root Cause: 'S3 upload failed'.", rootCause.getMessage());
// }
// }
