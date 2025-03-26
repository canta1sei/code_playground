package com.example.s3api.service;

import com.example.s3api.dto.BucketInfo;
import com.example.s3api.dto.FileMetadata;
import com.example.s3api.exception.S3ApiException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.http.SdkHttpRequest;
import org.springframework.web.multipart.MultipartFile;

import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3ServiceTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private S3Presigner s3Presigner;

    @Mock
    private S3Utilities s3Utilities;

    private S3Service s3Service;

    @BeforeEach
    void setUp() {
        s3Service = new S3Service(s3Client, s3Presigner);
        when(s3Client.utilities()).thenReturn(s3Utilities);
    }

    @Test
    void listFiles_Success() {
        String bucketName = "test-bucket";
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(
                        S3Object.builder().key("file1.txt").build(),
                        S3Object.builder().key("file2.txt").build()
                )
                .build();
        when(s3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        List<String> files = s3Service.listFiles(bucketName);

        assertEquals(2, files.size());
        assertTrue(files.contains("file1.txt"));
        assertTrue(files.contains("file2.txt"));
        verify(s3Client).listObjects(any(ListObjectsRequest.class));
    }

    @Test
    void listFiles_BucketNotFound() {
        // Arrange
        String bucketName = "non-existent-bucket";
        when(s3Client.listObjects(any(ListObjectsRequest.class)))
                .thenThrow(NoSuchBucketException.builder().build());

        // Act & Assert
        S3ApiException exception = assertThrows(S3ApiException.class,
                () -> s3Service.listFiles(bucketName));
        assertEquals("BUCKET_NOT_FOUND", exception.getErrorCode());
        verify(s3Client).listObjects(any(ListObjectsRequest.class));
    }

    @Test
    void uploadFile_Success() throws Exception {
        String bucketName = "test-bucket";
        String key = "test.txt";
        MultipartFile file = mock(MultipartFile.class);
        when(file.getOriginalFilename()).thenReturn(key);
        when(file.getBytes()).thenReturn("test content".getBytes());
        Map<String, String> metadata = Map.of("test-key", "test-value");
        
        URL presignedUrl = new URL("https://test-bucket.s3.amazonaws.com/test.txt");
        when(s3Utilities.getUrl(any(GetUrlRequest.class))).thenReturn(presignedUrl);
        when(s3Presigner.presignPutObject(any(PutObjectPresignRequest.class)))
                .thenReturn(PresignedPutObjectRequest.builder()
                        .expiration(java.time.Instant.now().plusSeconds(3600))
                        .isBrowserExecutable(true)
                        .build());

        FileMetadata result = s3Service.uploadFile(bucketName, key, file, metadata);

        assertNotNull(result);
        assertEquals(key, result.getKey());
        assertEquals(presignedUrl.toString(), result.getUrl());
        verify(s3Presigner).presignPutObject(any(PutObjectPresignRequest.class));
        verify(s3Utilities).getUrl(any(GetUrlRequest.class));
    }

    @Test
    void createBucket_Success() {
        String bucketName = "test-bucket";
        String region = "ap-northeast-1";
        CreateBucketResponse response = CreateBucketResponse.builder().build();
        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(response);

        BucketInfo result = s3Service.createBucket(bucketName, region);

        assertEquals(bucketName, result.getName());
        assertEquals(region, result.getRegion());
        verify(s3Client).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    void createBucket_AlreadyExists() {
        // Arrange
        String bucketName = "existing-bucket";
        String region = "ap-northeast-1";

        when(s3Client.createBucket(any(CreateBucketRequest.class)))
                .thenThrow(BucketAlreadyExistsException.builder().build());

        // Act & Assert
        S3ApiException exception = assertThrows(S3ApiException.class,
                () -> s3Service.createBucket(bucketName, region));
        assertEquals("BUCKET_ALREADY_EXISTS", exception.getErrorCode());
        verify(s3Client).createBucket(any(CreateBucketRequest.class));
    }
} 