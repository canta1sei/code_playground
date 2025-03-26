package com.example.s3api.service;

import com.example.s3api.dto.BucketInfo;
import com.example.s3api.dto.FileMetadata;
import com.example.s3api.exception.S3ApiException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class S3Service {

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;

    public FileMetadata uploadFile(String bucketName, String key, MultipartFile file, Map<String, String> userMetadata) throws IOException {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("Content-Type", file.getContentType());
            metadata.put("Original-Filename", file.getOriginalFilename());
            if (userMetadata != null) {
                metadata.putAll(userMetadata);
            }

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(file.getContentType())
                    .metadata(metadata)
                    .build();

            PutObjectResponse response = s3Client.putObject(request, RequestBody.fromInputStream(file.getInputStream(), file.getSize()));
            
            return FileMetadata.builder()
                    .key(key)
                    .bucketName(bucketName)
                    .size(file.getSize())
                    .contentType(file.getContentType())
                    .lastModified(LocalDateTime.now())
                    .etag(response.eTag())
                    .userMetadata(metadata)
                    .url(generatePresignedUrl(bucketName, key))
                    .build();
        } catch (S3Exception e) {
            log.error("Failed to upload file to S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to upload file to S3",
                    "S3_UPLOAD_ERROR",
                    e.getMessage()
            );
        }
    }

    public FileMetadata getFileMetadata(String bucketName, String key) {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            HeadObjectResponse response = s3Client.headObject(request);
            
            return FileMetadata.builder()
                    .key(key)
                    .bucketName(bucketName)
                    .size(response.contentLength())
                    .contentType(response.contentType())
                    .lastModified(LocalDateTime.ofInstant(
                            response.lastModified(),
                            ZoneId.systemDefault()))
                    .etag(response.eTag())
                    .userMetadata(response.metadata())
                    .url(generatePresignedUrl(bucketName, key))
                    .build();
        } catch (NoSuchKeyException e) {
            log.error("File not found in S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "File not found in S3",
                    "FILE_NOT_FOUND",
                    "File with key: " + key + " not found in bucket: " + bucketName
            );
        } catch (S3Exception e) {
            log.error("Failed to get file metadata from S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to get file metadata from S3",
                    "S3_METADATA_ERROR",
                    e.getMessage()
            );
        }
    }

    public void updateFileMetadata(String bucketName, String key, Map<String, String> userMetadata) {
        try {
            // 既存のオブジェクトのメタデータを取得
            HeadObjectResponse currentMetadata = s3Client.headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );

            // 新しいメタデータを設定
            CopyObjectRequest request = CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(key)
                    .destinationBucket(bucketName)
                    .destinationKey(key)
                    .metadata(userMetadata)
                    .metadataDirective(MetadataDirective.REPLACE)
                    .build();

            s3Client.copyObject(request);
        } catch (NoSuchKeyException e) {
            log.error("File not found in S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "File not found in S3",
                    "FILE_NOT_FOUND",
                    "File with key: " + key + " not found in bucket: " + bucketName
            );
        } catch (S3Exception e) {
            log.error("Failed to update file metadata in S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to update file metadata in S3",
                    "S3_METADATA_UPDATE_ERROR",
                    e.getMessage()
            );
        }
    }

    private String generatePresignedUrl(String bucketName, String key) {
        try {
            GetUrlRequest request = GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            return s3Client.utilities().getUrl(request).toString();
        } catch (S3Exception e) {
            log.error("Failed to generate presigned URL: {}", e.getMessage(), e);
            return null;
        }
    }

    public byte[] downloadFile(String bucketName, String key) {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            return s3Client.getObject(request).readAllBytes();
        } catch (NoSuchKeyException e) {
            log.error("File not found in S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "File not found in S3",
                    "FILE_NOT_FOUND",
                    "File with key: " + key + " not found in bucket: " + bucketName
            );
        } catch (IOException e) {
            log.error("Failed to read file from S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to read file from S3",
                    "FILE_READ_ERROR",
                    e.getMessage()
            );
        } catch (S3Exception e) {
            log.error("Failed to download file from S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to download file from S3",
                    "S3_DOWNLOAD_ERROR",
                    e.getMessage()
            );
        }
    }

    public void deleteFile(String bucketName, String key) {
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3Client.deleteObject(request);
        } catch (NoSuchKeyException e) {
            log.error("File not found in S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "File not found in S3",
                    "FILE_NOT_FOUND",
                    "File with key: " + key + " not found in bucket: " + bucketName
            );
        } catch (S3Exception e) {
            log.error("Failed to delete file from S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to delete file from S3",
                    "S3_DELETE_ERROR",
                    e.getMessage()
            );
        }
    }

    public List<String> listFiles(String bucketName) {
        try {
            ListObjectsRequest request = ListObjectsRequest.builder()
                    .bucket(bucketName)
                    .build();

            return s3Client.listObjects(request)
                    .contents()
                    .stream()
                    .map(S3Object::key)
                    .collect(Collectors.toList());
        } catch (NoSuchBucketException e) {
            log.error("Bucket not found: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Bucket not found",
                    "BUCKET_NOT_FOUND",
                    "Bucket: " + bucketName + " not found"
            );
        } catch (S3Exception e) {
            log.error("Failed to list files from S3: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to list files from S3",
                    "S3_LIST_ERROR",
                    e.getMessage()
            );
        }
    }

    public BucketInfo createBucket(String bucketName, String region) {
        try {
            CreateBucketRequest request = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(CreateBucketConfiguration.builder()
                            .locationConstraint(region)
                            .build())
                    .build();

            CreateBucketResponse response = s3Client.createBucket(request);
            return BucketInfo.builder()
                    .name(bucketName)
                    .creationDate(LocalDateTime.now())
                    .region(region)
                    .totalSize(0)
                    .objectCount(0)
                    .build();
        } catch (BucketAlreadyExistsException e) {
            log.error("Bucket already exists: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Bucket already exists",
                    "BUCKET_ALREADY_EXISTS",
                    "Bucket: " + bucketName + " already exists"
            );
        } catch (S3Exception e) {
            log.error("Failed to create bucket: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to create bucket",
                    "BUCKET_CREATE_ERROR",
                    e.getMessage()
            );
        }
    }

    public void deleteBucket(String bucketName) {
        try {
            // バケット内のオブジェクトを削除
            ListObjectsRequest listRequest = ListObjectsRequest.builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse listResponse = s3Client.listObjects(listRequest);
            if (listResponse.hasContents()) {
                List<ObjectIdentifier> objects = listResponse.contents().stream()
                        .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                        .collect(Collectors.toList());

                DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(Delete.builder().objects(objects).build())
                        .build();

                s3Client.deleteObjects(deleteRequest);
            }

            // バケットを削除
            DeleteBucketRequest request = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.deleteBucket(request);
        } catch (NoSuchBucketException e) {
            log.error("Bucket not found: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Bucket not found",
                    "BUCKET_NOT_FOUND",
                    "Bucket: " + bucketName + " not found"
            );
        } catch (S3Exception e) {
            log.error("Failed to delete bucket: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to delete bucket",
                    "BUCKET_DELETE_ERROR",
                    e.getMessage()
            );
        }
    }

    public List<BucketInfo> listBuckets() {
        try {
            ListBucketsResponse response = s3Client.listBuckets();
            return response.buckets().stream()
                    .map(bucket -> {
                        try {
                            GetBucketLocationResponse locationResponse = s3Client.getBucketLocation(
                                    GetBucketLocationRequest.builder()
                                            .bucket(bucket.name())
                                            .build()
                            );
                            String region = locationResponse.locationConstraintAsString();
                            
                            // バケットの統計情報を取得
                            ListObjectsResponse objectsResponse = s3Client.listObjects(
                                    ListObjectsRequest.builder()
                                            .bucket(bucket.name())
                                            .build()
                            );
                            
                            long totalSize = objectsResponse.contents().stream()
                                    .mapToLong(S3Object::size)
                                    .sum();
                            
                            return BucketInfo.builder()
                                    .name(bucket.name())
                                    .creationDate(LocalDateTime.ofInstant(
                                            bucket.creationDate(),
                                            ZoneId.systemDefault()))
                                    .region(region)
                                    .totalSize(totalSize)
                                    .objectCount(objectsResponse.contents().size())
                                    .build();
                        } catch (S3Exception e) {
                            log.error("Failed to get bucket details: {}", e.getMessage(), e);
                            return BucketInfo.builder()
                                    .name(bucket.name())
                                    .creationDate(LocalDateTime.ofInstant(
                                            bucket.creationDate(),
                                            ZoneId.systemDefault()))
                                    .region("unknown")
                                    .totalSize(0)
                                    .objectCount(0)
                                    .build();
                        }
                    })
                    .collect(Collectors.toList());
        } catch (S3Exception e) {
            log.error("Failed to list buckets: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to list buckets",
                    "BUCKET_LIST_ERROR",
                    e.getMessage()
            );
        }
    }

    public String generatePresignedUrl(String bucketName, String key, int expirationMinutes) {
        try {
            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(expirationMinutes))
                    .getObjectRequest(req -> req.bucket(bucketName).key(key))
                    .build();

            PresignedGetObjectRequest presignedRequest = s3Presigner.presignGetObject(presignRequest);
            return presignedRequest.url().toString();
        } catch (S3Exception e) {
            log.error("Failed to generate presigned URL: {}", e.getMessage(), e);
            throw new S3ApiException(
                    "Failed to generate presigned URL",
                    "PRESIGNED_URL_ERROR",
                    e.getMessage()
            );
        }
    }

    public boolean isPreviewable(String contentType) {
        if (contentType == null) {
            return false;
        }
        return contentType.startsWith("image/") ||
                contentType.startsWith("video/") ||
                contentType.startsWith("audio/") ||
                contentType.equals("application/pdf") ||
                contentType.equals("text/plain") ||
                contentType.equals("text/html");
    }
} 