package com.example.s3api.dto;

import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.Map;

@Getter
@Builder
public class FileMetadata {
    private final String key;
    private final String bucketName;
    private final long size;
    private final String contentType;
    private final LocalDateTime lastModified;
    private final String etag;
    private final Map<String, String> userMetadata;
    private final String url;
} 