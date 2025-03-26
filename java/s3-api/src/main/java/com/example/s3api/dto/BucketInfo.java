package com.example.s3api.dto;

import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class BucketInfo {
    private final String name;
    private final LocalDateTime creationDate;
    private final String region;
    private final long totalSize;
    private final int objectCount;
} 