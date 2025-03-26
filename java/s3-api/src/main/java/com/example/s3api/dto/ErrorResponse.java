package com.example.s3api.dto;

import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class ErrorResponse {
    private final LocalDateTime timestamp;
    private final String errorCode;
    private final String message;
    private final String details;
    private final String path;
} 