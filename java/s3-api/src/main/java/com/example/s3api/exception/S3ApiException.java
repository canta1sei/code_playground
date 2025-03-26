package com.example.s3api.exception;

import lombok.Getter;

@Getter
public class S3ApiException extends RuntimeException {
    private final String errorCode;
    private final String details;

    public S3ApiException(String message, String errorCode, String details) {
        super(message);
        this.errorCode = errorCode;
        this.details = details;
    }

    public S3ApiException(String message, String errorCode) {
        this(message, errorCode, null);
    }
} 