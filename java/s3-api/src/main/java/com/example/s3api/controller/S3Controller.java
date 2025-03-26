package com.example.s3api.controller;

import com.example.s3api.dto.BucketInfo;
import com.example.s3api.dto.FileMetadata;
import com.example.s3api.service.S3Service;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/s3")
@RequiredArgsConstructor
public class S3Controller {

    private final S3Service s3Service;

    @PostMapping("/upload")
    public ResponseEntity<FileMetadata> uploadFile(
            @RequestParam("bucket") String bucketName,
            @RequestParam("key") String key,
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "metadata", required = false) Map<String, String> metadata) throws IOException {
        FileMetadata fileMetadata = s3Service.uploadFile(bucketName, key, file, metadata);
        return ResponseEntity.ok(fileMetadata);
    }

    @GetMapping("/metadata")
    public ResponseEntity<FileMetadata> getFileMetadata(
            @RequestParam("bucket") String bucketName,
            @RequestParam("key") String key) {
        FileMetadata metadata = s3Service.getFileMetadata(bucketName, key);
        return ResponseEntity.ok(metadata);
    }

    @PutMapping("/metadata")
    public ResponseEntity<Void> updateFileMetadata(
            @RequestParam("bucket") String bucketName,
            @RequestParam("key") String key,
            @RequestBody Map<String, String> metadata) {
        s3Service.updateFileMetadata(bucketName, key, metadata);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/download")
    public ResponseEntity<byte[]> downloadFile(
            @RequestParam("bucket") String bucketName,
            @RequestParam("key") String key) {
        byte[] fileContent = s3Service.downloadFile(bucketName, key);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + key + "\"")
                .body(fileContent);
    }

    @DeleteMapping("/delete")
    public ResponseEntity<String> deleteFile(
            @RequestParam("bucket") String bucketName,
            @RequestParam("key") String key) {
        s3Service.deleteFile(bucketName, key);
        return ResponseEntity.ok("File deleted successfully");
    }

    @GetMapping("/list")
    public ResponseEntity<List<String>> listFiles(
            @RequestParam("bucket") String bucketName) {
        List<String> files = s3Service.listFiles(bucketName);
        return ResponseEntity.ok(files);
    }

    @PostMapping("/buckets")
    public ResponseEntity<BucketInfo> createBucket(
            @RequestParam("name") String bucketName,
            @RequestParam("region") String region) {
        BucketInfo bucketInfo = s3Service.createBucket(bucketName, region);
        return ResponseEntity.ok(bucketInfo);
    }

    @DeleteMapping("/buckets/{bucketName}")
    public ResponseEntity<Void> deleteBucket(@PathVariable String bucketName) {
        s3Service.deleteBucket(bucketName);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/buckets")
    public ResponseEntity<List<BucketInfo>> listBuckets() {
        List<BucketInfo> buckets = s3Service.listBuckets();
        return ResponseEntity.ok(buckets);
    }

    @GetMapping("/preview")
    public ResponseEntity<Map<String, Object>> getFilePreview(
            @RequestParam("bucket") String bucketName,
            @RequestParam("key") String key) {
        FileMetadata metadata = s3Service.getFileMetadata(bucketName, key);
        String previewUrl = s3Service.generatePresignedUrl(bucketName, key, 60);
        
        Map<String, Object> response = Map.of(
                "metadata", metadata,
                "previewable", s3Service.isPreviewable(metadata.getContentType()),
                "previewUrl", previewUrl
        );
        
        return ResponseEntity.ok(response);
    }
} 