package com.redis.test.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.ExecutorService;

@RestController
@RequestMapping("/v1/api/redis")
public class RedisController {

    @Autowired
    PayloadService payloadService;

    @Autowired
    private ObjectMapper objectMapper;

    @PostMapping("/addPayload")
    public ResponseEntity<String> addPayload(@RequestBody Map<String, Object> payload) throws JsonProcessingException {

        Object userId = payload.get("userId");
        Object corrId = payload.get("corrId");

        String payloadJson = objectMapper.writeValueAsString(payload);

        payloadService.addPayload(
                userId != null ? userId.toString() : null,
                corrId != null ? corrId.toString() : null,
                payloadJson
        );
        return ResponseEntity.ok("Payload added successfully");
    }

    @GetMapping("/getPayload/{userId}")
    public ResponseEntity< Map<String, String>> getPayload(@PathVariable String userId) {
        Map<String, String> payload = payloadService.consumeAndDelete(userId);
        return ResponseEntity.ok(payload);
    }

    @GetMapping("/test-concurrent-read/{userId}/{threadCount}")
    public ResponseEntity<String> testConcurrentRead(
            @PathVariable String userId,
            @PathVariable int threadCount) throws InterruptedException {

        ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                Map<String, String> result = payloadService.consumeAndDelete(userId);
                System.out.println("Thread " + Thread.currentThread().getName() + " result: " + result);
            });
        }

        executor.shutdown();
        executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);

        return ResponseEntity.ok("Concurrent read test finished. Check logs for results.");
    }




}
