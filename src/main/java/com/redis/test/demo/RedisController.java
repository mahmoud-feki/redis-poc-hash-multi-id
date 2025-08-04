package com.redis.test.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

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




}
