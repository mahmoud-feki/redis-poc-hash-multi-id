package com.redis.test.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PayloadService {
    private final RedisTemplate<String, String> redisTemplate;

    @Autowired
    public PayloadService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * This method adds a payload to the Redis store for a specific userId and correlationId.
     * The payload is stored as a hash under the key "user:pending:{userId}".
     *
     * @param userId      the ID of the user
     * @param corrId      the correlation ID for the payload
     * @param payloadJson the JSON representation of the payload
     */
    public void addPayload(String userId, String corrId, String payloadJson) {
        String key = "user:pending:" + userId;
        redisTemplate.opsForHash().put(key, corrId, payloadJson);
    }

    /**
     * This method retrieves and deletes the payload for a given userId.
     * It uses a Lua script to ensure atomicity in fetching and deleting the data.
     *
     * @param userId the ID of the user whose payload is to be consumed
     * @return a Map containing the payload data, or an empty map if no data exists
     */
    public Map<String, String> consumeAndDelete(String userId) {
        String key = "user:pending:" + userId;

        String luaScript = """
                    local key = KEYS[1]
                    local result = redis.call('HGETALL', key)
                    redis.call('DEL', key)
                    return result
                """;

        List<Object> rawResult = redisTemplate.execute(
                (RedisCallback<List<Object>>) connection ->
                        connection.scriptingCommands().eval(
                                luaScript.getBytes(),
                                ReturnType.MULTI,
                                1,
                                key.getBytes()
                        )
        );

        Map<String, String> resultMap = getStringStringMap(rawResult);

        return resultMap;
    }

    /**
     * This method converts the raw result from Redis into a Map<String, String>.
     * The raw result is expected to be a list of alternating keys and values.
     *
     * @param rawResult the raw result from Redis
     * @return a Map containing the key-value pairs
     */
    private static Map<String, String> getStringStringMap(List<Object> rawResult) {
        Map<String, String> resultMap = new HashMap<>();
        if (rawResult != null) {
            for (int i = 0; i < rawResult.size(); i += 2) {
                resultMap.put(toString(rawResult.get(i)), toString(rawResult.get(i + 1)));
            }
        }
        return resultMap;
    }

    /**
     * Converts an Object to a String.
     * If the object is a byte array, it converts it to a UTF-8 string.
     * Otherwise, it uses the default toString method.
     *
     * @param obj the object to convert
     * @return the string representation of the object
     */
    private static String toString(Object obj) {
        return obj instanceof byte[] bytes
                ? new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
                : String.valueOf(obj);
    }
}
