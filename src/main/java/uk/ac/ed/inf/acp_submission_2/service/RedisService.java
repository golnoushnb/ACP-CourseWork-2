package uk.ac.ed.inf.acp_submission_2.service;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisService {

    private final StringRedisTemplate stringRedisTemplate;

    public RedisService(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public String getValue(String key) {
        return stringRedisTemplate.opsForValue().get(key);
    }

    public void setValue(String key, String value) {
        stringRedisTemplate.opsForValue().set(key, value);
    }

    public void deleteKey(String key) {
        stringRedisTemplate.delete(key);
    }

    public void putHashValue(String hashName, String field, String value) {
        stringRedisTemplate.opsForHash().put(hashName, field, value);
    }

    public Object getHashValue(String hashName, String field) {
        return stringRedisTemplate.opsForHash().get(hashName, field);
    }

    public void flushAll() {
        RedisConnection connection = stringRedisTemplate.getConnectionFactory().getConnection();
        try {
            connection.serverCommands().flushDb();
        } finally {
            connection.close();
        }
    }
}