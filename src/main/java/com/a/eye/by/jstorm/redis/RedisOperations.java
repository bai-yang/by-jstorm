package com.a.eye.by.jstorm.redis;

import java.io.Serializable;
import java.util.Map;

import redis.clients.jedis.Jedis;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RedisOperations implements Serializable {

    private static final long serialVersionUID = -6843097344858875054L;

    Jedis jedis = null;

    public RedisOperations(String redisIP, int port) {
        jedis = new Jedis(redisIP, port);
    }

    public void insert(String id, Map<String, Object> record) {
        try {
            jedis.set(id, new ObjectMapper().writeValueAsString(record));
        } catch (Exception e) {
            System.out.println("Record not persisted into datastore");
        }
    }
}
