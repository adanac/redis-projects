package com.adanac.redis;

import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.Map;

/**
 * @author adanac
 * @date 2018/5/26
 */
public class BatchOperatorGet {
    private static final String HOST = "localhost";
    private static final int PORT = 6379;
    private static Jedis jedis;

    static {
        jedis = new Jedis(HOST, PORT);
    }

    /**
     * 批量从Redis中获取数据，正常使用
     *
     * @return
     * @throws Exception
     */
    @Test
    public static void batchGetNotUsePipeline() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        String keyPrefix = "normal";
        long begin = System.currentTimeMillis();
        for (int i = 1; i < 10000; i++) {
            String key = keyPrefix + "_" + i;
            String value = jedis.get(key);
            map.put(key, value);
        }
        jedis.close();
        long end = System.currentTimeMillis();
        System.out.println("not use pipeline batch get total time：" + (end - begin));
    }

    /**
     * 批量从Redis中获取数据，使用Pipeline
     */
    @Test
    public static void batchGetUsePipeline() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        Pipeline pipelined = jedis.pipelined();
        String keyPrefix = "pipeline";
        // 使用pipeline方式批量获取数据，只能获取到value值，对应的key获取不到，我们通过一个中间map来获取keyHash
        Map<String, Response<String>> intrmMap = new HashMap<String, Response<String>>();
        long begin = System.currentTimeMillis();
        for (int i = 1; i < 10000; i++) {
            String key = keyPrefix + "_" + i;
            intrmMap.put(key, pipelined.get(key));
        }
        pipelined.sync();
        jedis.close();
        for (Map.Entry<String, Response<String>> entry : intrmMap.entrySet()) {
            Response<String> sResponse = (Response<String>) entry.getValue();
            String key = new String(entry.getKey());
            String value = sResponse.get();
            map.put(key, value);
        }
        long end = System.currentTimeMillis();
        System.out.println("use pipeline batch get total time：" + (end - begin));
    }

}
