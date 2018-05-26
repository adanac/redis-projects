package com.adanac.redis;

import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Hello world!
 */
public class BatchOperatorSet {
    private static final String HOST = "localhost";
    private static final int PORT = 6379;
    private static Jedis jedis;

    static {
        jedis = new Jedis(HOST, PORT);
    }

    @Test
    public void testRedis() {
        String testKey = jedis.get("testKey");
        System.out.println(testKey);
    }

    /**
     * 批量插入数据到Redis，正常使用
     *
     * @throws Exception
     */
    @Test
    public static void batchSetNotUsePipeline() throws Exception {
        String keyPrefix = "normal";
        long begin = System.currentTimeMillis();
        for (int i = 1; i < 10000; i++) {
            String key = keyPrefix + "_" + i;
            String value = String.valueOf(i);
            jedis.set(key, value);
        }
        jedis.close();
        long end = System.currentTimeMillis();
        System.out.println("not use pipeline batch set total time：" + (end - begin));
    }

    /**
     * 批量插入数据到Redis，使用Pipeline
     *
     * @throws Exception
     */
    @Test
    public static void batchSetUsePipeline() throws Exception {
        Jedis jedis = new Jedis(HOST, PORT);
        Pipeline pipelined = jedis.pipelined();
        String keyPrefix = "pipeline";
        long begin = System.currentTimeMillis();
        for (int i = 1; i < 10000; i++) {
            String key = keyPrefix + "_" + i;
            String value = String.valueOf(i);
            pipelined.set(key, value);
        }
        pipelined.sync();
        jedis.close();
        long end = System.currentTimeMillis();
        System.out.println("use pipeline batch set total time：" + (end - begin));
    }
}
