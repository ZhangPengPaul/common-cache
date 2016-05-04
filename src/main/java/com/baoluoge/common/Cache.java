package com.baoluoge.common;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by PaulZhang on 2016/5/3.
 */
public class Cache {

    private static ObservableCache cacheImpl;

    public static Properties properties;

    public static Cache getCache() {
        return null;
    }

    private void init() {
        properties = new Properties();
        InputStream in = new BufferedInputStream(Cache.class.getResourceAsStream(CacheConfig.CLIENT_CONFIG_FILE));
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (Objects.equals(properties.getProperty("cache.type"), "memcached")) {
            try {
                cacheImpl = MemcachedImpl.getInstance(true);
            } catch (IOException e) {
                e.printStackTrace();
                // TODO fall back to local cache
            }
        } else {
            // TODO init local cache
        }
    }


}
