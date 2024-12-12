package org.apache.iotdb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 用于读取配置文件
 */
public class ReadConfig {

    // 单例对象
    private static ReadConfig instance;

    // 存储配置数据
    private Properties config;

    // 私有构造函数，防止外部实例化
    private ReadConfig() {
        config = new Properties();
        // 使用ClassLoader获取InputStream
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
        try {
            config.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 提供全局访问点
    public static ReadConfig getInstance() {
        if (instance == null) {
            instance = new ReadConfig();
        }
        return instance;
    }

    // 获取配置值
    public String getConfigValue(String key) {
        return config.getProperty(key);
    }
}