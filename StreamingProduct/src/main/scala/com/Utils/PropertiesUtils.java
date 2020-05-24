package com.Utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * 获取配置文件属性的工具类
 */
public class PropertiesUtils {

    private final static String propertiesfile = "application.properties";

    private PropertiesUtils() {

    }

    private static Properties properties;

    public static Properties getProperties() {

        Resource resource = new ClassPathResource(propertiesfile);
        Properties props = null;
        try {
            props = PropertiesLoaderUtils.loadProperties(resource);
        } catch (IOException e) {
            System.out.println("========读取配置文件："+propertiesfile+"出现异常===========");
            e.printStackTrace();
        }
        return props;
    }


}
