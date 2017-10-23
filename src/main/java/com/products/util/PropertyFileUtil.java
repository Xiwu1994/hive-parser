package com.products.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileUtil {

    private static Properties properties = null;

    public static synchronized boolean isLoaded() {
        return properties != null;
    }

    public static void init(String path) {
        System.out.println("path: " + path + "\n");
        InputStream is = PropertyFileUtil.class.getClass().getResourceAsStream(path);
        if (null != is) {
            loadProperty(is);
            System.out.println("properties init success\n");
            return;
        }
    }

//    static  {
//        InputStream is = PropertyFileUtil.class.getClass().getResourceAsStream("/app.properties");
//        if (null != is) {
//            loadProperty(is);
//        }
//    }

    private static void loadProperty(String file) {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void loadProperty(InputStream is) {
        properties = new Properties();
        try {
            properties.load(is);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}    