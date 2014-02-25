package com.hortonworks;

import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

public class Log4jPropertyHelper {

  public static void updateLog4jConfiguration(Class<?> targetClass) {
    try (InputStream is = targetClass.getResourceAsStream("/log4j-container.properties");) {
      Properties originalProperties = new Properties();
      originalProperties.load(is);
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(originalProperties);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
