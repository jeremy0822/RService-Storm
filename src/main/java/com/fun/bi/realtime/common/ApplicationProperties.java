package com.fun.bi.realtime.common;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ApplicationProperties {

	private static Log log = LogFactory.getLog(ApplicationProperties.class);

	private static Properties config = new Properties();

	static {

		try {
			config.load(new InputStreamReader(ApplicationProperties.class
					.getResourceAsStream("/application.properties"), "utf-8"));
			log.info("application.properties:" + config);
		} catch (UnsupportedEncodingException e) {
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}

	}

	public static String getProperties(String name) {

		return config.getProperty(name);
	}

}
