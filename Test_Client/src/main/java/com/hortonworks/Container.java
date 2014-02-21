package com.hortonworks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Container {
	private static final Log LOG = LogFactory.getLog(Container.class);

	public static void main(String[] args) {
		LOG.info("Inside main method of Container");
		try {
			LOG.info("Container is sleeping...");
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		LOG.info("Container is ending...");
	}

}
