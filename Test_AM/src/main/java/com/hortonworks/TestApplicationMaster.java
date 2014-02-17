package com.hortonworks;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class TestApplicationMaster {

	private static final Log LOG = LogFactory.getLog(TestApplicationMaster.class);
	private YarnConfiguration conf;
	private AMRMClientAsync amRMClient;
	private AMRMClientAsync<ContainerRequest> resourceManager;
	
	public TestApplicationMaster() {
		conf = new YarnConfiguration();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, null);
		amRMClient.init(conf);
		amRMClient.start();
	}
	
	public static void main(String [] args) {
		
		TestApplicationMaster appMaster = new TestApplicationMaster();
		try {
			appMaster.run();
			appMaster.finish();
		} catch (YarnException | IOException e) {
			e.printStackTrace();
		}
	}

	public boolean run() throws YarnException, IOException {
		LOG.info("Running TestApplicationMaster...");
		
		//We need to communicate with the ResourceManager, so initialize the service
		resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, null);
		resourceManager.init(conf);
		resourceManager.start();
		
		//Register this ApplicationMaster with the ResourceManager
		String appHostName = NetUtils.getHostname();
		int appHostPort = -1;
		String appTrackingUrl = "";
		RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);

		LOG.info("ApplicationMaster is registered with response: " + response.toString());
		
		return true;
	}
	
	private boolean finish() {
		LOG.info("Finishing TestApplicationMaster...");
		try {
			amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Finishing TestApplicationMaster", null);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}

