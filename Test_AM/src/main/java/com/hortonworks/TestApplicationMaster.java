package com.hortonworks;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;


public class TestApplicationMaster {

	private static final Log LOG = LogFactory.getLog(TestApplicationMaster.class);
	private YarnConfiguration conf;
	private AMRMClient<ContainerRequest> resourceManager;
	
	public TestApplicationMaster() {
		conf = new YarnConfiguration();
		resourceManager = AMRMClient.createAMRMClient();
		resourceManager.init(conf);
		resourceManager.start();
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
		
		//Register this ApplicationMaster with the ResourceManager
		String appHostName = NetUtils.getHostname();
		int appHostPort = -1;
		String appTrackingUrl = "";
		RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);

		LOG.info("ApplicationMaster is registered with response: " + response.toString());
		
		//Create a Container to run httpd
		Resource capHttp = Records.newRecord(Resource.class);
		capHttp.setMemory(256);
		capHttp.setVirtualCores(1);
		ContainerRequest httpAsk = new ContainerRequest(capHttp,null,null,null);
		resourceManager.addContainerRequest(httpAsk);
		AllocateResponse allocResponse = resourceManager.allocate(0);		
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

