package com.hortonworks;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;



public class TestApplicationMaster {

	private static final Log LOG = LogFactory.getLog(TestApplicationMaster.class);
	private YarnConfiguration conf;
	private AMRMClient<ContainerRequest> resourceManager;
	private NMClient nodeManager;
	
	public TestApplicationMaster() {
		conf = new YarnConfiguration();
		resourceManager = AMRMClient.createAMRMClient();
		resourceManager.init(conf);
		resourceManager.start();
		
		nodeManager = NMClient.createNMClient();
		nodeManager.init(conf);
		nodeManager.start();
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
		Priority httpdPriority = Records.newRecord(Priority.class);
		httpdPriority.setPriority(0);
		
		Resource capHttp = Records.newRecord(Resource.class);
		capHttp.setMemory(256);
		ContainerRequest httpAsk = new ContainerRequest(capHttp,null,null,httpdPriority);
		LOG.info("Requesting a Container for httpd");
		resourceManager.addContainerRequest(httpAsk);
		LOG.info("Allocating the httpd Container...");
		AllocateResponse allocResponse = resourceManager.allocate(0);	
		LOG.info("Containers allocated with resources " + allocResponse.getAvailableResources());
		LOG.info("Number of Containers allocated = " + allocResponse.getAllocatedContainers().size());
		
		for(Container container : allocResponse.getAllocatedContainers()) {
			//Launch httpd on its Container
			ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
			String httpdCommand = "/etc/init.d/httpd start";
			ctx.setCommands(
					Collections.singletonList(httpdCommand
							+ " 1>/tmp/httpdstdout"
							+ " 2>/tmp/httpdstderr")
				);
			LOG.info("Starting httpd Container...");
			nodeManager.startContainer(container, ctx);
			LOG.info("httpd is now running on host " + container.getNodeHttpAddress());

		}
		return true;
	}
	
	private boolean finish() {
		LOG.info("Finishing TestApplicationMaster...");
		try {
			resourceManager.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Finishing TestApplicationMaster", null);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}

