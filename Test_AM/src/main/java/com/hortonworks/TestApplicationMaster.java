package com.hortonworks;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
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
	private FileSystem fileSystem;
	private Path inputFile;
	private ContainerId httpdContainerID;
	private NodeId httpdNodeID;
	
	public TestApplicationMaster(String inputFileName) throws IOException {
		conf = new YarnConfiguration();
		resourceManager = AMRMClient.createAMRMClient();
		resourceManager.init(conf);
		resourceManager.start();
		
		nodeManager = NMClient.createNMClient();
		nodeManager.init(conf);
		nodeManager.start();
		
		fileSystem = FileSystem.get(conf);
		inputFile = new Path(inputFileName);
	}
	
	public static void main(String [] args) {
		
		TestApplicationMaster appMaster = null;
		try {
			appMaster = new TestApplicationMaster(args[0]);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
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
		startHttpdContainer(false);
		startSearchContainers();
		
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {}
		
		return true;
	}
	
	private void startSearchContainers() throws IOException, YarnException {
		//Read the block information from HDFS
		int start = 0, len = 0;
		FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
		LOG.info("File status = " + fileStatus.toString());
		BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		LOG.info("Number of blocks for " + inputFile.toString() + " = " + blocks.length);
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);
		Resource capacity = Records.newRecord(Resource.class);
		capacity.setMemory(256);
		//String [] racks = {"default-rack"};
		
		int numOfContainers = 0;
		for(BlockLocation block : blocks) {
			ContainerRequest ask = new ContainerRequest(capacity,block.getHosts(),null,priority,false);
			for(String host : block.getHosts()) {
				System.out.println(host + " for block " + block.toString());
			}
			LOG.info("Asking for Container for block " + block.toString());
			resourceManager.addContainerRequest(ask);
			numOfContainers++;
		}
		LOG.info("Attempting to allocate " + numOfContainers + " containers...");
		int allocatedContainers = 0;
		while (allocatedContainers < numOfContainers) {
			AllocateResponse response = resourceManager.allocate(0);
			for(Container container : response.getAllocatedContainers()) {
				++allocatedContainers;
				LOG.info("Container just allocated on node " + container.getNodeHttpAddress());
				ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
				context.setCommands(
						Collections.singletonList("sleep 30")
					);
				nodeManager.startContainer(container, context);
				LOG.info("Container just launched on " + container.getNodeHttpAddress());
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
	}

	private void startHttpdContainer(boolean killExistingContainer) throws YarnException, IOException {
		Priority httpdPriority = Records.newRecord(Priority.class);
		httpdPriority.setPriority(0);
		Resource capHttp = Records.newRecord(Resource.class);
		capHttp.setMemory(128);
		String [] hosts = {"node1"};
		ContainerRequest httpAsk = new ContainerRequest(capHttp,hosts,null,httpdPriority,false);
		LOG.info("Requesting a Container for httpd");
		resourceManager.addContainerRequest(httpAsk);
		LOG.info("Allocating the httpd Container...");
		
		int allocatedContainers = 0;
		while(allocatedContainers < 1) {
			AllocateResponse allocResponse = resourceManager.allocate(0);	
			LOG.info("Containers allocated with resources " + allocResponse.getAvailableResources());
			for(Container container : allocResponse.getAllocatedContainers()) {
				++allocatedContainers;
				//Launch httpd on its Container
				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				String httpdCommand = "";
				if(killExistingContainer) {
					LOG.info("Stopping httpd Container...");
					httpdCommand = "/usr/sbin/httpd -k stop";
				} else {
					LOG.info("Starting httpd Container...");
					httpdCommand = "/usr/sbin/httpd -k start";
				}
				ctx.setCommands(
						Collections.singletonList(httpdCommand
								+ " 1>/tmp/httpdstdout"
								+ " 2>/tmp/httpdstderr")
					);
				
				nodeManager.startContainer(container, ctx);
				//LOG.info("httpd is now running on host " + container.getNodeHttpAddress());
				httpdContainerID = container.getId();
				httpdNodeID = container.getNodeId();
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
		LOG.info("httpd Container is running...");
	}
	
	private boolean finish() throws YarnException, IOException {
		LOG.info("Finishing TestApplicationMaster...");
		//We need to stop the httpd Container since it will not finish on its own
		nodeManager.stopContainer(this.httpdContainerID, this.httpdNodeID);
		//We need to kill the httpd process on node1
		this.startHttpdContainer(true);
		//Now we need to kill the Container that we just created
		nodeManager.stopContainer(this.httpdContainerID, this.httpdNodeID);
		
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

