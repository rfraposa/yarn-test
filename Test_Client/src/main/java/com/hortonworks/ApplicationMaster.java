package com.hortonworks;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;



public class ApplicationMaster {

	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
	private YarnConfiguration conf;
	private AMRMClient<ContainerRequest> resourceManager;
	private NMClient nodeManager;
	private FileSystem fileSystem;
	private Path inputFile;
	private ContainerId httpdContainerID;
	private NodeId httpdNodeID;
	
	public ApplicationMaster(String inputFileName) throws IOException {
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
		
		ApplicationMaster appMaster = null;
		try {
			appMaster = new ApplicationMaster(args[0]);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
		try {
			appMaster.run();
			appMaster.finish();
		} catch (YarnException | IOException | URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public boolean run() throws YarnException, IOException, URISyntaxException {
		LOG.info("Running ApplicationMaster...");
		
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
	
	private void startSearchContainers() throws IOException, YarnException, URISyntaxException {
		BlockLocation[] blocks = this.getBlockLocations();
		
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);
		Resource capacity = Records.newRecord(Resource.class);
		capacity.setMemory(256);
		
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
		
		//Each Container needs the application JAR file, which is in HDFS via the AMJAR environment variable
		Map<String, LocalResource> localResources = new HashMap<String,LocalResource>();
		
		LocalResource appJarFile = Records.newRecord(LocalResource.class);
		appJarFile.setType(LocalResourceType.FILE);
		appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
		Map<String,String> env = System.getenv();
		appJarFile.setResource(ConverterUtils.getYarnUrlFromURI(new URI(env.get("AMJAR"))));
		appJarFile.setTimestamp(Long.valueOf((env.get("AMJARTIMESTAMP"))));
		appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
		localResources.put("app.jar", appJarFile);
		LOG.info("Added " + appJarFile.toString() + " as a local resource to each Container");
		
		
		LOG.info("Attempting to allocate " + numOfContainers + " containers...");
		int allocatedContainers = 0;
		while (allocatedContainers < numOfContainers) {
			AllocateResponse response = resourceManager.allocate((float) (((float) allocatedContainers) / 100.0));
			for(Container container : response.getAllocatedContainers()) {
				++allocatedContainers;
				LOG.info("Container just allocated on node " + container.getNodeHttpAddress());
				ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
				context.setLocalResources(localResources);
				
				//Configure the command line argument that launches the Container
				Vector<CharSequence> vargs = new Vector<CharSequence>(30);
				vargs.add("hadoop jar ./app.jar com.hortonworks.Container ");
				vargs.add("1>/tmp/TestContainer.stdout");
				vargs.add("2>/tmp/TestContainer.stderr");
				StringBuilder command = new StringBuilder();
				for(CharSequence str : vargs) {
					command.append(str).append(" ");
				}
				List<String> commands = new ArrayList<String>();
				commands.add(command.toString());		
				context.setCommands(commands);
				LOG.info("Command to execute Container = " + command);
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
		LOG.info("Finishing ApplicationMaster...");
		//We need to stop the httpd Container since it will not finish on its own
		nodeManager.stopContainer(this.httpdContainerID, this.httpdNodeID);
		//We need to kill the httpd process on node1
		this.startHttpdContainer(true);
		//Now we need to kill the Container that we just created
		nodeManager.stopContainer(this.httpdContainerID, this.httpdNodeID);
		
		try {
			resourceManager.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Finishing ApplicationMaster", null);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	public BlockLocation[] getBlockLocations() throws IOException {
		//Read the block information from HDFS
 		FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
		LOG.info("File status = " + fileStatus.toString());
		BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		LOG.info("Number of blocks for " + inputFile.toString() + " = " + blocks.length);
		return blocks;
	}
}

