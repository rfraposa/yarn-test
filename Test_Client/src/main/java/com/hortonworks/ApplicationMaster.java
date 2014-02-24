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
import org.apache.hadoop.yarn.api.records.ContainerStatus;
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
	private List<BlockStatus> blockList;
	private String searchTerm;
	private int numOfContainers;
	private String outputFolder;
	
	public ApplicationMaster(String [] args) throws IOException { // inputFileName, String searchTerm) throws IOException {
		conf = new YarnConfiguration();
		resourceManager = AMRMClient.createAMRMClient();
		resourceManager.init(conf);
		resourceManager.start();
		
		nodeManager = NMClient.createNMClient();
		nodeManager.init(conf);
		nodeManager.start();
		
		fileSystem = FileSystem.get(conf);
		inputFile = new Path(args[0]);
		this.searchTerm = args[1];
		outputFolder = args[2];
		
		
		blockList = new ArrayList<BlockStatus>();
	}
	
	public static void main(String [] args) {
		
		ApplicationMaster appMaster = null;
		try {
			appMaster = new ApplicationMaster(args);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
		try {
			appMaster.run();
			appMaster.monitor();
			appMaster.finish();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void monitor() throws YarnException, IOException, InterruptedException {
		//All the initial Containers are running...this method sends heartbeats to the RM and monitors all running containers
		LOG.info("Waiting for Containers to finish...");
		int completedContainers = 0;
		while(completedContainers < this.numOfContainers) {
			AllocateResponse response = resourceManager.allocate(completedContainers/numOfContainers);
			for(ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				LOG.info("Container just finished: " + status.toString());
			}
			Thread.sleep(100);
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
		
		return true;
	}
	
	private void startSearchContainers() throws IOException, YarnException, URISyntaxException {
		BlockLocation[] blocks = this.getBlockLocations();
		
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);
		Resource capacity = Records.newRecord(Resource.class);
		capacity.setMemory(2048);
		
		numOfContainers = 0;
		for(BlockLocation block : blocks) {
			ContainerRequest ask = new ContainerRequest(capacity,block.getHosts(),null,priority,false);
			for(String host : block.getHosts()) {
				System.out.println(host + " for block " + block.toString());
			}
			LOG.info("Asking for Container for block " + block.toString());
			resourceManager.addContainerRequest(ask);
			blockList.add(new BlockStatus(block));
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
				
		
				String command = getLaunchContainerCommand(container);
				
				List<String> commands = new ArrayList<String>();
				commands.add(command);		
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


	/**
	 * Given a hostname, this method determines which block this Container should process
	 * attempting to apply data locality as much as possible
	 * 
	 * @param container the Container that is about to be started
	 * @return the command to be executed on this new Container
	 * @throws IOException 
	 */
	private String getLaunchContainerCommand(Container container) throws IOException {
		String hostname = container.getNodeHttpAddress();
		boolean foundContainer = false;
		BlockStatus blockToProcess = null;
		
		//Find a BlockStatus that needs to be processed...
			outer: for(BlockStatus current : blockList) {
				if(!current.isStarted()) {
					for(int i = 0; i < current.getLocation().getHosts().length; i++) {
						String currentHost = current.getLocation().getHosts()[i] + ":8042";
						LOG.info("Comparing " + currentHost + " with container on " + hostname);
						if(currentHost.equals(hostname)) {
							//Assign this BlockStatus to this Container
							blockToProcess = current;
							current.setStarted(true);
							current.setContainer(container);
							foundContainer = true;
							break outer;
						} 
					}
				}
			}
		if(foundContainer) {
			LOG.info("Data Locality achieved!!!");
		}
		if(!foundContainer) {
			//Just find any block to process
			LOG.info("Data locality not found - trying another node");
			for(BlockStatus current : blockList) {
				if(!current.isStarted()) {
					blockToProcess = current;
					current.setStarted(true);
					current.setContainer(container);
					foundContainer = true;
					break;
				}
			}
		}
		if(foundContainer) {
			LOG.info("Processing block from " + blockToProcess.getLocation() + " on a Container running on " + container.getNodeHttpAddress());
		} else {
			LOG.error("No container found to handle block!");
			return "sleep 5";
		}

		
		//Configure the command line argument that launches the Container
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add("yarn jar ./app.jar com.hortonworks.Container ");
		vargs.add(inputFile.toString());  //File to read
		String offsetString = Long.toString(blockToProcess.getLocation().getOffset());
		String lengthString = Long.toString(blockToProcess.getLocation().getLength());
		LOG.info("Reading block starting at " + offsetString + " and length " + lengthString);
		vargs.add(offsetString); //Offset into the file
		vargs.add(lengthString); //Number of bytes to read
		vargs.add(this.searchTerm); //The term we are searching for
		vargs.add(this.outputFolder); //Folder in HDFS to store results
		
		vargs.add("1>/tmp/TestContainer.stdout");
		vargs.add("2>/tmp/TestContainer.stderr");
		StringBuilder command = new StringBuilder();
		for(CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		return command.toString();
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

