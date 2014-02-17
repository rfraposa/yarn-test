package com.hortonworks;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class TestClient {
	private static final Log LOG = LogFactory.getLog(TestClient.class);
	
	private YarnConfiguration conf;
	private YarnClient yarnClient;
	private String appJar = "/home/train/workspace/Test_AM/testam.jar";
	private String appJarDest = "/user/root/testam.jar";
	private ApplicationId appId;
	private String jobInputFolder;
	private FileSystem fs;
	
	String appMasterMainClass = "com.hortonworks.TestApplicationMaster";
	
	public TestClient(String [] args) {
		this.conf = new YarnConfiguration();
		if(args.length < 2) {
			System.out.println("Usage: TestClient <resourcemanager_ip> <job_input_folder>");
			System.exit(1);
		}
		conf.set("yarn.resourcemanager.address", args[0]);
		jobInputFolder = args[1];
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		
	}
	
	public static void main(String[] args) {
		TestClient client = new TestClient(args);
		boolean result = false;
		try {
			result = client.run();
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(result) {
			try {
				client.monitorApplication();
			} catch (YarnException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void monitorApplication() throws YarnException, IOException {
		ApplicationReport report = yarnClient.getApplicationReport(appId);
		LOG.info("Application started at " + report.getStartTime() + " on " + report.getHost());
		
		while(true) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {}
			
			report = yarnClient.getApplicationReport(appId);
			LOG.info("Application progress: " + (report.getProgress() * 100.0) + "%");

			YarnApplicationState state = report.getYarnApplicationState();
			LOG.info("Current state is " + state.toString());
			
			if(state == YarnApplicationState.FINISHED) {
				LOG.info("Application is finished!");
				return;
			} else if(state == YarnApplicationState.KILLED || state == YarnApplicationState.FAILED) {
				LOG.info("Application did not finish.");
				return;
			}
		}
	}

	public boolean run() throws YarnException, IOException {
		yarnClient.start();
		printClusterInfo();
		
		//Get a new application ID
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		appId = appResponse.getApplicationId();
		LOG.info("Application ID = " + appId);
		
		//Set the application name
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appContext.setApplicationName("Test_YARN");
			
		//Create the Container launch context for the ApplicationMaster
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		//Add the ApplicationMaster JAR file to HDFS as a LocalResource
		fs = FileSystem.get(conf);
		Path src = new Path(appJar);
		//Path dest = new Path(fs.getHomeDirectory(), File.separator + appJar);
		Path dest = new Path(this.appJarDest);
		String appJarUri = dest.toUri().toString();
		LOG.info("Adding " + appJar + " to HDFS folder " + appJarUri);
		LOG.info("FileSystem info: " + fs.getCanonicalServiceName() + " " + fs.getHomeDirectory());
		fs.copyFromLocalFile(false, true, src, dest);
		FileStatus destStatus = fs.getFileLinkStatus(dest);
		LOG.info("Status of " + appJarUri + " = " + destStatus);
		LocalResource amJarResource = Records.newRecord(LocalResource.class);
		amJarResource.setType(LocalResourceType.FILE);
		amJarResource.setVisibility(LocalResourceVisibility.APPLICATION);
		Path amJarPath = new Path("hdfs://namenode:8020" + this.appJarDest);
		amJarResource.setResource(ConverterUtils.getYarnUrlFromPath(amJarPath));
		LOG.info("Adding resource JAR = " +  amJarPath.toString());
		amJarResource.setTimestamp(destStatus.getModificationTime());
		amJarResource.setSize(destStatus.getLen());
		localResources.put("testam.jar", amJarResource);
		
		amContainer.setLocalResources(localResources);

		//Configure the CLASSPATH of the ApplicationMaster
		Map<String, String>	env = new HashMap<String, String>();
		StringBuilder classpathEnv = new StringBuilder("./*"); 
//				Environment.CLASSPATH.$()).append(
//				File.pathSeparatorChar).append("./*");
		
		for(String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classpathEnv.append(File.pathSeparatorChar).append(c.trim());
		}
		LOG.info("CLASSPATH for ApplicationMaster = " + classpathEnv);
		env.put("CLASSPATH", classpathEnv.toString());
		amContainer.setEnvironment(env);
		
		//Configure the command line argument that launches the ApplicationMaster
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		String tmpJarFileName = "/tmp/" + appId + ".jar";
		vargs.add("hadoop fs -copyToLocal hdfs://namenode:8020/" + this.appJarDest + " " + tmpJarFileName 
					+ "  && hadoop jar " + tmpJarFileName);
		vargs.add("1>/tmp/TestAM.stdout");
		vargs.add("2>/tmp/TestAM.stderr");
		StringBuilder command = new StringBuilder();
		for(CharSequence str : vargs) {
			command.append(str).append(" ");
		}
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());		
		amContainer.setCommands(commands);
		LOG.info("Command to execute ApplicationMaster = " + command);
		
		//Ask for resources in the ApplicationSubmissionContext
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(appResponse.getMaximumResourceCapability().getMemory());
		capability.setVirtualCores(appResponse.getMaximumResourceCapability().getVirtualCores());
		appContext.setResource(capability);
		

		//Set the Container launch context with the ApplicationSubmissionContext
		appContext.setAMContainerSpec(amContainer);
		
		//Write the input folder metainfo to HDFS
		processInputFolder();
		
		yarnClient.submitApplication(appContext);
		
		LOG.info("Application submitted with id = " + appId);
		
		return true;
	}
	
	
	
	private void processInputFolder() throws IOException {
		Path inputPath = new Path(this.jobInputFolder);
		if(fs.isDirectory(inputPath)) {
			LOG.info("Input path " + jobInputFolder + " is a folder.");
		} else {
			LOG.info("Input path " + jobInputFolder + " is a file.");
			FileStatus fileStatus = fs.getFileStatus(inputPath);
			LOG.info("File status = " + fileStatus.toString());
			BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for(BlockLocation block : blockLocations) {
				LOG.info("Block at " + block.toString());
			}
		}
	}

	private void printClusterInfo() throws YarnException, IOException {
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		LOG.info("Cluster metrics received from RM. There are " + clusterMetrics.getNumNodeManagers() + " NodeManagers.");

		List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		for(NodeReport node : nodeReports) {
			LOG.info("Node ID = " + node.getNodeId() 
						+ "  address = " + node.getHttpAddress() 
						+ " containers = " + node.getNumContainers());
		}
		List<QueueInfo> queueList = yarnClient.getAllQueues();
		for(QueueInfo queue : queueList) {
			LOG.info("Available queue: " + queue.getQueueName() 
					+ " with capacity " + queue.getCapacity() + " to " + queue.getMaximumCapacity());
		}
		
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		
		int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		
		LOG.info("Max memory = " + maxMemory + " and max vcores = " + maxVCores);
	}

}
