package com.hortonworks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private YarnConfiguration conf;
  private YarnClient yarnClient;
  private String appJar = "testclient.jar";
  // private String appJarDest = "/user/root/testclient.jar";
  private ApplicationId appId;
  private String jobInputFolder;
  private FileSystem fs;
  private String searchTerm;

  String appMasterMainClass = "com.hortonworks.ApplicationMaster";

  private Path inputPath;

  private String outputFolder;

  public Client(String[] args) throws IOException {
    this.conf = new YarnConfiguration();
    if (args.length < 3) {
      System.out.println("Usage: Client <job_input_folder> <search_term> <job_output_folder>");
      System.exit(1);
    }
    this.jobInputFolder = args[0];
    this.searchTerm = args[1];
    this.outputFolder = args[2];
    inputPath = new Path(this.jobInputFolder);

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    fs = FileSystem.get(conf);

    // Delete the output folder
    Path outputPath = new Path(outputFolder);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    // Now create the output folder
    fs.create(outputPath);
    fs.setOwner(outputPath, "yarn", "yarn");
    fs.setPermission(outputPath, FsPermission.getDirDefault());

  }

  public static void main(String[] args) {
    Client client = null;
    try {
      client = new Client(args);
      boolean result = false;
      result = client.run();
      if (result) {
        client.monitorApplication();
      }
    }
    catch (YarnException | IOException e) {
      e.printStackTrace();
    }
  }

  private void monitorApplication() throws YarnException, IOException {
    ApplicationReport report = yarnClient.getApplicationReport(appId);
    LOG.info("Application started at {} on {}", report.getStartTime(), report.getHost());

    while (true) {
      try {
        Thread.sleep(3000);
      }
      catch (InterruptedException e) {
      }

      report = yarnClient.getApplicationReport(appId);
      LOG.info("Application progress: {}%", (report.getProgress() * 100.0));

      YarnApplicationState state = report.getYarnApplicationState();
      LOG.info("Current state is {}", state.toString());

      if (state == YarnApplicationState.FINISHED) {
        LOG.info("Application is finished!");
        return;
      }
      else if (state == YarnApplicationState.KILLED || state == YarnApplicationState.FAILED) {
        LOG.info("Application did not finish.");
        return;
      }
    }
  }

  public boolean run() throws YarnException, IOException {
    yarnClient.start();
    printClusterInfo();

    // Get a new application ID
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    appId = appResponse.getApplicationId();
    LOG.info("Application ID = {}", appId);

    // Set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appContext.setApplicationName("Test_YARN");

    // Create the Container launch context for the ApplicationMaster
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    Map<String, LocalResource> localResources = new HashMap<>();

    // Add the application JAR file as a LocalResource

    Path src = new Path(this.appJar);
    String pathSuffix = appId.getId() + "/app.jar";
    Path dest = new Path(fs.getHomeDirectory(), pathSuffix);
    fs.copyFromLocalFile(false, true, src, dest);
    FileStatus destStatus = fs.getFileStatus(dest);

    LocalResource jarResource = Records.newRecord(LocalResource.class);
    jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
    jarResource.setSize(destStatus.getLen());
    jarResource.setTimestamp(destStatus.getModificationTime());
    jarResource.setType(LocalResourceType.FILE);
    jarResource.setVisibility(LocalResourceVisibility.APPLICATION);
    localResources.put("app.jar", jarResource);

    // Add the app.jar to the Environment so it's available to Containers
    Map<String, String> env = new HashMap<>();
    String appJarDest = dest.toUri().toString();
    env.put("AMJAR", appJarDest);
    LOG.info("AMJAR environment variable is set to {}", appJarDest);
    env.put("AMJARTIMESTAMP", Long.toString(destStatus.getModificationTime()));
    env.put("AMJARLEN", Long.toString(destStatus.getLen()));

    amContainer.setLocalResources(localResources);
    amContainer.setEnvironment(env);

    // Configure the command line argument that launches the ApplicationMaster
    Vector<CharSequence> vargs = new Vector<>(30);
    vargs.add("yarn jar ./app.jar com.hortonworks.ApplicationMaster " + this.inputPath + " " + this.searchTerm + " "
        + this.outputFolder + " ");
    vargs.add("1>/tmp/TestAM.stdout");
    vargs.add("2>/tmp/TestAM.stderr");
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }
    List<String> commands = new ArrayList<>();
    commands.add(command.toString());
    amContainer.setCommands(commands);
    LOG.info("Command to execute ApplicationMaster = " + command);

    // Ask for resources in the ApplicationSubmissionContext
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(1024);
    // capability.setVirtualCores(1);
    appContext.setResource(capability);

    // Set the Container launch context with the ApplicationSubmissionContext
    appContext.setAMContainerSpec(amContainer);

    // Write the input folder metainfo to HDFS
    // processInputFolder();

    yarnClient.submitApplication(appContext);

    LOG.info("Application submitted with id = " + appId);

    return true;
  }

  public void processInputFolder() throws IOException {
    if (fs.isDirectory(inputPath)) {
      LOG.info("Input path {} is a folder.", jobInputFolder);
    }
    else {
      LOG.info("Input path {} is a file.", jobInputFolder);
      FileStatus fileStatus = fs.getFileStatus(inputPath);
      LOG.info("File status = " + fileStatus.toString());
      BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      for (BlockLocation block : blockLocations) {
        LOG.info("Block at {}", block.toString());
      }
    }
  }

  private void printClusterInfo() throws YarnException, IOException {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Cluster metrics received from RM. There are {} NodeManagers.", clusterMetrics.getNumNodeManagers());

    List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    for (NodeReport node : nodeReports) {
      LOG.info("Node ID = {}, address = {}, containers = {}", node.getNodeId(), node.getHttpAddress(), node.getNumContainers());
    }
    List<QueueInfo> queueList = yarnClient.getAllQueues();
    for (QueueInfo queue : queueList) {
      LOG.info("Available queue: {} with capacity {} to {}", queue.getQueueName(), queue.getCapacity(), queue.getMaximumCapacity());
    }

    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();

    LOG.info("Max memory = {} and max vcores = {}", maxMemory, maxVCores);
  }

}
