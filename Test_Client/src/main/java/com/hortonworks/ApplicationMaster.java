package com.hortonworks;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMaster {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);
  private YarnConfiguration conf;

  private AMRMClientAsync<ContainerRequest> amRMClient;

  private NMClientAsync nmClientAsync;
  private NMCallbackHandler containerListener;

  private FileSystem fileSystem;
  private Path inputFile;
  private List<BlockStatus> blockList;
  private String searchTerm;
  private int numOfContainers;
  private String outputFolder;
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  private volatile boolean done;

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();

  public ApplicationMaster(String[] args) throws IOException {
    conf = new YarnConfiguration();

    fileSystem = FileSystem.get(conf);
    inputFile = new Path(args[0]);
    this.searchTerm = args[1];
    outputFolder = args[2];

    blockList = new ArrayList<>();

    Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class);
  }

  public static void main(String[] args) {
    ApplicationMaster appMaster = null;
    try {
      appMaster = new ApplicationMaster(args);
      appMaster.run();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public boolean run() throws YarnException, IOException, URISyntaxException {
    LOG.info("Running ApplicationMaster...");

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    containerListener = new NMCallbackHandler(this);
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    // Register this ApplicationMaster with the ResourceManager
    String appHostName = NetUtils.getHostname();
    int appHostPort = -1;
    String appTrackingUrl = "";
    RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);
    LOG.info("ApplicationMaster is registered with response: {}", response.toString());

    BlockLocation[] blocks = this.getBlockLocations();

    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);
    Resource capacity = Records.newRecord(Resource.class);
    capacity.setMemory(2048);

    numOfContainers = 0;
    for (BlockLocation block : blocks) {
      ContainerRequest ask = new ContainerRequest(capacity, block.getHosts(), null, priority, false);
      for (String host : block.getHosts()) {
        System.out.println(host + " for block " + block.toString());
      }
      LOG.info("Asking for Container for block {}", block.toString());
      numOfContainers++;
      amRMClient.addContainerRequest(ask);
      blockList.add(new BlockStatus(block));
    }

    while (!done && (numCompletedContainers.get() != numOfContainers)) {
      try {
        Thread.sleep(200);
      }
      catch (InterruptedException ex) {
      }
    }

    finish();

    return true;
  }

  private void finish() {
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      }
      catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: {}", e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    String appMessage = "Application Complete!";
    try {
      amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, appMessage, null);
    }
    catch (YarnException | IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRMClient.stop();
  }

  private class LaunchContainerRunnable implements Runnable {

    private Container container;
    private NMCallbackHandler containerListener;

    public LaunchContainerRunnable(Container container, NMCallbackHandler containerListener) {
      this.container = container;
      this.containerListener = containerListener;
    }

    @Override
    public void run() {
      try {
        LOG.info("Setting up container launch container for containerid = {}", container.getId());
        // Each Container needs the application JAR file, which is in HDFS via
        // the AMJAR environment variable
        Map<String, LocalResource> localResources = new HashMap<>();

        LocalResource appJarFile = Records.newRecord(LocalResource.class);
        appJarFile.setType(LocalResourceType.FILE);
        appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
        Map<String, String> env = System.getenv();
        appJarFile.setResource(ConverterUtils.getYarnUrlFromURI(new URI(env.get("AMJAR"))));
        appJarFile.setTimestamp(Long.valueOf((env.get("AMJARTIMESTAMP"))));
        appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
        localResources.put("app.jar", appJarFile);
        LOG.info("Added {} as a local resource to each Container", appJarFile.toString());

        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setLocalResources(localResources);

        String command = getLaunchContainerCommand(container);

        List<String> commands = new ArrayList<>();
        commands.add(command);
        context.setCommands(commands);
        LOG.info("Command to execute Container = {}", command);

        containerListener.addContainer(container.getId(), container);
        nmClientAsync.startContainerAsync(container, context);

        LOG.info("Container {} launched!", container.getId());
      }
      catch (Exception e) {
        e.printStackTrace();
      }

    }
  }

  /**
   * Given a hostname, this method determines which block this Container should
   * process attempting to apply data locality as much as possible
   * 
   * @param container
   *          the Container that is about to be started
   * @return the command to be executed on this new Container
   * @throws IOException
   */
  private String getLaunchContainerCommand(Container container) throws IOException {
    String hostname = container.getNodeHttpAddress();
    boolean foundContainer = false;
    BlockStatus blockToProcess = null;

    // Find a BlockStatus that needs to be processed...
    outer: for (BlockStatus current : blockList) {
      synchronized (current) {
        if (!current.isStarted()) {
          for (int i = 0; i < current.getLocation().getHosts().length; i++) {
            String currentHost = current.getLocation().getHosts()[i] + ":8042";
            LOG.info("Comparing {} with container on {}", currentHost, hostname);
            if (currentHost.equals(hostname)) {
              // Assign this BlockStatus to this Container
              blockToProcess = current;
              current.setStarted(true);
              current.setContainer(container);
              foundContainer = true;
              break outer;
            }
          }
        }
      }
    }
    if (foundContainer) {
      LOG.info("Data Locality achieved!!!");
    }
    if (!foundContainer) {
      // Just find any block to process
      LOG.info("Data locality not found - trying another node");
      for (BlockStatus current : blockList) {
        if (!current.isStarted()) {
          blockToProcess = current;
          current.setStarted(true);
          current.setContainer(container);
          foundContainer = true;
          break;
        }
      }
    }
    if (foundContainer) {
      LOG.info("Processing block from {} on a Container running on {}", blockToProcess.getLocation(), container.getNodeHttpAddress());
    }
    else {
      LOG.error("No container found to handle block!");
      return "sleep 5";
    }

    // Configure the command line argument that launches the Container
    Vector<CharSequence> vargs = new Vector<>(30);
    vargs.add("yarn jar ./app.jar com.hortonworks.Container ");
    vargs.add(inputFile.toString()); // File to read
    String offsetString = Long.toString(blockToProcess.getLocation().getOffset());
    String lengthString = Long.toString(blockToProcess.getLocation().getLength());
    LOG.info("Reading block starting at {} and length {}", offsetString, lengthString);
    vargs.add(offsetString); // Offset into the file
    vargs.add(lengthString); // Number of bytes to read
    vargs.add(this.searchTerm); // The term we are searching for
    vargs.add(this.outputFolder); // Folder in HDFS to store results

    vargs.add("1><LOG_DIR>/TestContainer.stdout");
    vargs.add("2><LOG_DIR>/TestContainer.stderr");
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    return command.toString();
  }

  public BlockLocation[] getBlockLocations() throws IOException {
    // Read the block information from HDFS
    FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
    LOG.info("File status = {}", fileStatus.toString());
    BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    LOG.info("Number of blocks for {} = {}", inputFile.toString(), blocks.length);
    return blocks;
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completed count = {}", completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info("Got container status for containerID = {}, state = {}, exitStatus = {}, diagnostics = {}",
            containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(),
            containerStatus.getDiagnostics());
        numCompletedContainers.incrementAndGet();

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);

        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // TODO: retry failed containers
            LOG.info("Container failed, containerId = {}", containerStatus.getContainerId());
          }
        }
        else {
          LOG.info("Container completed successfully, containerId = {}", containerStatus.getContainerId());
        }
      }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocated count = {}", allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        LOG.info("Launching new container: containerId = {}, containerNode = {}:{}, containerResourceMemory = {}",
            allocatedContainer.getId(), allocatedContainer.getNodeId().getHost(), allocatedContainer.getNodeId().getPort(),
            allocatedContainer.getResource().getMemory());

        LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener);
        Thread launchThread = new Thread(runnableLaunchContainer);

        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchThread.start();
      }
    }

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
    }

    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = numOfContainers <= 0 ? 0 : (float) numCompletedContainers.get() / numOfContainers;
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      done = true;
      amRMClient.stop();
    }
  }

  static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

    private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
    private final ApplicationMaster applicationMaster;

    public NMCallbackHandler(ApplicationMaster applicationMaster) {
      this.applicationMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
      containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      LOG.debug("Succeeded to stop Container {}", containerId);
      containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOG.debug("Container Status: id = {}, status = {}", containerId, containerStatus);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      LOG.debug("Succeeded to start Container {}", containerId);
      Container container = containers.get(containerId);
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container {}", containerId);
      containers.remove(containerId);
      applicationMaster.numCompletedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container {}", containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container {}", containerId);
      containers.remove(containerId);
    }
  }
}
