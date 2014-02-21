package com.hortonworks;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.Records;

public class JobMonitor implements Runnable {
	private static final Log LOG = LogFactory.getLog(JobMonitor.class);

	private ApplicationMaster am;
	private Map<String,Boolean> blockMap;

	public JobMonitor(ApplicationMaster am) {
		this.am = am;
	}
	
	@Override
	public void run() {
		BlockLocation [] blocks = null;
		try {
			blocks = am.getBlockLocations();
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("JobMonitor unable to retrieve block locations from ApplicationMaster");
			return;
		}

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
		LOG.info("Attempting to allocate " + numOfContainers + " containers...");
		int allocatedContainers = 0;
		while (allocatedContainers < numOfContainers) {
			AllocateResponse response = resourceManager.allocate(allocatedContainers);
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

}
