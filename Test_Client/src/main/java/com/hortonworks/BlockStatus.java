package com.hortonworks;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.yarn.api.records.Container;

public class BlockStatus {
  private BlockLocation location;
  private Container container;
  private boolean processed;
  private boolean started;

  public BlockStatus(BlockLocation location) {
    this.location = location;
  }

  public Container getContainer() {
    return container;
  }

  public void setContainer(Container container) {
    this.container = container;
  }

  public boolean isProcessed() {
    return processed;
  }

  public void setProcessed(boolean processed) {
    this.processed = processed;
  }

  public boolean isStarted() {
    return started;
  }

  public void setStarted(boolean started) {
    this.started = started;
  }

  public BlockLocation getLocation() {
    return location;
  }
}
