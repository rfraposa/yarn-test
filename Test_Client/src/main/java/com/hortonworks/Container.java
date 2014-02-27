package com.hortonworks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Container {
  private static final Logger LOG = LoggerFactory.getLogger(Container.class);

  private Path inputFile;
  private long start;
  private int length;
  private FileSystem fs;
  private YarnConfiguration conf;
  private String hostname;
  private Path outputFile;
  ArrayList<String> results;
  private String searchTerm;

  private String outputFolder;

  public Container(String[] args) throws IOException {
    hostname = NetUtils.getHostname();
    inputFile = new Path(args[0]);
    start = Long.valueOf(args[1]);
    length = Integer.valueOf(args[2]);
    searchTerm = args[3];
    outputFolder = args[4];
    outputFile = new Path(this.outputFolder + "/result_" + start);

    Log4jPropertyHelper.updateLog4jConfiguration(Container.class);
  }

  public static void main(String[] args) {
    LOG.info("Container just started on {}", NetUtils.getHostname());
    Container container;
    try {
      container = new Container(args);
      container.run();
    }
    catch (Exception e) {
      LOG.info("Error running Container on {}", NetUtils.getHostname());
      e.printStackTrace();
    }

    LOG.info("Container is ending...");
  }

  private void run() throws IOException {
    LOG.info("Running Container on {}", this.hostname);
    long bytesRead = 0;
    try {
      conf = new YarnConfiguration();
      fs = FileSystem.get(conf);
      fs.delete(outputFile, false);

      // in = fs.open(this.inputFile);
      FSDataInputStream fsdis = fs.open(inputFile);
      fsdis.seek(this.start);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fsdis));
      LOG.info("Reading from {} to {} from {}", start, start + length, inputFile.toString());
      String current = "";
      results = new ArrayList<String>();

      while (bytesRead < this.length && (current = reader.readLine()) != null) {
        bytesRead += current.getBytes().length;
        if (current.contains(searchTerm)) {
          results.add(current);
        }
      }

      LOG.info("Just found {} entries from {} on host {}", results.size(), this.inputFile, NetUtils.getHostname());
    }
    catch (IOException e) {
      LOG.info("Unable to open file at {}", inputFile.toString());
      e.printStackTrace();
      return;
    }

    if (results.size() > 0) {
      LOG.info("Writing results to HDFS...");
      FSDataOutputStream fsout = fs.create(outputFile, true);
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));) {
        for (String current : results) {
          writer.write(current + "\n");
        }
      }
    }
    else {
      LOG.info("Search term not found in current block at {}", this.start);
    }
  }

}
