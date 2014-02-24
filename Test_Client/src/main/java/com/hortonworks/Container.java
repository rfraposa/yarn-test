package com.hortonworks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class Container {
	private static final Log LOG = LogFactory.getLog(Container.class);

	private Path inputFile;
	private long start;
	private int length;
	private FileSystem fs;
	private YarnConfiguration conf;
	private String hostname;
	private Path outputFile;
	List<String> results;
	private String searchTerm;
	
	public Container(String [] args) throws IOException {
		hostname = NetUtils.getHostname();
		inputFile = new Path(args[0]);
		start = Long.valueOf(args[1]);
		length = Integer.valueOf(args[2]);
		searchTerm = args[3];
		outputFile = new Path("/user/yarn/output/data_" + start);
	}

	public static void main(String[] args) {
		LOG.info("Container just started on " + NetUtils.getHostname());
		Container container;
		try {
			container = new Container(args);
			container.run();
		} catch (Exception e) {
			LOG.info("Eror running Container on " + NetUtils.getHostname());
			e.printStackTrace();
		}
		
		LOG.info("Container is ending...");
	}

	private  void run() throws IOException {
		LOG.info("Running Container on " + this.hostname);
		FSDataInputStream in = null;

		try {
			conf = new YarnConfiguration();
			fs = FileSystem.get(conf);
			fs.delete(outputFile, false);
			
			//in = fs.open(this.inputFile);
			FSDataInputStream fsdis = fs.open(inputFile);
			fsdis.seek(this.start);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsdis));
			LOG.info("Reading from " + start + " to " + length + " from " + inputFile.toString());
			String current = "";
			results = new ArrayList<String>(5000);
			long bytesRead = 0;
			while(bytesRead < this.length && (current = reader.readLine()) != null) {
				bytesRead += current.getBytes().length;
				results.add(current);
			}
			
			LOG.info("Just read " + results.size() + " lines from " + this.inputFile + " on host " + NetUtils.getHostname());
		} catch (IOException e) {
			LOG.info("Unable to open file at " + inputFile.toString());
			e.printStackTrace();
			return;
		} 
		LOG.info("Writing bytes to HDFS...");
		fs.create(outputFile);
		FSDataOutputStream fsout = fs.create(outputFile, true);
		//FileOutputStream fsout = new FileOutputStream("/tmp/container.txt");
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
		try {
			for(String current : results) {
				if(current.contains(searchTerm)) {
					writer.write(current + "\n");
				}
			}
		} finally {
			writer.close();
		}
	}

}
