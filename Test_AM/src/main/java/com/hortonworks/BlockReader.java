package com.hortonworks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.mortbay.log.Log;

public class BlockReader {

	private String blockId;
	private String hostName;
	private String pathToBlock;
	
	public BlockReader(String blockId) {
		this.blockId = blockId;
		pathToBlock = "/hadoop/hdfs/data/current/BP-341950454-172.17.0.2-1392831955182/current/finalized/blk_" + blockId;
	}

	public static void main(String[] args) {
		BlockReader reader = new BlockReader(args[0]);
		try {
			reader.run();
		} catch (IOException e) {
			Log.info("Error occured during the reading of block " + reader.blockId + " on host " + reader.hostName);
			e.printStackTrace();
		}
	}

	public void run() throws IOException {
		FileReader fr = new FileReader(pathToBlock);
		BufferedReader in = new BufferedReader(fr,1048576);
		String currentLine = null;
		Log.info("Reading lines from " + pathToBlock);
		while((currentLine = in.readLine()) != null) {
			CharSequence s = "AB";
			if(currentLine.contains(s )) {
				System.out.println(currentLine);
			}
		}
	}
}
