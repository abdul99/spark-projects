package com.abdul.bigdata.spark.partions.airline;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Partitioner;

public class AirlineCustomPartioner extends Partitioner implements Serializable {
	private int partitions;
	
	public AirlineCustomPartioner(int noOfPartitioners){
		partitions=noOfPartitioners; 
	}
	
	
	@Override
	public int getPartition(Object key) {
		String[] sa = StringUtils.splitPreserveAllTokens(key.toString(), ',');
		int y = (Integer.parseInt(sa[0])-1987);
		System.out.println("writing " + sa[0] + " " + y%partitions );
		return (y%partitions);
	}

	@Override
	public int numPartitions() {
		return partitions;
	}

}
