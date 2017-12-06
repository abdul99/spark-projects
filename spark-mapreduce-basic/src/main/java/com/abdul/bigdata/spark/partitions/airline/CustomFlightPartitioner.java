package com.abdul.bigdata.spark.partitions.airline;

import com.abdul.bigdata.spark.partions.employee.CustomEmployeePartitioner;

public class CustomFlightPartitioner extends org.apache.spark.Partitioner {

	private static final long serialVersionUID = 343434;
	private int numPartitions;

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}

	public CustomFlightPartitioner(int numPartitions) {
		super();
		this.numPartitions = numPartitions;
	}

	@Override
	public int getPartition(Object arg0) {

		 CustomFlightKey flight = (CustomFlightKey) arg0;
		// int partition = flight.getDEST_AIRPORT_ID().hashCode() % getNumPartitions();
    	 int partition = Math.abs(flight.getDEST_AIRPORT_ID().hashCode() % getNumPartitions());
	//	 System.out.println("writing " + flight.getDEST_AIRPORT_ID() + " " + partition ); 
		 return partition;
 	 
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CustomFlightPartitioner) {
			CustomFlightPartitioner partitionerObject = (CustomFlightPartitioner) obj;
			if (partitionerObject.getNumPartitions() == this.getNumPartitions())
				return true;
		}

		return false;
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return getNumPartitions();
	}
 

}