package com.abdul.bigdata.spark.partions;

public class CustomEmployeePartitioner extends org.apache.spark.Partitioner {

	private static final long serialVersionUID = 343434;
	private int numPartitions;

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}

	public CustomEmployeePartitioner(int numPartitions) {
		super();
		this.numPartitions = numPartitions;
	}

	@Override
	public int getPartition(Object arg0) {

		Employee_Key emp = (Employee_Key) arg0;

		return Math.abs(emp.getDepartment().hashCode() % getNumPartitions());
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return getNumPartitions();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CustomEmployeePartitioner) {
			CustomEmployeePartitioner partitionerObject = (CustomEmployeePartitioner) obj;
			if (partitionerObject.getNumPartitions() == this.getNumPartitions())
				return true;
		}

		return false;
	}

}