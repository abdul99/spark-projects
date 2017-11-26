package com.abdul.bigdata.spark.partions;

import java.io.Serializable;

public class Employee_Value implements Serializable {

	private static final long serialVersionUID = 4343434;
	private String designation;
	private String lastname;
	private String jobtype;

	public Employee_Value(String designation, String lastname, String jobtype) {
		super();
		this.designation = designation;
		this.lastname = lastname;
		this.jobtype = jobtype;
	}

	public String getDesignation() {
		return designation;
	}

	public void setDesignation(String designation) {
		this.designation = designation;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getJobtype() {
		return jobtype;
	}

	public void setJobtype(String jobtype) {
		this.jobtype = jobtype;
	}

}
