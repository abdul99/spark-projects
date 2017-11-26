package com.abdul.bigdata.spark.partions.employee;

import java.io.Serializable;

public class Employee_Key implements Comparable<Employee_Key>, Serializable {

	private static final long serialVersionUID = 3232323;
	private String name;
	private String department;
	private double salary;

	public Employee_Key(String name, String department, double salary) {
		super();
		this.name = name;
		this.department = department;
		this.salary = salary;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDepartment() {
		return department;
	}

	public void setDepartment(String department) {
		this.department = department;
	}

	public double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

	@Override
	public int compareTo(Employee_Key emp1) {

		int compare = (int) emp1.getSalary() - (int) this.getSalary();

		if (compare == 0) {
			compare = this.getName().compareTo(emp1.getName());
		}

		return compare;

	}

}