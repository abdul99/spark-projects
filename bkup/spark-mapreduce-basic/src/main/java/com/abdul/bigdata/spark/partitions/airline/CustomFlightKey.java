package com.abdul.bigdata.spark.partitions.airline;

import java.io.Serializable;

 

public class CustomFlightKey implements Comparable<CustomFlightKey>, Serializable {
	
	private String UNIQUE_CARRIER;
	private String DEST_AIRPORT_ID;
	private double ARR_DELAY;
	
	
	public CustomFlightKey(String carrierId,String destAirportId,double arrDelay ) {
		this.UNIQUE_CARRIER = carrierId;
		this.DEST_AIRPORT_ID = destAirportId;
		this.ARR_DELAY =  arrDelay;
	}


	public String getUNIQUE_CARRIER() {
		return UNIQUE_CARRIER;
	}


	public void setUNIQUE_CARRIER(String uNIQUE_CARRIER) {
		UNIQUE_CARRIER = uNIQUE_CARRIER;
	}


	public String getDEST_AIRPORT_ID() {
		return DEST_AIRPORT_ID;
	}


	public void setDEST_AIRPORT_ID(String dEST_AIRPORT_ID) {
		DEST_AIRPORT_ID = dEST_AIRPORT_ID;
	}


	public double getARR_DELAY() {
		return ARR_DELAY;
	}


	public void setARR_DELAY(double aRR_DELAY) {
		ARR_DELAY = aRR_DELAY;
	} 

	
	/* @Override
	    public String toString() {
	        return "UNIQUE_CARRIER="  + UNIQUE_CARRIER + "," + 
	               "DEST_AIRPORT_ID="	+ DEST_AIRPORT_ID + "," + 
	               "ARR_DELAY="      + ARR_DELAY  ;
	        
	 }*/
	 
	 @Override
	    public String toString() {
	        return 
	               "DEST_AIRPORT_ID="	+ DEST_AIRPORT_ID + "," + 
	               "UNIQUE_CARRIER="  + UNIQUE_CARRIER + "," + 
	               "ARR_DELAY="      + ARR_DELAY  ;
	        
	 }


	/*@Override
	public int compareTo(CustomFlightKey flight) {
		int compare =  this.getUNIQUE_CARRIER().compareTo(flight.getUNIQUE_CARRIER());
		if(compare==0) {
			compare = this.getDEST_AIRPORT_ID().compareTo(flight.getDEST_AIRPORT_ID());
		}
		
    return compare;
	}*/
	
	
	@Override
	public int compareTo(CustomFlightKey flight) {
		int compare =  this.getDEST_AIRPORT_ID().compareTo(flight.getDEST_AIRPORT_ID());
		if(compare==0) {
			compare = this.getUNIQUE_CARRIER().compareTo(flight.getUNIQUE_CARRIER());
		}
		
    return compare;
	}
}
