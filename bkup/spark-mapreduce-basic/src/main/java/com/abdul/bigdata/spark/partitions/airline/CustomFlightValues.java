package com.abdul.bigdata.spark.partitions.airline;

import java.io.Serializable;

public class CustomFlightValues implements Serializable  {

	
	private String FL_DATE;
	
	private String ORIGIN_AIRPORT_ID;
	
	private String ORIGIN_CITY_MARKET_ID;
	
	private String DEST_CITY_MARKET_ID;
	
	
	public CustomFlightValues(String flightDate, String originAirportId, 
		                      	String originCityMarketId,String destCityMarketId) {
		      
	              this.FL_DATE = 	flightDate;
	              this.ORIGIN_AIRPORT_ID = originAirportId;
	              this.ORIGIN_CITY_MARKET_ID = originCityMarketId;
	              this.DEST_CITY_MARKET_ID =  destCityMarketId;
	}

	public String getFL_DATE() {
		return FL_DATE;
	}

	public void setFL_DATE(String fL_DATE) {
		FL_DATE = fL_DATE;
	}

	public String getORIGIN_AIRPORT_ID() {
		return ORIGIN_AIRPORT_ID;
	}

	public void setORIGIN_AIRPORT_ID(String oRIGIN_AIRPORT_ID) {
		ORIGIN_AIRPORT_ID = oRIGIN_AIRPORT_ID;
	}

	public String getORIGIN_CITY_MARKET_ID() {
		return ORIGIN_CITY_MARKET_ID;
	}

	public void setORIGIN_CITY_MARKET_ID(String oRIGIN_CITY_MARKET_ID) {
		ORIGIN_CITY_MARKET_ID = oRIGIN_CITY_MARKET_ID;
	}

	public String getDEST_CITY_MARKET_ID() {
		return DEST_CITY_MARKET_ID;
	}

	public void setDEST_CITY_MARKET_ID(String dEST_CITY_MARKET_ID) {
		DEST_CITY_MARKET_ID = dEST_CITY_MARKET_ID;
	}
	
	
	 @Override
	    public String toString() {
	        return "FL_DATE="  + FL_DATE + "," + 
	               "ORIGIN_AIRPORT_ID="	+ ORIGIN_AIRPORT_ID + "," + 
	               "ORIGIN_CITY_MARKET_ID="      + ORIGIN_CITY_MARKET_ID + "," + 
	               "DEST_CITY_MARKET_ID="      + DEST_CITY_MARKET_ID  ;
	               
	               
	               
	               
	        
	 }
	 
	 
	
}
