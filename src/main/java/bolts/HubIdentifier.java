package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;

/** This bolt HubIdentifier is responsible for
 * resding airports data from text file & 
 * identifying flights near to each aiport and emit 
 * that data to next bolt(AirlineSorter)
 */
public class HubIdentifier extends BaseBasicBolt {
	private FileReader fileReader;
	String name;
	Integer id;
	List<Airport> airports = new ArrayList<Airport>();

	public void cleanup() {}

	/**
	 * On create
	 * this method will intialize a file Reader 
	 * to read Airports Data and call 
	 * extractAirportsData method to retrieve data
	 */
	@Override
	public void prepare(Map conf, TopologyContext context) {
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		System.out.println("-- Name ::: ["
			+name+"- TASK ID ::: "+id+"] --");
		try {
			this.fileReader = 
				new FileReader(conf.get("AirportsData").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["
				+conf.get("AirportsData")+"]");
		}
		extractAirportsData();

	}

	/** This method will read file line by line 
	 * and extracts airports data and store in 
	 * a list.
	 */
	private void extractAirportsData() {
		String strLine;
		BufferedReader reader = new BufferedReader(fileReader);
		Airport airport;
		System.out.println("**** Aiports Data ******");
		try{
			//Read each airport data from each line
			while((strLine = reader.readLine()) != null){
				if(strLine.isEmpty()) continue;
				String[] lineArr = strLine.split(",");
				
				//creates an airport object
				airport = new Airport(lineArr[0], lineArr[1], 
					Double.parseDouble(lineArr[2]), 
					Double.parseDouble(lineArr[3]));
				
					
				System.out.println(airport.airportName + ", " + 
					airport.airportCode +", " + airport.latitude 
					+ ", " + airport.longitude);
				// stores aiport object in list of airports
				airports.add(airport);
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading aiports data",e);
		}finally{
		}
	}

	/**
	 * The method will receive each flight data.
	 * It identifies flights which are near to 
	 * any of the airports and emits it next bolt
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {

        String callSign = input.getStringByField("call sign");
		Double latitude = input.getDoubleByField("latitude");
		Double longitude = input.getDoubleByField("longitude");
		// if call sign of flight is empty, discard that flight
		if(callSign.isBlank()){
			return;
		}


		int NumberOfAirports = airports.size();
		double latThreshold = 0.2857; // latitude value for 20 miles 
		double longThreshold = 0.4444; // longitude value for 20 miles 
		
		/** For each flight data, it checks with latitude 
		 * and longitude of each airport to identify 
		 * if the current flight is near to any aiport 
		 * of the list of airports*/ 

        for(int i=0;i<NumberOfAirports;i++){
			Airport airport = airports.get(i);
			double latRangeMin = airport.latitude - latThreshold;
			double latRangeMax = airport.latitude + latThreshold;
			double longRangeMin = airport.longitude - longThreshold;
			double longRangeMax = airport.longitude + longThreshold;

			if((latitude <= latRangeMax && latitude >= latRangeMin) &&
				(longitude <= longRangeMax && longitude >= longRangeMin)){
					System.out.println("Hub Identifier Execeute::: flight "
					+ callSign + "is close to "+ airport.airportName 
					+ " (" +airport.airportCode + ")");
					collector.emit(new Values(airport.airportName,
						airport.airportCode,callSign.substring(0, 3)));
			}
		}
	}
	

	/**
	 * The bolt will only emit airport.city, airport.code and 
	 * call sign fields. 
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("airport.city",
			"airport.code","call sign"));
	}

	/** This ia an Airport class to store data of 
	 * the each airport
	 */
	private class Airport implements Serializable{
		String airportName;
		String airportCode;
		double latitude;
		double longitude;

		/**Constructor to create an aiport object
		 * @param String aiportCityName
		 * @param String airportCode
		 * @param double aiportLatitide
		 * @param double aiportLongitude
		 */
		public Airport(String aiport_name, String airport_code, 
			double air_latitude, double air_longitude){
			this.airportName = aiport_name;
			this.airportCode = airport_code;
			this.latitude = air_latitude;
			this.longitude = air_longitude;
		}
	}
}
