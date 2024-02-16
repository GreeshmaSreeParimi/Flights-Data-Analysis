package spouts;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/** This is a spout which extends BaseRichSpout
 * It is intended to read flights data from a text file
 * and emits the each flight data to bolt(HubIdentifier)
 */
public class FlightsDataReader extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}
	public void close() {}
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}

	/**
	 * This method will call extractFlightData method 
	 * to read the flights data from the text file. 
	 * 
	 */
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */
		if(completed){
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
		//helper method to extract flights data from a text file.
		extractFlightsData();
	}

	/**
	 * Helper method which will be called from nextTuple
	 * It will read the flights data from a text file and emits
	 * each flight data to bolt(HubIdentifier).
	 */
	private void extractFlightsData() {
		try{
			//to parse a JSON file and create a JSONObject 
			JSONObject jsonData = 
				new JSONObject(new JSONTokener(this.fileReader));

			// Extract the 'states' array from the JSON data
			JSONArray statesArray = jsonData.getJSONArray("states");
			// Loop through each flight state in the 'states' array
			for (int i = 0; i < statesArray.length(); i++) {
				JSONArray flightState = statesArray.getJSONArray(i);
				String transponderAddr = flightState.getString(0);
				String callsign = flightState.getString(1);
				String originCountry = flightState.getString(2);
				long lastTimeStamp1 = flightState.isNull(3) ? 
					0 : flightState.getLong(3);
				long lastTimeStamp2 = flightState.isNull(4) ?
					0 : flightState.getLong(4);
				double longitude = flightState.isNull(5) ? 
					0 : flightState.getDouble(5);
				double latitude = flightState.isNull(6) ? 
					0 : flightState.getDouble(6);
				double altitudeBarometric = flightState.isNull(7) ? 
					0 : flightState.getDouble(7);
				boolean surfaceOrAir = flightState.isNull(8) ? 
					false : flightState.getBoolean(8);
				double velocity = flightState.isNull(9) ? 
					0 : flightState.getDouble(9);
				double degreeNorth = flightState.isNull(10) ? 
					0 : flightState.getDouble(10);
				double verticalRate = flightState.isNull(11) ? 
					0 : flightState.getDouble(11);
				double sensors = flightState.isNull(12) ? 
					0 : flightState.getDouble(12);
				double altitudeGeometric = flightState.isNull(13) ? 
					0 : flightState.getDouble(13);
				String transponderCode = flightState.isNull(14) ? 
					null : flightState.getString(14);
				boolean specialPurpose = flightState.isNull(15) ? 
					false : flightState.getBoolean(15);
				long origin = flightState.isNull(16) ? 
					0 : flightState.getLong(16); 
				
				// Emits each flights data to the HubIdentifier bolt. 
				this.collector.emit(new Values(transponderAddr,callsign,
				originCountry,lastTimeStamp1,lastTimeStamp2,longitude,
				latitude,altitudeBarometric,surfaceOrAir,velocity,
				degreeNorth,verticalRate,sensors,altitudeGeometric,
				transponderCode,specialPurpose,origin));

			}
		}catch(Exception e){
			throw new RuntimeException("Error reading flights data",e);
		}finally{
			completed = true;
		}

	}

	/**
	 * We will create the file reader to read flights data
	 * and get the collector object
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.fileReader = 
				new FileReader(conf.get("FlightsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["
				+conf.get("FlightsFile")+"]");
		}
		this.collector = collector;
	}

	/**
	 * Declare the output fields of all the 17 fields of each flight data
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("transponder address",
		"call sign","origin country","last timestamp 1",
		"last timestamp 2","longitude","latitude","altitude-barometric",
		"surface or air","velocity-m/s","degree north = 0",
		"vertical rate","sensors","altitude-geometric",
		"transponder code","special purpose","origin"));

	}
}
