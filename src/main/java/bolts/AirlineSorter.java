package bolts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/** This bolt is reposible for sorting flights data
 * according to aiport and print the output
 */
public class AirlineSorter extends BaseBasicBolt {

	Integer id;
	String name;
	HashMap<String, HashMap<String,Integer>> airportsMap;
	HashMap<String,String> flightDataSet;

	/**
	 * This method print the ouput
	 * for each airport , the count of each flight that
	 * is near or in proximity to the aiport is displayed.
	 */
	@Override
	public void cleanup() {
		/**this method creates a dataset of 
		 * airlines name w.r.t airline code.*/
		CreateFlightDataSet();
		System.out.println("-- Name ::: ["+name
			+"- TASK ID ::: "+id+"] --");
		
		// To print each aiport data
		for(Map.Entry<String, HashMap<String,Integer>> entry : 
			airportsMap.entrySet()){
			
			System.out.println("At Airport: " + entry.getKey());
			HashMap<String, Integer> flightsMap = entry.getValue();

			List<Map.Entry<String, Integer>> sortedFlightsData = 
				sortflights(flightsMap);
			int NumberOfFlights = 0;
			
			/** for each airport, to display each airline and flights count */
			for (Map.Entry<String, Integer> flight : sortedFlightsData) {
				String flightCode = flight.getKey();
				String flightName;
				if(flightDataSet.containsKey(flightCode)){
					flightName = flightDataSet.get(flightCode);
				}else{
					flightName = "Not-Available";
				}
				Integer flightCount = flight.getValue();
				NumberOfFlights += flightCount;
				System.out.println("\t"+flightCode + " (" 
					+flightName +")" + " : " + flightCount);
			}
			// to print total flighst for each airport
			System.out.println("\ttotal #flights = " + NumberOfFlights);

		}
		// Analysis finish time
		long finish = System.currentTimeMillis( );
		System.out.println("************* Finish Time :" 
			+ finish + " ***************");
	}

	/** This method sorts flights data of each aiport using the flight call Sign
	 * @param HashMap<String, Integer> flightsMap : this is a map which
	 * contains each number of flights for each call Sign.
	 * returns list of sorted key value pairs of flights data.
	 */
	private List<Entry<String, Integer>> sortflights(
			HashMap<String, Integer> flightsMap) {
		List<Map.Entry<String, Integer>> sortedFlightsData = 
			new ArrayList<Map.Entry<String, Integer>>(flightsMap.entrySet());
    	
			Collections.sort(sortedFlightsData, 
				new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Map.Entry<String, Integer> 
				f1, Map.Entry<String, Integer> f2) {
				return f2.getValue().compareTo(f1.getValue());
			}
    	});
		return sortedFlightsData;
	}

	/**
	 * On create 
	 * this method intitalizes hasmap to sort flights data according airport
	 * also initializes name of component and task ID
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.airportsMap = new HashMap<String, HashMap<String,Integer>>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	/** this method creates a Map based on airport
	 *  data and flights call sign */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String airportCity = input.getStringByField("airport.city");
		String airportCode = input.getStringByField("airport.code");
		String callSign = input.getStringByField("call sign");

		// key for the aiport map
		synchronized(this){
			String airportKey = airportCode + '(' + airportCity + ')';
			// map to store airport data
			if(!airportKey.isEmpty() && !callSign.isEmpty()){
				if(!airportsMap.containsKey(airportKey)){
					airportsMap.put(airportKey,
						new HashMap<String,Integer>());
				}
				// inner map to store number of flights for each airline
				HashMap<String,Integer> flightsMap = 
					airportsMap.get(airportKey);
				flightsMap.put(callSign,
					flightsMap.getOrDefault(callSign, 0)+1);
				airportsMap.put(airportKey,flightsMap);
			}
		}
		
		
	}
	/** As part of additional information
	 * this method maps  each flightCode with airlines name
	 */
	public void CreateFlightDataSet(){
		flightDataSet = new HashMap<String,String>();
		flightDataSet.put("LOT","Polish");
		flightDataSet.put("UAL","United");
		flightDataSet.put("RPA","Republic");
		flightDataSet.put("AIJ","Interjet");
		flightDataSet.put("AAL","American");
		flightDataSet.put("SWA","Southwest");
		flightDataSet.put("N21","Holly Ridge/Topsail Island");
		flightDataSet.put("EJA","NETJETS");
		flightDataSet.put("SKW","SkyWest");
		flightDataSet.put("DAL","Delta");
		flightDataSet.put("ASQ","Atlantic SouthEast");
		flightDataSet.put("VOI","Volaris");
		flightDataSet.put("FFT","Frontier");
		flightDataSet.put("N28","Minsk");
		flightDataSet.put("RCH","Almirante Padilla");
		flightDataSet.put("ENY","Envoy Air");
		flightDataSet.put("AFL","Aeroflot");
		flightDataSet.put("SWR","Swiss ");
		flightDataSet.put("N35","Punxsutawney Muni");
		flightDataSet.put("UCA","CommuteAir");
		flightDataSet.put("EDV","Endeavor Air");
		flightDataSet.put("NKS","Spirit");
		flightDataSet.put("PWA","Priester");
		flightDataSet.put("JBU","JetBlue");
		flightDataSet.put("GJS","GoJet");
		flightDataSet.put("N17","Vaughn Municipal");
		flightDataSet.put("N82","Wurtsboro–Sullivan County");
		flightDataSet.put("N86","Spanish Springs");
		flightDataSet.put("N42","Shippensburg");
		flightDataSet.put("N75","Twin-pine");
		flightDataSet.put("BAW","British");
		flightDataSet.put("ROU","Air Canada Rouge");
		flightDataSet.put("VIR","Virgin Atlantic");
		flightDataSet.put("N52","Jaars Townsend");
		flightDataSet.put("N74","Penns Cave");
		flightDataSet.put("N63","Meadow Brook");
		flightDataSet.put("N98","Boyne City");
		flightDataSet.put("N44","Sky Acres");
		flightDataSet.put("N53","Stroudsburg–Pocono");
		flightDataSet.put("N56","Great Valley");
		flightDataSet.put("N89","Resnick");
		flightDataSet.put("N19","Aztec");
		flightDataSet.put("N81","Hammonton");
		flightDataSet.put("PTP","Pointe-a-Pitre");
		flightDataSet.put("PTP","Pointe-a-Pitre");
		flightDataSet.put("N27","Bradford");
		flightDataSet.put("810","Not-Avilable");
		flightDataSet.put("SDU","Sud Airlines");
		flightDataSet.put("N50","Li Calzi");
		flightDataSet.put("S76","Millard");
		flightDataSet.put("N31","Kutztown");
		flightDataSet.put("N77","Mahopac");
		flightDataSet.put("N99","Brandywine");
		flightDataSet.put("N13","Bloomsburg");
		flightDataSet.put("N16","Centre Airpark");
		flightDataSet.put("N38","Grand Canyon");
		flightDataSet.put("N15","Kingston");
		flightDataSet.put("N37","Half Moon Bay");
		flightDataSet.put("N48","Ingolstadt Manching");
		flightDataSet.put("JAL","Japan");
		flightDataSet.put("BAD","Barksdale Afb");
		flightDataSet.put("KAL","korean");
		flightDataSet.put("N11","Beja Airbase");
		flightDataSet.put("JIA","PSA Airlines");
		flightDataSet.put("FDX","Fedex");
		flightDataSet.put("JZA","Jazz Aviation");
		flightDataSet.put("LOF","Trans States");
		flightDataSet.put("N85","Alexandria");
		flightDataSet.put("N73","Red Lion");
		flightDataSet.put("N97","Hiatt");
		flightDataSet.put("DLH","Lufthansa");
		flightDataSet.put("N92","Laneys");
		flightDataSet.put("N51","Solberg–Hunterdon");
		flightDataSet.put("N12","Lakewood");
		flightDataSet.put("N67","Wings");
		flightDataSet.put("ASA","Alaska");
		flightDataSet.put("N36","Wotje");
		flightDataSet.put("CAP","Cap Haitien");
		flightDataSet.put("QXE","Horizon Air");
		flightDataSet.put("N29","Magdalena");
		flightDataSet.put("N72","Warwick");
		flightDataSet.put("MLN","Melilla");
		flightDataSet.put("AFR","Air France");
		flightDataSet.put("COO","Cotonou Cadjehoun");
		flightDataSet.put("PHM","Boeblingen");
		flightDataSet.put("CGH","Congonhas Sao Paulo");
		flightDataSet.put("EDG","Jet Edge");
		flightDataSet.put("CPZ","Compass");
		flightDataSet.put("WJA","WestJet");
		flightDataSet.put("OPT","Bucharest Henri Coandă");
		flightDataSet.put("ASH","Mesa");
		flightDataSet.put("OXF","London Oxford");
		flightDataSet.put("NDU","Rundu");
		flightDataSet.put("CMP","Copa");
		flightDataSet.put("KLM","KLM Royal Dutch");
		flightDataSet.put("CAL","China");
		flightDataSet.put("ANA","All Nippon");
		flightDataSet.put("AVA","Avianca");
		flightDataSet.put("GTI","Atlas Air");
		flightDataSet.put("CHH","Hainan");
		flightDataSet.put("KAP","Cape Air");
		flightDataSet.put("BKN","Balkanabat");
		flightDataSet.put("BOE","Boeing");
		flightDataSet.put("EMD","Emerald");
		flightDataSet.put("GAJ","Gama");
		flightDataSet.put("PJC","Pedro Juan Caballero");
		flightDataSet.put("LXJ","Flexjet");
		flightDataSet.put("BOE","Boeing");
	}
}
