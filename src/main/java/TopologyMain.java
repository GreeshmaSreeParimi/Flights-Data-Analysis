import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.AirlineSorter;
import bolts.HubIdentifier;
import spouts.FlightsDataReader;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		//Set the "flight-data-reader" spout
		builder.setSpout("flight-data-reader",new FlightsDataReader());
		// Set the "hub-identifier" bolt 
		builder.setBolt("hub-identifier", new HubIdentifier(),1)
			.shuffleGrouping("flight-data-reader");
		// Set the "airline-sorter" bolt
		builder.setBolt("airline-sorter", new AirlineSorter(),1)
			.fieldsGrouping("hub-identifier", new Fields("airport.city"));
		
        //Configuration
		Config conf = new Config();
		/**Set the FlightsFile and AirportsData properties 
		 * based on the command line arguments */
		conf.put("FlightsFile", args[0]);
		conf.put("AirportsData", args[1]);
		conf.setDebug(false);
		
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		
		// Analysis start time.
		long start = System.currentTimeMillis( );
		System.out.println("************** Start Time :" 
			+ start + " ******************");

		/**Submit the topology "Flight-Analysis-Topology" 
		to the local cluster with the provided configuration */
		cluster.submitTopology("Flight-Analysis-Toplogie", 
			conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
	}
}
