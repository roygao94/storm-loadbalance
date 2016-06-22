import bolt.DBolt;
import bolt.UBolt;
import conf.Parameters;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import spout.RedisQueueSpout;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class SimpleTopology {

	public static void main(String[] args) throws Exception {
		Parameters parameters = new Parameters();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RedisQueueSpout(parameters), 1);

		builder.setBolt("u-bolt",
				new UBolt(parameters), 10).shuffleGrouping("spout");
		builder.setBolt("d-bolt",
				new DBolt(parameters), 10).directGrouping("u-bolt");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(10);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("simple-topo", conf, builder.createTopology());

			Thread.sleep(30000);

			cluster.shutdown();
		}
	}
}
