package bolt;

import conf.Parameters;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class UBolt implements IRichBolt {

	TopologyContext context;
	OutputCollector _collector;
	int myNumber;
	int DBoltNumber;

	private Parameters parameters;
	//	private boolean balance;
//	private String host;
//	private String head;
//	private int port = Parameters.REDIS_PORT;
	private transient Jedis jedis;

	Map<Integer, Integer> routingTable;

	public UBolt(Parameters parameters) {
		this.parameters = new Parameters(parameters);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		routingTable = new HashMap<>();
		this.context = context;
		_collector = collector;
		myNumber = context.getThisTaskIndex();
		DBoltNumber = context.getComponentTasks(Parameters.D_BOLT_NAME).size();
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();

////		if (balance) {
//		if (jedis.exists(parameters.getRedisHead() + Parameters.REDIS_U_WAIT + myNumber)) {
//			jedis.del(parameters.getRedisHead() + Parameters.REDIS_U_WAIT + myNumber);
//			try {
//				Thread.sleep(1000);
//			} catch (Exception e) {
//			}
//		}

		if (jedis.exists(parameters.getRedisHead() + Parameters.REDIS_RT + myNumber)) {
			// update routing table
			String routingInfo = jedis.get(parameters.getRedisHead() + Parameters.REDIS_RT + myNumber);
			Map<Integer, Integer> newRouting = new HashMap<>();
			String[] split = routingInfo.split("\t");
			for (String routing : split) {
				String[] item = routing.split(":");
				newRouting.put(Integer.parseInt(item[0]), Integer.parseInt(item[1]));
			}

			routingTable = newRouting;

			jedis.del(parameters.getRedisHead() + Parameters.REDIS_RT + myNumber);
		}
//		}

		String line = tuple.getString(0);
		String[] split = line.split(",");
		int key = Integer.parseInt(split[0]);
		int g = Integer.parseInt(split[1]);
		int taskID = context.getComponentTasks(Parameters.D_BOLT_NAME).get(routingTable.containsKey(key) ?
				routingTable.get(key) : key % DBoltNumber);

		_collector.emitDirect(taskID, tuple, new Values(key, g));
		_collector.ack(tuple);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "g"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	private Jedis getConnectedJedis() {
		if (jedis != null)
			return jedis;

		try {
			jedis = new Jedis(parameters.getHost(), Parameters.REDIS_PORT);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jedis;
	}
}
