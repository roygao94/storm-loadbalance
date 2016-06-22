package bolt;

import balancing.util.KGS;
import conf.Parameters;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by Roy Gao on 4/9/2016.
 */
public class DBolt implements IRichBolt {

	TopologyContext context;
	OutputCollector _collector;
	int myNumber;

	private Parameters parameters;
	//	private boolean balance;
//	private String host;
//	private String head;
//	private int port = Parameters.REDIS_PORT;
	private transient Jedis jedis;

	private Map<Integer, KGS> infoList = new HashMap<>();
	private int load;
	private int loadReportRound;
	private int detailReportRound;

	public DBolt(Parameters parameters) {
		this.parameters = new Parameters(parameters);
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.context = context;
		_collector = collector;
		myNumber = context.getThisTaskIndex();
		load = 0;
		loadReportRound = detailReportRound = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();

//		if (balance) {
		if (jedis.exists(parameters.getRedisHead() + Parameters.REDIS_LOAD + myNumber)) {
			// emit sum to Controller
			_collector.emitDirect(context.getComponentTasks(Parameters.CONTROLLER_NAME).get(0),
					new Values(Parameters.LOAD_REPORT + "-" + loadReportRound++, myNumber, load, ""));
			// jedis.lpush(Parameters.LOAD_REPORT + "-" + loadReportRound++, myNumber + "-" + load);
			load = 0;
			jedis.del(parameters.getRedisHead() + Parameters.REDIS_LOAD + myNumber);

//			while (!jedis.exists(parameters.getRedisHead() + Parameters.REDIS_DETAIL + myNumber)
//					&& !jedis.exists(parameters.getRedisHead() + Parameters.REDIS_D_CONTINUE + myNumber))
//				;
		} else if (jedis.exists(parameters.getRedisHead() + Parameters.REDIS_DETAIL + myNumber)) {
			// emit detail to Controller
			String detailInfo = getDetailInfo();
			_collector.emitDirect(context.getComponentTasks(Parameters.CONTROLLER_NAME).get(0),
					new Values(Parameters.DETAIL_REPORT + "-" + detailReportRound++, myNumber, load, detailInfo));
			infoList.clear();
			jedis.del(parameters.getRedisHead() + Parameters.REDIS_DETAIL + myNumber);
		}
//		else jedis.del(parameters.getRedisHead() + Parameters.REDIS_D_CONTINUE + myNumber);

//		}
		// record kgs info and put pressure
		int key = (int) tuple.getValue(0);
		int g = (int) tuple.getValue(1);

		recordInfo(key, g, 1);
		for (int i = 0; i < g; ++i)
			calculatePi();

		_collector.ack(tuple);
	}

	private String getDetailInfo() {
		String detailInfo = "";
		List<Map.Entry<Integer, KGS>> tempList = new ArrayList<>(infoList.entrySet());

		Collections.sort(tempList, new Comparator<Map.Entry<Integer, KGS>>() {
			@Override
			public int compare(Map.Entry<Integer, KGS> o1, Map.Entry<Integer, KGS> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});

		for (Map.Entry<Integer, KGS> entry : tempList)
			detailInfo += entry.getValue().getKey() + ","
					+ entry.getValue().getG() + ","
					+ entry.getValue().getS() + "\t";

		return detailInfo;
	}

	private void recordInfo(int key, int g, int s) {
		load += g;
		if (!infoList.containsKey(key))
			infoList.put(key, new KGS(key, g, s));
		else {
			infoList.get(key).addG(g);
			infoList.get(key).addS(s);
		}
	}

	private double calculatePi() {
		double pi = 0;
		for (int n = 0; n <= 500000; ++n)
			pi += (n % 2 == 0 ? 1 : -1) / (2 * n + 1);
		return pi;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("report-head", "id", "load", "detail"));
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
