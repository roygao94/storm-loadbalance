package bolt;

import balancing.BalanceInfo;
import balancing.Balancer;
import balancing.util.NodeWithCursor;
import balancing.util.Pair;
import conf.Parameters;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roy Gao on 4/13/2016.
 */
public class Controller implements IRichBolt {

	TopologyContext context;
	OutputCollector _collector;
	int UBoltNumber;
	int DBoltNumber;

	private Parameters parameters;
	//	private boolean balance;
//	private String host;
//	private String head;
//	private int port = Parameters.REDIS_PORT;
	private transient Jedis jedis;

	private Map<Integer, Integer> loadList;
	private Map<Integer, NodeWithCursor> detailList;
	private int loadReportRound;
	private int detailReportRound;

	public Controller(Parameters parameters) {
		this.parameters = new Parameters(parameters);
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.context = context;
		_collector = collector;
		UBoltNumber = context.getComponentTasks(Parameters.U_BOLT_NAME).size();
		DBoltNumber = context.getComponentTasks(Parameters.D_BOLT_NAME).size();

		loadList = new HashMap<>();
		detailList = new HashMap<>();
		loadReportRound = detailReportRound = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();
		String reportHead = tuple.getValue(0).toString();

		if (reportHead.equals(Parameters.LOAD_REPORT + "-" + loadReportRound)) {
			// receive summary report from DBolt
			int boltNumber = (int) tuple.getValue(1);
			int boltLoad = (int) tuple.getValue(2);
			loadList.put(boltNumber, boltLoad);

			if (loadList.size() == DBoltNumber) {
				int sum = 0;
				for (Map.Entry<Integer, Integer> entry : loadList.entrySet())
					sum += entry.getValue();

				int average = sum / DBoltNumber;
				boolean balanced = true;

				for (Map.Entry<Integer, Integer> entry : loadList.entrySet())
					if (entry.getValue() > average * (1 + parameters.getBalanceIndex())) {
						balanced = false;

						for (int i = 0; i < DBoltNumber; ++i) {
							jedis.lpush(parameters.getRedisHead() + Parameters.REDIS_DETAIL + i, "");
//							jedis.lpush(parameters.getRedisHead() + "imbalanced-" + loadReportRound, i + "-" + loadList.get(i));
						}
						break;
					}

				if (balanced) {
					for (int i = 0; i < DBoltNumber; ++i) {
//						jedis.lpush(parameters.getRedisHead() + Parameters.REDIS_D_CONTINUE + i, "");
						jedis.lpush(parameters.getRedisHead() + "balanced-" + loadReportRound, i + "-" + loadList.get(i));
					}
				}

				reportLoad(loadList);

				loadList.clear();
				loadReportRound++;
			}

		} else if (reportHead.equals(Parameters.DETAIL_REPORT + "-" + detailReportRound)) {
			// receive detail report from DBolt
			int boltNumber = (int) tuple.getValue(1);
			String detailInfo = tuple.getValue(3).toString();

			NodeWithCursor node = new NodeWithCursor(boltNumber, detailInfo);
			detailList.put(boltNumber, node);

			if (detailList.size() == DBoltNumber) {
//				for (int i = 0; i < DBoltNumber; ++i)
//					jedis.lpush(parameters.getRedisHead() + Parameters.DETAIL_REPORT
//							+ "-" + detailReportRound + "-all-received", i + "-" + detailList.get(i).getTotalLoad());

				if (parameters.getBalance()) {
//					long start = System.currentTimeMillis();
//					Map<Integer, Integer> newRouting = Balancer.reBalance(detailList, parameters.getBalanceIndex());
					BalanceInfo info = Balancer.reBalance(detailList, parameters.getBalanceIndex());

					if (info.getRoutingSize() > 0) {
						Map<Integer, Integer> newRouting = info.getRoutingTable();
						String routingInfo = "";
						for (Map.Entry<Integer, Integer> entry : newRouting.entrySet())
							routingInfo += entry.getKey() + ":" + entry.getValue() + "\t";

						int UBoltNumber = context.getComponentTasks(Parameters.U_BOLT_NAME).size();
						for (int i = 0; i < UBoltNumber; ++i)
							jedis.set(parameters.getRedisHead() + Parameters.REDIS_RT + i, routingInfo);
//						jedis.set(parameters.getRedisHead() + "routing-" + detailReportRound, routingInfo);

						jedis.lpush(parameters.getRedisHead() + "rebalanced-" + detailReportRound,
								"" + info.getTime());

						reportBalance(info);
					}
				}
				// send massage to update routing table and adjust bolts

				detailList.clear();
				detailReportRound++;
			}

		}
	}

	private void reportLoad(Map<Integer, Integer> loadList) {
		try {
			File tempDir = new File(parameters.getBaseDir() + parameters.getTopologyName());
			if (!tempDir.exists())
				tempDir.mkdirs();

			BufferedWriter writer = new BufferedWriter(new FileWriter(
					tempDir.getAbsolutePath() + "/load.txt"));
			writer.write("d-bolt " + 0 + "," + loadList.get(0));
			for (int i = 1; i < DBoltNumber; ++i)
				writer.write("\nd-bolt " + i + "," + loadList.get(i));
			writer.close();

			if (parameters.isRemoteMode()) {
				Runtime runtime = Runtime.getRuntime();
				runtime.exec("scp" + " " + parameters.getBaseDir() + parameters.getTopologyName() + "/load.txt"
						+ " " + "admin@blade56:~/apache-storm-0.10.0/public/roy/" + parameters.getTopologyName());
			}
		} catch (Exception e) {
		}
	}

	private void reportBalance(BalanceInfo info) {
		try {
			File tempDir = new File(parameters.getBaseDir() + parameters.getTopologyName());
			if (!tempDir.exists())
				tempDir.mkdirs();

			BufferedWriter writer = new BufferedWriter(new FileWriter(
					tempDir.getAbsolutePath() + "/rebalance.txt"));
			writer.write(info.getTime() + "ms" + "\n" + info.getRoutingSize());
			writer.close();


			BufferedWriter writer2 = new BufferedWriter(new FileWriter(
					tempDir.getAbsolutePath() + "/migration.txt"));

			String line = "";
			for (int i = 0; i < info.getUnrelated().size(); ++i)
				line += info.getUnrelated().get(i) + ",";
			line = line.substring(0, line.length() - 1);
			writer2.write(line);

			writer2.write("\n|\n");

			for (Map.Entry<Pair<Integer, Integer>, Pair<Integer, Integer>> entry
					: info.getMigrationPlan().entrySet())
				writer2.write(entry.getKey().getFirst() + "," + entry.getKey().getSecond()
						+ "," + (entry.getValue().getFirst() + entry.getValue().getSecond())
						+ "," + entry.getValue().getFirst() + "," + entry.getValue().getSecond() + "\n");
			writer2.close();

			if (parameters.isRemoteMode()) {
				Runtime runtime = Runtime.getRuntime();
				runtime.exec("scp" + " "
						+ parameters.getBaseDir() + parameters.getTopologyName() + "/rebalance.txt"
						+ " " + "admin@blade56:~/apache-storm-0.10.0/public/roy/" + parameters.getTopologyName());
				runtime.exec("scp" + " "
						+ parameters.getBaseDir() + parameters.getTopologyName() + "/migration.txt"
						+ " " + "admin@blade56:~/apache-storm-0.10.0/public/roy/" + parameters.getTopologyName());
			}

		} catch (Exception e) {
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

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
