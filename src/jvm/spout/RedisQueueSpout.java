package spout;

import conf.Parameters;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class RedisQueueSpout extends BaseRichSpout {

	public static final String OUTPUT_FIELD = "text";
	protected SpoutOutputCollector _collector;

	int UBoltNumber;
	int DBoltNumber;

	private Parameters parameters;
//	private String host;
//	private int port = Parameters.REDIS_PORT;
	private String skewName;
	private long len = 0;
	private static int index;
	private static int count;
	private static int loop;
	private static List<String> mySkew;
	//	private long lastWrite = 0;
	private transient Jedis jedis = null;

	private Queue<Integer> randomQueue;

	public RedisQueueSpout(Parameters parameters) {
		this.parameters = new Parameters(parameters);
		randomQueue = new LinkedList<>();
//		lastWrite = 0;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(OUTPUT_FIELD));
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		UBoltNumber = context.getComponentTasks(Parameters.U_BOLT_NAME).size();
		DBoltNumber = context.getComponentTasks(Parameters.D_BOLT_NAME).size();
		index = 0;
		count = 0;
		loop = 0;
	}

	@Override
	public void nextTuple() {
		Jedis jedis = getConnectedJedis();
		if (jedis == null)
			return;

		if (count == 0 && index == 0) {
			len = jedis.llen(Parameters.REDIS_SKEW + "-" + Parameters.skew[loop]);
			reportSkew();
		}
		Object text = getTextByOrder();
//		Object text = getRandomText();

		if (text != null)
			emitData(text);
	}

	private Object getTextByOrder() {
		Object text = mySkew.get(index);
		if (++index >= len) {
			index = 0;
			count++;
			if (count % 3 == 0)
				for (int i = 0; i < DBoltNumber; ++i)
					jedis.lpush(parameters.getRedisHead() + Parameters.REDIS_LOAD + i, "");
			if (count >= 8) {
//			for (int i = 0; i < UBoltNumber; ++i)
//				jedis.lpush(parameters.getRedisHead() + Parameters.REDIS_U_WAIT + i, "");
				count = 0;
				if (++loop >= Parameters.skew.length)
					loop = 0;
			}
		}

		return text;
	}

	private void reportSkew() {
		try {
			mySkew = jedis.lrange(Parameters.REDIS_SKEW + "-" + Parameters.skew[loop], 0, len);

			File tempDir = new File(parameters.getBaseDir() + parameters.getTopologyName());
			if (!tempDir.exists())
				tempDir.mkdirs();

			BufferedWriter writer = new BufferedWriter(new FileWriter(tempDir.getAbsolutePath() + "/keys.txt"));
			writer.write(Parameters.skew[loop] + "\n");
			int first = Integer.parseInt(mySkew.get(0).split(",")[1]);

			int prev = 0, last = first;
			writer.write(mySkew.get(0));
			for (int i = 1; i < first; ) {
				while (last - Integer.parseInt(mySkew.get(i).split(",")[1]) < 100 && i - prev < 10)
					i++;
				writer.write("\n" + mySkew.get(i));
				last = Integer.parseInt(mySkew.get(i).split(",")[1]);
				prev = i;
			}

			writer.close();

			if (parameters.isRemoteMode()) {
				Runtime runtime = Runtime.getRuntime();
				runtime.exec("scp" + " "
						+ parameters.getBaseDir() + parameters.getTopologyName() + "/keys.txt"
						+ " " + "admin@blade56:~/apache-storm-0.10.0/public/roy/" + parameters.getTopologyName());
			}

		} catch (Exception e) {
		}
	}

	private Object getRandomText() {
		Object text = null;

		if (randomQueue.isEmpty())
			generateRandomNumbers();
		text = jedis.lindex(Parameters.REDIS_KGS, randomQueue.poll());

		return text;
	}

	private void generateRandomNumbers() {
		Set<Integer> set = new HashSet<>();
		int num;

		while (set.size() < Parameters.KEY_NUMBER) {
			num = (int) (Math.random() * Parameters.KEY_NUMBER);
			set.add((int) (Math.random() * Parameters.KEY_NUMBER));
			randomQueue.offer(num);
		}
	}

	private Jedis getConnectedJedis() {
		if (jedis != null)
			return jedis;

		try {
			jedis = new Jedis(parameters.getHost(), Parameters.REDIS_PORT);
//			len = jedis.llen(Parameters.REDIS_KGS);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jedis;
	}

	public void emitData(Object data) {
		_collector.emit(Arrays.asList(data), data);
	}

	private void disconnect() {
		try {
			jedis.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		jedis = null;
	}
}
