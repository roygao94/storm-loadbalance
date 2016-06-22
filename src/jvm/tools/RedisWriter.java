package tools;

import conf.Parameters;
import org.apache.commons.math3.distribution.ZipfDistribution;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Roy Gao on 1/13/2016.
 */
public class RedisWriter {

	private String host;
	private int port = Parameters.REDIS_PORT;

	public static void main(String[] args) throws IOException {
		writeToRedis(Parameters.REMOTE_HOST, Parameters.REDIS_PORT);
		writeSkewsToRedis(Parameters.REMOTE_HOST, Parameters.REDIS_PORT);
//		writer.writeToRedis();
	}

	public RedisWriter(Parameters parameters) {
		this.host = parameters.getHost();
	}

	public static void writeToRedis(String host, int port) throws IOException {
		String line;
		int val;
		Jedis jedis = new Jedis(host, port);

		if (!jedis.exists(Parameters.REDIS_KGS)) {
			BufferedReader reader = new BufferedReader(new FileReader("equal-10000.txt"));
			List<Integer> gList = new ArrayList<>();

			while ((line = reader.readLine()) != null) {
				val = Integer.parseInt(line);
				gList.add(val);
			}
			reader.close();

			for (int i = gList.size() - 1; i >= 0; --i)
				jedis.lpush(Parameters.REDIS_KGS, i + 1 + "," + gList.get(i));
		}
		jedis.disconnect();
	}

	public static void writeSkewsToRedis(String host, int port) throws IOException {
		Jedis jedis = new Jedis(host, port);

		for (Double s : Parameters.skew)
			if (!jedis.exists(Parameters.REDIS_SKEW + "-" + s)) {
				ZipfDistribution dist = new ZipfDistribution(Parameters.KEY_NUMBER, s);
				double minProbability = dist.probability(Parameters.KEY_NUMBER);

				List<Integer> gList = new ArrayList<>();
				for (int i = 1; i <= Parameters.KEY_NUMBER; ++i)
					gList.add((int) (dist.probability(i) / minProbability));

				for (int i = gList.size() - 1; i >= 0; --i)
					jedis.lpush(Parameters.REDIS_SKEW + "-" + s, i + 1 + "," + gList.get(i));
			}

		jedis.disconnect();
	}
}
