package tools;

import conf.Parameters;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Created by roy on 1/15/16.
 */
public class RedisCleaner {

	public static void main(String[] args) {
		redisCleanUp(new Parameters());
	}

	public static void redisCleanUp(Parameters parameters) {
		Jedis jedis = new Jedis(parameters.getHost(), Parameters.REDIS_PORT);
		String head = parameters.getRedisHead();

//		for (int i = 0; i < 10; ++i) {
//			if (jedis.exists(Parameters.REDIS_LOAD + i))
//				jedis.del(Parameters.REDIS_LOAD + i);
//			if (jedis.exists(Parameters.REDIS_DETAIL + i))
//				jedis.del(Parameters.REDIS_DETAIL + i);
//			if (jedis.exists("emit-" + Parameters.REDIS_LOAD + i))
//				jedis.del("emit-" + Parameters.REDIS_LOAD + i);
//			if (jedis.exists("balanced"))
//				jedis.del("balanced");
//			if (jedis.exists("imbalanced"))
//				jedis.del("imbalanced");
//		}

		Set<String> keys = jedis.keys("*");
		for (String key : keys)
			if (key.startsWith(head))
				jedis.del(key);

		jedis.disconnect();
	}
}
