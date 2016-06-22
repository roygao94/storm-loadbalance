import balancing.BalanceInfo;
import balancing.Balancer;
import balancing.util.KGS;
import balancing.util.NodeWithCursor;
import balancing.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by roy on 1/16/16.
 */
public class NodeTest {

	@Test
	public void nodeTest() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("equal-10000.txt"));
		int[] skew = new int[10001];
		NodeWithCursor[] node = new NodeWithCursor[10];
		for (int i = 0; i < 10; ++i)
			node[i] = new NodeWithCursor();

		String line;
		for (int count = 0; (line = reader.readLine()) != null; ) {
			count++;
			int key = count;
			int g = Integer.parseInt(line);
			skew[key] = g;
			node[key % 10].add(new KGS(key, g, 1));
		}
		reader.close();

		Map<Integer, NodeWithCursor> nodeList = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			System.out.println(i + "-" + node[i].getTotalLoad());
			nodeList.put(i, node[i]);
		}

		BalanceInfo info = Balancer.reBalance(nodeList, 0.1);
		Assert.assertTrue("routing size:\t", info.getRoutingSize() != 0);

		System.out.println("time:\t" + info.getTime());
		System.out.println("cost:\t" + info.getCost());
		for (Map.Entry<Pair<Integer, Integer>, Pair<Integer, Integer>> entry : info.getMigrationPlan().entrySet())
			System.out.println(entry.getKey().getFirst() + " --> " + entry.getKey().getSecond()
					+ "\t" + entry.getValue().getFirst() + entry.getValue().getSecond());
		for (Map.Entry<Integer, Integer> entry : info.getRoutingTable().entrySet())
			System.out.println(entry.getKey() + ":" + entry.getValue());

		System.out.println("==================================================");

		for (int i = 0; i < node.length; ++i)
			node[i] = new NodeWithCursor();
		for (int key = 1; key <= 10000; ++key) {
			int nodeID;
			if (info.getRoutingTable().containsKey(key))
				nodeID = info.getRoutingTable().get(key);
			else nodeID = key % 10;

			node[nodeID].add(new KGS(key, skew[key], 1));
		}
		for (int i = 0; i < 10; ++i) {
			System.out.println(i + "-" + node[i].getTotalLoad());
			nodeList.put(i, node[i]);
		}

		info = Balancer.reBalance(nodeList, 0.1);
		Assert.assertTrue("routing size:\t", info.getRoutingSize() != 0);

		System.out.println("time:\t" + info.getTime());
		System.out.println("cost:\t" + info.getCost());
		for (Map.Entry<Pair<Integer, Integer>, Pair<Integer, Integer>> entry : info.getMigrationPlan().entrySet())
			System.out.println(entry.getKey().getFirst() + " --> " + entry.getKey().getSecond()
					+ "\t" + entry.getValue().getFirst() + entry.getValue().getSecond());
		for (Map.Entry<Integer, Integer> entry : info.getRoutingTable().entrySet())
			System.out.println(entry.getKey() + ":" + entry.getValue());
	}
}
