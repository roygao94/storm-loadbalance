package balancing;

import balancing.util.KGS;
import balancing.util.NodeWithCursor;
import balancing.util.Pair;
import balancing.util.historyS;
import conf.Parameters;

import java.util.Map;

/**
 * Created by Roy Gao on 5/29/2016.
 */
public class RoutingControlBalancer extends Balancer {

	public static BalanceInfo reBalance(Map<Integer, NodeWithCursor> nodeList, double balanceIndex) {
		long start = System.currentTimeMillis();

		if (history == null) {
			history = new historyS[Parameters.KEY_NUMBER + 1];
			for (int i = 0; i < Parameters.KEY_NUMBER + 1; ++i)
				history[i] = new historyS(Parameters.WINDOW_SIZE);
		}

		N = nodeList.size();
		node = new NodeWithCursor[N];
		for (int i = 0; i < N; ++i)
			node[i] = nodeList.get(i);

		computeBound(balanceIndex);
		backup();

		// migrate and ensure routing size
		int size = getRoutingSize();
		migrateBack();

		migrate();
		int currentSize = getRoutingSize();
		while (currentSize > 150) {
			recoverNodes();
			if (size <= currentSize - 150) {
				migrateBackN(size);
				size = 0;
				migrate();
				currentSize = getRoutingSize();
				break;
			}
			migrateBackN(currentSize - 150);
			size -= (currentSize - 150);
			migrate();
			currentSize = getRoutingSize();
		}

		updateRouting();

		long timeElapsed = System.currentTimeMillis() - start;

		BalanceInfo info = new BalanceInfo();
		info.setTime(timeElapsed);
		info.setCost(getCostAndMigrationPlan());
		info.setRoutingTable(routing);
		info.setMigrationPlan(migrationPlan);

		boolean[] temp = new boolean[N];
		for (int i = 0; i < N; ++i)
			temp[i] = false;
		for (Pair<Integer, Integer> pair : migrationPlan.keySet())
			temp[pair.getFirst()] = temp[pair.getSecond()] = true;

		info.setUnrelated(temp);

		return info;
	}

	private static int getRoutingSize() {
		int size = 0;
		for (int i = 0; i < N; ++i)
			for (KGS kgs : node[i].values())
				if (kgs.getKey() % N != i)
					size++;

		return size;
	}

	private static void recoverNodes() {
	}

	private static void migrateBackN(int size) {
	}

}
