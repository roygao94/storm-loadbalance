package balancing;

import balancing.util.Pair;

import java.util.*;

/**
 * Created by roy on 4/21/16.
 */
public class BalanceInfo {

	private long time;
	private long cost;
	private Map<Integer, Integer> routingTable;
	private Map<Pair<Integer, Integer>, Pair<Integer, Integer>> migrationPlan;
	private List<Integer> unrelated;

	public BalanceInfo() {
	}

	public void setTime(long time) {
		this.time = time;
	}

	public void setCost(long cost) {
		this.cost = cost;
	}

	public void setRoutingTable(Map<Integer, Integer> routingTable) {
		this.routingTable = routingTable;
	}

	public void setMigrationPlan(Map<Pair<Integer, Integer>, Pair<Integer, Integer>> migrationPlan) {
		this.migrationPlan = migrationPlan;
	}

	public List<Integer> getUnrelated() {
		return unrelated;
	}

	public long getTime() {
		return time;
	}

	public int getRoutingSize() {
		return routingTable.size();
	}

	public long getCost() {
		return cost;
	}

	public Map<Integer, Integer> getRoutingTable() {
		return routingTable;
	}

	public Map<Pair<Integer, Integer>, Pair<Integer, Integer>> getMigrationPlan() {
		return migrationPlan;
	}

	public void setUnrelated(boolean[] temp) {
		unrelated = new ArrayList<>();
		for (int i = 0; i < temp.length; ++i)
			if (!temp[i])
				unrelated.add(i);
	}
}
