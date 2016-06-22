package balancing.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roy Gao on 7/14/2015.
 */
public class NodeWithCursor {

	int ID;
	int totalLoad;
	int cursor;
	public Map<Integer, KGS> infoList;

	public NodeWithCursor() {
		infoList = new HashMap<>();
	}

	public NodeWithCursor(NodeWithCursor node) {
		ID = node.ID;
		totalLoad = node.totalLoad;
		cursor = node.cursor;
		infoList = new HashMap<>(node.infoList);
	}

	public NodeWithCursor(int ID, String detailInfo) {
		this.ID = ID;
		infoList = new HashMap<>();
		String[] kgsList = detailInfo.split("\t");
		for (String kgs : kgsList) {
			String[] split = kgs.split(",");
			add(new KGS(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2])));
		}
	}

	public void setID(int ID) {
		this.ID = ID;
	}

	public void setCursor(int cursor) {
		this.cursor = cursor;
	}

	public int getTotalLoad() {
		return totalLoad;
	}

	public int getCursor() {
		return cursor;
	}

	public void add(KGS kgs) {
		if (infoList.isEmpty())
			setCursor(kgs.g * 2);

		infoList.put(kgs.key, kgs);
		totalLoad += kgs.g;
	}

	public void remove(KGS kgs) {
		if (infoList.containsKey(kgs.key)) {
			infoList.remove(kgs.key);
			totalLoad -= kgs.g;
		}
	}

	public void remove(int key) {
		if (infoList.containsKey(key)) {
			totalLoad -= infoList.get(key).g;
			infoList.remove(key);
		}
	}

	public Collection<KGS> values() {
		return infoList.values();
	}

	public boolean containsKey(KGS kgs) {
		return infoList.containsKey(kgs.key);
	}

	public boolean containsKey(int key) {
		return infoList.containsKey(key);
	}
}
