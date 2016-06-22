package balancing.util;

/**
 * Created by roy on 5/17/15.
 */
public class MigrationKGS {
	int nodeID;
	KGS info;

	public MigrationKGS(int nodeID, KGS info) {
		this.nodeID = nodeID;
		this.info = new KGS(info);
	}

	public int getNodeID() {
		return nodeID;
	}

	public KGS getInfo() {
		return info;
	}
}
