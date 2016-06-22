package conf;

import java.io.Serializable;

/**
 * Created by roy on 1/12/16.
 */
public class Parameters implements Serializable {

	enum Mode {LOCAL, REMOTE}

	public static final String REMOTE_HOST = "blade56";
	public static final String LOCAL_HOST = "localhost";
	public static final String REDIS_VM = "192.168.56.143";
	public static final int REDIS_PORT = 6379;

	public static final String LOCAL_BASE_DIR = "/home/roy/Tools/apache-storm-0.10.0/public/roy/";
	public static final String REMOTE_BASE_DIR = "/home/admin/apache-storm-0.10.0/public/roy/";

	public static final String REDIS_RT = "rt";
	public static final String REDIS_KGS = "kgs";
	public static final String REDIS_SKEW = "skew";

	public static final double[] skew = new double[]{0.75, 0.8, 0.85, 0.9, 0.95, 1.0};

	public static final String REDIS_LOAD = "load";
	public static final String LOAD_REPORT = "load-report";
	public static final String REDIS_DETAIL = "detail";
	public static final String DETAIL_REPORT = "detail-report";
	public static final String REDIS_D_CONTINUE = "d-continue";
	public static final String REDIS_U_WAIT = "u-wait";

	public static final String DEFAULT_TOPOLOGY_NAME = "load-balance-driver";
	public static final String SPOUT_NAME = "spout";
	public static final String U_BOLT_NAME = "u-bolt";
	public static final String D_BOLT_NAME = "d-bolt";
	public static final String CONTROLLER_NAME = "controller";

	public static long REPORT_TIME = 10000;

	public static final long LOCAL_TIME = 5000;
	public static final int KEY_NUMBER = 10000;

	public static int DBOLT_NUMBER = 10;
	public static int WINDOW_SIZE = 5;

	public static int POW_EXP = 3;
	public static double DEFAULT_BALANCED_INDEX = 0.1;
	public static boolean ENSURE_LOW = true;


	private Mode mode;
	private String topologyName;
	private boolean balance;
	private String host;
	private String redisHead;
	private double balanceIndex;
	private String baseDir;


	public Parameters() {
		mode = Mode.LOCAL;
		topologyName = DEFAULT_TOPOLOGY_NAME;
		balance = false;
		host = LOCAL_HOST;
		redisHead = "TOPO-";
		balanceIndex = DEFAULT_BALANCED_INDEX;
		baseDir = LOCAL_BASE_DIR;
	}

	public Parameters(Parameters parameters) {
		mode = parameters.mode;
		topologyName = parameters.topologyName;
		balance = parameters.balance;
		host = parameters.host;
		redisHead = parameters.redisHead;
		balanceIndex = parameters.balanceIndex;
		baseDir = parameters.baseDir;
	}

	public void setLocalMode() {
		mode = Mode.LOCAL;
		setHost(LOCAL_HOST);
		setBaseDir(LOCAL_BASE_DIR);
	}

	public void setRemoteMode() {
		mode = Mode.REMOTE;
		setHost(REMOTE_HOST);
		setBaseDir(REMOTE_BASE_DIR);
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setTopologyName(String name) {
		topologyName = name;
	}

	public void setBalance(boolean balance) {
		this.balance = balance;
	}

	public void appendRedisHead(String add) {
		redisHead += add;
	}

	public void setBalanceIndex(double balanceIndex) {
		this.balanceIndex = balanceIndex;
	}

	public void setBaseDir(String baseDir) {
		this.baseDir = baseDir;
	}

	public boolean isLocalMode() {
		return mode == Mode.LOCAL;
	}

	public boolean isRemoteMode() {
		return mode == Mode.REMOTE;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public boolean getBalance() {
		return balance;
	}

	public String getHost() {
		return host;
	}

	public String getRedisHead() {
		return redisHead;
	}

	public double getBalanceIndex() {
		return balanceIndex;
	}

	public String getBaseDir() {
		return baseDir;
	}
}
