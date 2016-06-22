package balancing.util;

/**
 * Created by roy on 5/16/15.
 */
public class KGS implements Comparable<KGS> {

	int key;
	int g;
	int s;

	public KGS(int key, int g, int s) {
		this.key = key;
		this.g = g;
		this.s = s;
	}

	public KGS(KGS o) {
		this.key = o.key;
		this.g = o.g;
		this.s = o.s;
	}

	public int getKey() {
		return key;
	}

	public int getG() {
		return g;
	}

	public int getS() {
		return s;
	}

	public void setG(int g) {
		this.g = g;
	}

	public void setS(int s) {
		this.s = s;
	}

	public void addG(int g) {
		this.g += g;
	}

	public void addS(int s) {
		this.s += s;
	}

	public String toString() {
		return key + "\t" + g + "\t" + s;
	}

	@Override
	public int compareTo(KGS o) {
		if (((Double) ((double) this.s / (double) this.g)).compareTo(((double) o.s / (double) o.g)) == 0)
			return -((Integer) this.g).compareTo(o.g);
		else
			return (((Double) ((double) this.s / (double) this.g)).compareTo(((double) o.s / (double) o.g)));
	}
}
