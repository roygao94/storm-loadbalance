package balancing.util;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by roy on 5/16/15.
 */
public class historyS {

	int historySum;
	int windowSize;
	LinkedBlockingQueue<Integer> queue;

	public historyS(int windowSize) {
		historySum = 0;
		this.windowSize = windowSize;
		queue = new LinkedBlockingQueue<>();
	}

	public int getHistorySum() {
		return historySum;
	}

	public void add(int s) {
		queue.add(s);
		historySum += s;
		if (queue.size() > windowSize)
			historySum -= queue.poll();
	}
}
