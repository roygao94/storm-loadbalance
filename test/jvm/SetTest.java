import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * Created by roy on 1/17/16.
 */
public class SetTest {

	@Test
	public void setTest() {
		long start = System.currentTimeMillis();
		int num;
		Set<Integer> set;
		Queue<Integer> queue = new LinkedList<>();

		for (int i = 0; i < 100; ++i) {
			set = new HashSet<>();

			queue = new LinkedList<>();
			while (set.size() < 10000) {
				num = (int) (Math.random() * 10000);
				set.add(num);
				queue.offer(num);
			}
		}

		while (!queue.isEmpty())
			System.out.println(queue.poll());

		System.out.println(System.currentTimeMillis() - start);
	}
}
