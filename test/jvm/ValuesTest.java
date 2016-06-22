import org.apache.storm.tuple.Values;
import org.junit.Test;

/**
 * Created by roy on 1/15/16.
 */
public class ValuesTest {

	@Test
	public void valueTest() {
		Values val = new Values("666", 3, 31, false);

		String a = val.get(0).toString();
		int b = (int) val.get(1);
		int c = (int) val.get(2);
		boolean d = (boolean) val.get(3);

		System.out.println(a + "\n" + b + "\n" + c + "\n" + d);
	}
}
