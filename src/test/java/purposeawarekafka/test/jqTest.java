package purposeawarekafka.test;

import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.org.apache.commons.io.input.CharSequenceReader;
import purposeawarekafka.pbac.datasubject.jq;

import static org.junit.Assert.*;

public class jqTest {
	private jq cut;

	@Before
	public void setUp(){
		this.cut = new jq();
	}
	@Test
	public void test() throws Exception{
		assertTrue(cut.evaluateToBool(".a == 5", new CharSequenceReader("{\"a\":5}")));
		assertFalse(cut.evaluateToBool(".a > 5", new CharSequenceReader("{\"a\":5}")));
	}
}