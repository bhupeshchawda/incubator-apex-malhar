package org.apache.apex.malhar.lib.dedup;

import java.util.Date;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class WindowedDeduperTest
{
  private static String applicationPath;
  private static final String APPLICATION_PATH_PREFIX = "target/DeduperTimeBasedPOJOImplTest";
  private static final String APP_ID = "DeduperTimeBasedPOJOImplTest";
  private static final int OPERATOR_ID = 0;
  private static WindowedDeduper<TestPojo> dedup;

  @BeforeClass
  public static void setup()
  {
    dedup = new WindowedDeduper<>();
    Accumulation dedupAccum = new WindowedDeduper.DedupAccumulation<>();
    dedup.setAccumulation(dedupAccum);
    dedup.setDataStorage(new InMemoryWindowedKeyedStorage<Long, TestPojo>());
    dedup.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    // Sliding windows: 1 minute. Slide by 10 seconds
    dedup.setWindowOption(new WindowOption.SlidingTimeWindows(Duration.standardMinutes(1), Duration.standardSeconds(10)));
    dedup.setTriggerOption(TriggerOption.AtWatermark().withEarlyFiringsAtEvery(Duration.millis(1000)).accumulatingFiredPanes());

  }

  @Test
  public void testWindowedDedup()
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);
    dedup.setup(context);

    CollectorTestSink<TestPojo> uniqueSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(dedup.output, uniqueSink);

    dedup.beginWindow(0);

    long millis = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      TestPojo pojo = new TestPojo(i, new Date(millis + i));
      Tuple.TimestampedTuple<TestPojo> tuple = new TimestampedTuple<TestPojo>(millis, pojo);
//      dedup.input.process(tuple);
    }
//    TestPojo expiredPojo = new TestPojo(100, new Date(millis - 1000 * 60));
//    dedup.input.process(expiredPojo);
//    for (int i = 90; i < 200; i++) {
//      TestPojo pojo = new TestPojo(i, new Date(millis + i));
//      dedup.input.process(pojo);
//    }
//    dedup.handleIdleTime();
//    dedup.endWindow();
//    Assert.assertTrue(uniqueSink.collectedTuples.size() == 200);
//    Assert.assertTrue(duplicateSink.collectedTuples.size() == 10);
//    Assert.assertTrue(expiredSink.collectedTuples.size() == 1);

    dedup.teardown();
  }
}
