package org.apache.apex.malhar.lib.dedup;

import java.util.Date;
import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.joda.time.Duration;
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

  public static class WindowedDeduperImpl extends WindowedDeduper<TestPojo>
  {
    @Override
    protected Object getKey(TestPojo tuple)
    {
      return tuple.getKey();
    }
  }

  @BeforeClass
  public static void setup()
  {
    dedup = new WindowedDeduperImpl();
    Accumulation dedupAccum = new DedupAccumulation<TestPojo>();
    dedup.setAccumulation(dedupAccum);
    dedup.setDataStorage(new InMemoryWindowedStorage<List<TestPojo>>());
    dedup.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    // 10 second time windows
    dedup.setWindowOption(new WindowOption.TimeWindows(Duration.standardSeconds(10)));
    // Watermark at 1000 milliseconds (per second)
    dedup.setFixedWatermark(1000);
    dedup.setTriggerOption(new TriggerOption().withEarlyFiringsAtEvery(Duration.millis(1000)).accumulatingFiredPanes());
  }

  @Test
  public void testWindowedDedup() throws InterruptedException
  {
    applicationPath = APPLICATION_PATH_PREFIX;
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);
    dedup.setup(context);

    CollectorTestSink<TestPojo> uniqueSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(dedup.output, uniqueSink);

    dedup.beginWindow(0);

    long millis = 1468207800000L; // June 11, 9:00 AM, IST
    Tuple.TimestampedTuple<TestPojo> tuple;
    for (int i = 0; i < 100; i++) {
      TestPojo pojo = new TestPojo(i, new Date(millis + i*1000));
      tuple = new TimestampedTuple<TestPojo>(millis, pojo);
      System.out.println("Sending tuple: " + pojo);
      dedup.input.process(tuple);
    }
    // Duplicate:
    TestPojo pojo = new TestPojo(2, new Date(millis + 2*1000));
    tuple = new TimestampedTuple<TestPojo>(millis, pojo);
    dedup.input.process(tuple);

    dedup.endWindow();
    System.out.println(uniqueSink.collectedTuples.size());
    System.out.println(uniqueSink.collectedTuples);
//    Assert.assertTrue(uniqueSink.collectedTuples.size() == 200);

    dedup.teardown();
  }
}
