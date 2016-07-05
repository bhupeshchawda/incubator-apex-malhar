package org.apache.apex.malhar.lib.dedup;

import java.io.IOException;
import java.util.Date;

import javax.validation.ConstraintViolationException;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.managed.TimeBucketAssigner;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.fileaccess.TFileImpl;

public class DeduperApplicationTest
{

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new DeduperApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();
      Thread.sleep(10 * 1000); // 10 seconds
      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class DeduperApp implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomDedupDataGenerator random = dag.addOperator("Input", RandomDedupDataGenerator.class);
      DeduperTimeBasedPOJOImpl dedup = dag.addOperator("Dedup", DeduperTimeBasedPOJOImpl.class);
      dedup.setKeyExpression("{$.key}");
      dedup.setTimeExpression("{$.date.getTime()}");
      TimeBucketAssigner tba = new TimeBucketAssigner();
      tba.setBucketSpan(Duration.standardSeconds(10));
      tba.setExpireBefore(Duration.standardSeconds(60));
      tba.setReferenceInstant(new Instant(0));
      dedup.managedState.setTimeBucketAssigner(tba);
      FileAccessFSImpl fAccessImpl = new TFileImpl.DTFileImpl();
      fAccessImpl.setBasePath(dag.getAttributes().get(DAG.APPLICATION_PATH) + "/bucket_data");
      dedup.managedState.setFileAccess(fAccessImpl);
      dag.setInputPortAttribute(dedup.input, Context.PortContext.TUPLE_CLASS, TestPojo.class);

      Verifier unique = dag.addOperator("Unique", Verifier.class);
      Verifier duplicate = dag.addOperator("Duplicate", Verifier.class);
      Verifier expired = dag.addOperator("Expired", Verifier.class);

      dag.addStream("Input to Dedup", random.output, dedup.input);
      dag.addStream("Dedup to Unique", dedup.output, unique.input);
      dag.addStream("Dedup to Duplicate", dedup.duplicates, duplicate.input);
      dag.addStream("Dedup to Expired", dedup.expired, expired.input);
    }
  }

  public static class RandomDedupDataGenerator extends BaseOperator implements InputOperator
  {
    private final long count = 10000;
    private long windowCount = 0;
    private long sequenceId = 0;

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId)
    {
      windowCount = 0;
    }

    @Override
    public void emitTuples()
    {
      if (windowCount < count) {
        TestPojo pojo = new TestPojo(sequenceId, new Date(), sequenceId);
        output.emit(pojo);
      }
    }
  }

  public static class Verifier extends BaseOperator
  {
    long prevSequence = 0;
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        TestPojo pojo = (TestPojo)tuple;
        if (pojo.getSequence() < prevSequence) {
          throw new RuntimeException("Wrong sequence");
        }
        prevSequence = pojo.sequence;
      }
    };
  }
}
