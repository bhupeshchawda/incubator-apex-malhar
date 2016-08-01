package org.apache.apex.malhar.lib.inputFormats;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;

public class TestTextInputFormat {

  public static class TestApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf) {
      TextInputFormatOperator input = dag.addOperator("input", new TextInputFormatOperator());
      ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
      dag.addStream("stream", input.output, console.input);
    }
  }

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new TestApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();
      Thread.sleep(1000 * 1000);
      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
