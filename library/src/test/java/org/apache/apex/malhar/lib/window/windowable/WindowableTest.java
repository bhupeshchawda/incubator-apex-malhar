package org.apache.apex.malhar.lib.window.windowable;

import java.util.List;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class WindowableTest
{
  public static class TestApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      WindowedFileInputOperator input = dag.addOperator("Input", new WindowedFileInputOperator());
      WatermarkGeneratorImpl wmGen = new WatermarkGeneratorImpl();
      WatermarkOption op1 = new WatermarkOption.EofWatermarkOption();
      WatermarkOption op2 = new WatermarkOption.FinalWatermarkOption();
      List<WatermarkOption> options = Lists.newArrayList();
      options.add(op1);
      options.add(op2);
      wmGen.setWatermarkOptions(options);
      input.setWatermarkGenerator(wmGen);
      input.setDirectory("/tmp/input");

      WindowedFileOutputOperator output = dag.addOperator("Output", new WindowedFileOutputOperator());
      output.setFilePath("/tmp/output");
      output.setOutputFileName("1.out");

      dag.addStream("io", input.output, output.input);
    }
  }

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new TestApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    Thread.sleep(10 * 1000);
    lc.shutdown();
  }
}
