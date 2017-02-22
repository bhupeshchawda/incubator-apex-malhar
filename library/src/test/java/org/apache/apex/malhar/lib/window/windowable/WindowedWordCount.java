package org.apache.apex.malhar.lib.window.windowable;

import org.joda.time.Duration;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.SumAccumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class WindowedWordCount implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    WindowedFileInputOperator input = dag.addOperator("input", new WindowedFileInputOperator());
    input.setDirectory("/tmp/input");
    input.setEmitBatchSize(1);
    StringToKeyValPair convert1 = dag.addOperator("convert1", new StringToKeyValPair());
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator =
      dag.addOperator("count", new KeyedWindowedOperatorImpl());
    Accumulation<Long, MutableLong, Long> sum = new SumAccumulation();

    windowedOperator.setAccumulation(sum);
    windowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<String, MutableLong>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<String, Long>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(2)));
    windowedOperator.setTriggerOption(TriggerOption.AtWatermark());

    KeyValPairToString convert2 = dag.addOperator("convert2", new KeyValPairToString());
    WindowedFileOutputOperator output = dag.addOperator("output", new WindowedFileOutputOperator());
    output.setFilePath("/tmp/output");

    dag.addStream("inputToConvert1", input.output, convert1.input);
    dag.addStream("convert1ToCount", convert1.output, windowedOperator.input);
    dag.addStream("countToConvert2", windowedOperator.output, convert2.input);
    dag.addStream("convert2ToOutput", convert2.output, output.input);
  }

  @Test
  public void testWordCount()
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    try {
      lma.prepareDAG(new WindowedWordCount(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(true);
      lc.run(1000*1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
