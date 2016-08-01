package org.apache.apex.malhar.lib.inputFormats;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public abstract class ApexInputFormat<K, V> implements InputFormat<K, V> {

  @Override
  public abstract RecordReader<K, V> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter)
      throws IOException;

  @Override
  public abstract InputSplit[] getSplits(JobConf jobConf, int arg1) throws IOException;

}
