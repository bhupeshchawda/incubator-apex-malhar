package org.apache.apex.malhar.lib.inputFormats;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

public class TextInputFormatOperator extends BaseOperator implements InputOperator {
  private TextInputFormat inputFormat = new TextInputFormat();
  private JobContext jobContext;
  private Configuration conf;
  private List<InputSplit> splits;
  private Job job;
  private RecordReader reader;

  @Override
  public void setup(OperatorContext context) {
    try {
      job = Job.getInstance();
      conf = job.getConfiguration();
      conf.set("textinputformat.record.delimiter", ",");
      FileInputFormat.addInputPath(job, new Path("/tmp/input"));
      jobContext = new JobContextImpl(job.getConfiguration(), job.getJobID());
      System.out.println("Paths: " + Arrays.asList(inputFormat.getInputPaths(jobContext)));
      splits = inputFormat.getSplits(jobContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitTuples() {
    if (reader == null && !splits.isEmpty()) {
      TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
      reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
      try {
        reader.initialize(splits.get(0), taskAttemptContext);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      if (!reader.nextKeyValue()) {
        return;
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    String tuple;
    try {
      tuple = reader.getCurrentValue().toString();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Tuple: " + tuple);
    if (tuple != null) {
      output.emit(tuple);
    } else {
      output.emit("No Data");
    }
  }

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

}
