package org.apache.apex.malhar.lib.inputFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
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
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

public class TextInputFormatOperator extends BaseOperator implements InputOperator,
    Partitioner<TextInputFormatOperator>, ActivationListener<Context>{
  private TextInputFormat inputFormat = new TextInputFormat();
  private transient JobContext jobContext;
  private transient Configuration conf;
  private List<InputSplit> splits;
  private transient Job job;
  private RecordReader reader;
  private transient Path p;
  private int partitionCount = 1;

  @Override
  public void setup(OperatorContext context) {
//    try {
//      job = Job.getInstance();
//      conf = job.getConfiguration();
//      conf.set("textinputformat.record.delimiter", ",");
//      FileInputFormat.addInputPath(job, new Path("/tmp/input"));
//      jobContext = new JobContextImpl(job.getConfiguration(), job.getJobID());
//      System.out.println("Paths: " + Arrays.asList(inputFormat.getInputPaths(jobContext)));
//      splits = inputFormat.getSplits(jobContext);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
  }

  @Override
  public void activate(Context context)
  {
  }

  @Override
  public void emitTuples() {
    if (reader == null && !splits.isEmpty()) {
      TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
      InputSplit split = splits.remove(0);
      reader = inputFormat.createRecordReader(split, taskAttemptContext);
      try {
        reader.initialize(split, taskAttemptContext);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    } else { // done reading all splits
      return;
    }

    try {
      if (!reader.nextKeyValue()) {
        reader = null;
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
    if (tuple != null) {
      output.emit(tuple);
    }
  }

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  @Override
  public Collection<com.datatorrent.api.Partitioner.Partition<TextInputFormatOperator>> definePartitions(
      Collection<com.datatorrent.api.Partitioner.Partition<TextInputFormatOperator>> partitions,
      com.datatorrent.api.Partitioner.PartitioningContext context)
  {
    try {
      job = Job.getInstance();
      conf = job.getConfiguration();
      conf.set("textinputformat.record.delimiter", ",");
      p = new Path("/tmp/input");
      FileInputFormat.addInputPath(job, p);
      jobContext = new JobContextImpl(job.getConfiguration(), job.getJobID());
      System.out.println("Paths: " + Arrays.asList(inputFormat.getInputPaths(jobContext)));
      splits = inputFormat.getSplits(jobContext);
    } catch(Exception e) {
      throw new RuntimeException("Define partitions " + e);
    }

    List<Partition<TextInputFormatOperator>> newPartitions =
        new ArrayList<Partition<TextInputFormatOperator>>(partitionCount);

    KryoCloneUtils<TextInputFormatOperator> cloneUtils = KryoCloneUtils.createCloneUtils(this);

    for (int i = 0; i < partitionCount; i++) {
      TextInputFormatOperator op = cloneUtils.getClone();
      newPartitions.add(new DefaultPartition<TextInputFormatOperator>(op));
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<TextInputFormatOperator>> partitions)
  {
  }

  @Override
  public void deactivate()
  {
    // TODO Auto-generated method stub
    
  }

}
