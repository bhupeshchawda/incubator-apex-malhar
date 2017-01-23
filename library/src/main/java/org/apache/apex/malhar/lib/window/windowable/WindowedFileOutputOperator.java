package org.apache.apex.malhar.lib.window.windowable;

import java.io.IOException;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.UserDefinedControlTuple;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;

import com.datatorrent.api.StreamCodec;

public class WindowedFileOutputOperator extends GenericFileOutputOperator.StringFileOutputOperator
{
  public final transient ControlAwareDefaultInputPort<String> input = new ControlAwareDefaultInputPort<String>()
  {
    @Override
    public boolean processControl(UserDefinedControlTuple payload)
    {
      processControlTuple(payload);
      return false;
    }

    @Override
    public void process(String tuple)
    {
      processTuple(tuple);
    }

    @Override
    public StreamCodec<String> getStreamCodec()
    {
      if (WindowedFileOutputOperator.this.streamCodec == null) {
        return super.getStreamCodec();
      } else {
        return streamCodec;
      }
    }
  };

  public void processControlTuple(UserDefinedControlTuple tuple)
  {
    if (tuple instanceof WatermarkControlTuple.EofWatermark) {
      try {
        finalizeFile(((WatermarkControlTuple.EofWatermark)tuple).getFileName());
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected void processTuple(String tuple)
  {
    super.processTuple(tuple);
  }
}
