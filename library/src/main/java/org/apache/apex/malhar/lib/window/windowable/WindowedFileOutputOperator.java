package org.apache.apex.malhar.lib.window.windowable;

import java.io.IOException;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.UserDefinedControlTuple;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class WindowedFileOutputOperator extends AbstractFileOutputOperator<String>
{
  private String currentFileName;

  public final transient ControlAwareDefaultInputPort<String> input = new ControlAwareDefaultInputPort<String>()
  {
    @Override
    public boolean processControl(UserDefinedControlTuple tuple)
    {
      if (tuple instanceof FileWatermark.BeginFileWatermark) {
        currentFileName = ((FileWatermark.BeginFileWatermark)tuple).getFileName();
        System.out.println("RECEIVED BEGIN FILE " + currentFileName);
      } else if (tuple instanceof FileWatermark.EndFileWatermark) {
        System.out.println("RECEIVED END FILE " + currentFileName);
        try {
          finalizeFile(currentFileName);
        } catch (IOException e) {
          throw new RuntimeException("finalize " + e);
        }
        currentFileName = "";
      }
      return true;
    }

    @Override
    public void process(String tuple)
    {
      System.out.println("RECEIVED TUPLE " + tuple);
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

  @Override
  public void beginWindow(long windowId)
  {
    System.out.println("WINDOW STARTED " + windowId);
    super.beginWindow(windowId);
  }

  @Override
  protected String getFileName(String tuple)
  {
    return currentFileName;
  }

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return tuple.getBytes();
  }
}
