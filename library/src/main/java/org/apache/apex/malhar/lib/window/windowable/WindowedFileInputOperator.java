package org.apache.apex.malhar.lib.window.windowable;

import java.io.IOException;
import java.io.InputStream;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;

public class WindowedFileInputOperator extends LineByLineFileInputOperator
{
  private long fileIndex = 0;
  private boolean run = true;
  private boolean scanned = false;
  private boolean shutdown = false;
  private String currentFileName;
  private int operatorId;

  public final transient ControlAwareDefaultOutputPort<String> output = new ControlAwareDefaultOutputPort<>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    operatorId = context.getId();
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    run = true;
    if (shutdown) {
      BaseOperator.shutdown();
    }
    super.beginWindow(windowId);
  }

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    currentFileName = path.getName();
    output.emitControl(new FileWatermark.BeginFileWatermark(operatorId, ++fileIndex, currentFileName));
    return super.openFile(path);
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    output.emitControl(new FileWatermark.EndFileWatermark(operatorId, ++fileIndex, currentFileName));
    run = false;
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }

  @Override
  public void emitTuples()
  {
    if (run) {
      super.emitTuples();
    }
  }

  @Override
  protected void scanDirectory()
  {
    if (!scanned) {
      super.scanDirectory();
      scanned = true;
    } else if (!shutdown){
      shutdown = true;
    }
  }
}
