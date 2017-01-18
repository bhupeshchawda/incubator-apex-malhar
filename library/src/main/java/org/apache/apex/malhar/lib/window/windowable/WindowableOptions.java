package org.apache.apex.malhar.lib.window.windowable;

import org.apache.apex.malhar.lib.window.WindowOption;

public interface WindowableOptions extends WindowOption
{
  class NumRecords implements WindowOption
  {
    long num;

    private NumRecords()
    {
      // for kryo
    }

    public NumRecords(long num)
    {
      this.num = num;
    }

    public long getNum()
    {
      return num;
    }
  }

}
