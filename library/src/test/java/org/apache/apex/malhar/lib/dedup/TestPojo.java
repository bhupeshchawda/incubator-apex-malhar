package org.apache.apex.malhar.lib.dedup;

import java.util.Date;

public class TestPojo
{
  int id;
  Date time;

  public TestPojo()
  {
  }

  public TestPojo(int id, Date d)
  {
    this.id = id;
    this.time = d;
  }

  public int getId()
  {
    return id;
  }

  public void setId(int id)
  {
    this.id = id;
  }

  public Date getTime()
  {
    return time;
  }

  public void setTime(Date time)
  {
    this.time = time;
  }

  @Override
  public String toString()
  {
    return "TestPojo [id=" + id + ", time=" + time + "]";
  }

}
