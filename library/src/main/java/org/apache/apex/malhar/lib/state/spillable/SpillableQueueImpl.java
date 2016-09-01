package org.apache.apex.malhar.lib.state.spillable;

import java.util.Collection;
import java.util.Iterator;

import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableComponent;
import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableQueue;

import com.datatorrent.api.Context.OperatorContext;

public class SpillableQueueImpl<T> implements SpillableQueue<T>, SpillableComponent
{

  @Override
  public boolean add(T e)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean offer(T e)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public T remove()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T poll()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T element()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T peek()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isEmpty()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean contains(Object o)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Iterator<T> iterator()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object[] toArray()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] toArray(T[] a)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean remove(Object o)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends T> c)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void clear()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub
    
  }

}
