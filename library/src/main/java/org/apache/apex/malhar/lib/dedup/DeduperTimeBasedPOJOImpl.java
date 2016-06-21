package org.apache.apex.malhar.lib.dedup;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.netlet.util.Slice;

public class DeduperTimeBasedPOJOImpl extends AbstractDeduper<Object> implements ActivationListener<Context>
{

  @NotNull
  private String keyExpression;

  @NotNull
  private String timeExpression;

  @NotNull
  private boolean useSystemTime = true;

  private transient Class<?> pojoClass;

  private transient Getter<Object, Long> timeGetter;

  private transient Getter<Object, Object> keyGetter;

  @InputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(PortContext context)
    {
      pojoClass = context.getAttributes().get(PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }
  };

  @Override
  protected long getTime(Object tuple)
  {
    return timeGetter.get(tuple);
  }

  @Override
  protected Slice getKey(Object tuple)
  {
    Object key = keyGetter.get(tuple);
    return new Slice(key.toString().getBytes());
  }

  @Override
  public void activate(Context context)
  {
    timeGetter = PojoUtils.createGetter(pojoClass, timeExpression, Long.class);
    keyGetter = PojoUtils.createGetter(pojoClass, keyExpression, Object.class);
  }

  @Override
  public void deactivate()
  {
  }

  public String getKeyExpression()
  {
    return keyExpression;
  }

  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  public String getTimeExpression()
  {
    return timeExpression;
  }

  public void setTimeExpression(String timeExpression)
  {
    this.timeExpression = timeExpression;
  }

  public boolean isUseSystemTime()
  {
    return useSystemTime;
  }

  public void setUseSystemTime(boolean useSystemTime)
  {
    this.useSystemTime = useSystemTime;
  }

}
