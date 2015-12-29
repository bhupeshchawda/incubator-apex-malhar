package com.datatorrent.contrib.hbase;

import java.util.List;

import com.datatorrent.lib.util.FieldValueGenerator;
import com.datatorrent.lib.util.PojoUtils;

/**
 * FieldValueGenerator for HBase
 */
public class HBaseFieldValueGenerator extends FieldValueGenerator<HBaseFieldInfo>
{

  public static HBaseFieldValueGenerator getHBaseFieldValueGenerator(final Class<?> clazz, List<HBaseFieldInfo>
      fieldInfos)
  {
    return new HBaseFieldValueGenerator(clazz, fieldInfos);
  }

  protected HBaseFieldValueGenerator(final Class<?> clazz, List<HBaseFieldInfo> fieldInfos)
  {
    for (HBaseFieldInfo fieldInfo : fieldInfos) {
      fieldInfoMap.put(fieldInfo.getFamilyName() + ":" + fieldInfo.getColumnName(), fieldInfo);

      PojoUtils.Getter<Object, Object> getter =
          PojoUtils.createGetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
      fieldGetterMap.put(fieldInfo, getter);
    }

    for (HBaseFieldInfo fieldInfo : fieldInfos) {
      PojoUtils.Setter<Object, Object> setter =
          PojoUtils.createSetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
      fieldSetterMap.put(fieldInfo, setter);
    }
  }

  public void setColumnValue(Object instance, String columnName, String columnFamily, Object value,
      ValueConverter<HBaseFieldInfo> valueConverter)
  {
    HBaseFieldInfo fieldInfo = fieldInfoMap.get(columnFamily + ":" + columnName);
    PojoUtils.Setter<Object, Object> setter = fieldSetterMap.get(fieldInfo);
    setter.set(instance, valueConverter == null ? value : valueConverter.convertValue(fieldInfo, value));
  }

}
