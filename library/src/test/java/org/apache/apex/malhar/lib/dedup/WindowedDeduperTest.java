package org.apache.apex.malhar.lib.dedup;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;

public class WindowedDeduperTest
{
  public void testWindowedDedup()
  {
    WindowedDeduper<TestPojo> dedup = new WindowedDeduper<>();
    Accumulation dedupAccum = new WindowedDeduper.DedupAccumulation<>();
    dedup.setAccumulation(dedupAccum);
    dedup.setDataStorage(new InMemoryWindowedKeyedStorage<Long, TestPojo>());
  }
}
