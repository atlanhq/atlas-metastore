package org.apache.atlas.service.redis;

import org.slf4j.Logger;

import java.util.concurrent.locks.Lock;

public interface RedisService {

  boolean acquireDistributedLock(String key) throws Exception;

  Lock acquireDistributedLockV2(String key) throws Exception;

  void releaseDistributedLock(String key);

  void releaseDistributedLockV2(Lock lock, String key);

  String getValue(String key);

  String getValue(String key, String defaultValue);

  String putValue(String key, String value);

  String putValue(String key, String value, int timeout);

  void removeValue(String key);

  Logger getLogger();

  /**
   * Check if Redis is available for operations.
   * This is used to check if Redis is connected and healthy.
   *
   * @return true if Redis is connected and healthy, false otherwise
   */
  default boolean isAvailable() {
    return true;  // Default for backward compatibility
  }

}
