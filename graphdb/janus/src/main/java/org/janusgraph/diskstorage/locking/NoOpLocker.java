// Copyright 2026 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.locking;

import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.KeyColumn;

/**
 * No-op locker used for Atlas-specific deployments that opt out of locking.
 * This is unsafe for concurrent writers.
 */
public class NoOpLocker implements Locker {
    @Override
    public void writeLock(KeyColumn lockID, StoreTransaction tx) {
        // no-op
    }

    @Override
    public void checkLocks(StoreTransaction tx) {
        // no-op
    }

    @Override
    public void deleteLocks(StoreTransaction tx) {
        // no-op
    }
}
