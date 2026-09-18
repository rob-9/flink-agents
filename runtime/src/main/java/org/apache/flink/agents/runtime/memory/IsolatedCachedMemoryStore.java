/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.memory;

import java.util.HashMap;
import java.util.Map;

/**
 * Memory owned by one internal sub-agent invocation and shared by its actions. Child writes never
 * enter the parent's keyed memory. Completed action-state records retain the memory updates and
 * replay them into this store on recovery; the store itself lives only for the invocation.
 */
public class IsolatedCachedMemoryStore extends CachedMemoryStore {
    private final Map<String, MemoryObjectImpl.MemoryItem> values = new HashMap<>();

    public IsolatedCachedMemoryStore() {
        super(null);
    }

    @Override
    public MemoryObjectImpl.MemoryItem get(String key) {
        return values.get(key);
    }

    @Override
    public void put(String key, MemoryObjectImpl.MemoryItem value) {
        values.put(key, value);
    }

    @Override
    public boolean contains(String key) {
        return values.containsKey(key);
    }

    @Override
    public void persistCache() {
        // Retain writes for subsequent child actions. Durability is provided by their ActionState.
    }

    @Override
    public void clear() {
        values.clear();
    }
}
