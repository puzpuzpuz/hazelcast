/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.function.Supplier;

import java.lang.ref.WeakReference;
import java.util.Map;

import static com.hazelcast.util.ConcurrentReferenceHashMap.ReferenceType.STRONG;
import static com.hazelcast.util.ConcurrentReferenceHashMap.ReferenceType.WEAK;

public final class ClientMessagePoolThreadLocal {

    private final ThreadLocal<WeakReference<ClientMessagePool>> threadLocal =
            new ThreadLocal<WeakReference<ClientMessagePool>>();
    private final Map<Thread, ClientMessagePool> strongReferences =
            new ConcurrentReferenceHashMap<Thread, ClientMessagePool>(WEAK, STRONG);

    public ClientMessagePoolThreadLocal() {
    }

    public ClientMessagePool get() {
        WeakReference<ClientMessagePool> ref = threadLocal.get();
        if (ref == null) {
            ClientMessagePool pool = new ClientMessagePool();
            ref = new WeakReference<ClientMessagePool>(pool);
            strongReferences.put(Thread.currentThread(), pool);
            threadLocal.set(ref);
            return pool;
        } else {
            ClientMessagePool pool = ref.get();
            if (pool == null) {
                throw new IllegalStateException("Could not find client message pool");
            }
            return pool;
        }
    }

    public void clear() {
        strongReferences.clear();
    }
}
