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

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * Default {BufferPool} implementation.
 *
 * This class is designed to that a subclass can be made. This is done for the Enterprise version.
 */
public class ClientMessagePool {

    static final int MAX_POOLED_ITEMS = 3;
    static final int DEFAULT_SIZE = 4096;

    final Queue<ClientMessage> outputQueue = new ArrayDeque<ClientMessage>(MAX_POOLED_ITEMS);

    public ClientMessagePool() {
    }

    public ClientMessage takeClientMessage() {
        ClientMessage msg = outputQueue.poll();
        if (msg == null) {
            msg = ClientMessage.createForEncode(DEFAULT_SIZE);
        }
        return msg;
    }

    public void returnClientMessage(ClientMessage msg) {
        if (msg == null) {
            return;
        }
        msg.clear();
        offerOrClose(outputQueue, msg);
    }

    private static <C extends Closeable> void offerOrClose(Queue<C> queue, C item) {
        if (queue.size() == MAX_POOLED_ITEMS) {
            closeResource(item);
            return;
        }
        queue.offer(item);
    }
}
