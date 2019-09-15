package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.codec.MapMessageType;
import com.hazelcast.client.impl.protocol.codec.MapSetCodec;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.internal.serialization.impl.bufferpool.ClientMessagePoolThreadLocal;
import com.hazelcast.nio.serialization.Data;

public class CustomMapSetCodec {

    public static final MapMessageType REQUEST_TYPE;
    public static final int RESPONSE_TYPE = 100;

    public CustomMapSetCodec() {
    }

    public static ClientMessage encodeRequest(ClientMessage clientMessage,
                                              String name, Data key, Data value, long threadId, long ttl) {
        int requiredDataSize = MapSetCodec.RequestParameters.calculateDataSize(name, key, value, threadId, ttl);
        clientMessage.ensureSize(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Map.set");
        clientMessage.set(name);
        clientMessage.set(key);
        clientMessage.set(value);
        clientMessage.set(threadId);
        clientMessage.set(ttl);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static MapSetCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        MapSetCodec.RequestParameters parameters = new MapSetCodec.RequestParameters();
        String name = null;
        name = clientMessage.getStringUtf8();
        parameters.name = name;
        Data key = null;
        key = clientMessage.getData();
        parameters.key = key;
        Data value = null;
        value = clientMessage.getData();
        parameters.value = value;
        long threadId = 0L;
        threadId = clientMessage.getLong();
        parameters.threadId = threadId;
        long ttl = 0L;
        ttl = clientMessage.getLong();
        parameters.ttl = ttl;
        return parameters;
    }

    public static ClientMessage encodeResponse() {
        int requiredDataSize = MapSetCodec.ResponseParameters.calculateDataSize();
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(100);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static MapSetCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        MapSetCodec.ResponseParameters parameters = new MapSetCodec.ResponseParameters();
        return parameters;
    }

    static {
        REQUEST_TYPE = MapMessageType.MAP_SET;
    }

    public static class ResponseParameters {
        public ResponseParameters() {
        }

        public static int calculateDataSize() {
            int dataSize = ClientMessage.HEADER_SIZE;
            return dataSize;
        }
    }

    public static class RequestParameters {
        public static final MapMessageType TYPE;
        public String name;
        public Data key;
        public Data value;
        public long threadId;
        public long ttl;

        public RequestParameters() {
        }

        public static int calculateDataSize(String name, Data key, Data value, long threadId, long ttl) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += ParameterUtil.calculateDataSize(name);
            dataSize += ParameterUtil.calculateDataSize(key);
            dataSize += ParameterUtil.calculateDataSize(value);
            dataSize += 8;
            dataSize += 8;
            return dataSize;
        }

        static {
            TYPE = MapSetCodec.REQUEST_TYPE;
        }
    }

}
