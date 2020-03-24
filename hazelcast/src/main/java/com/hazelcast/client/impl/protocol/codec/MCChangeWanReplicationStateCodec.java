/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;
import com.hazelcast.client.impl.protocol.codec.custom.*;

import javax.annotation.Nullable;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/*
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
 */

/**
 * Stops, pauses or resumes WAN replication for the given WAN replication
 * and publisher on the specified member.
 */
@Generated("8b685dd3befb03c64430341f9481b4a9")
public final class MCChangeWanReplicationStateCodec {
    //hex: 0x201300
    public static final int REQUEST_MESSAGE_TYPE = 2102016;
    //hex: 0x201301
    public static final int RESPONSE_MESSAGE_TYPE = 2102017;
    private static final int REQUEST_NEW_STATE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_MEMBER_UUID_FIELD_OFFSET = REQUEST_NEW_STATE_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_MEMBER_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private MCChangeWanReplicationStateCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the WAN replication to change state of.
         */
        public java.lang.String wanReplicationName;

        /**
         * ID of the WAN publisher to change state of.
         */
        public java.lang.String wanPublisherId;

        /**
         * New state for the WAN publisher:
         * 0 - REPLICATING
         * 1 - PAUSED
         * 2 - STOPPED
         */
        public byte newState;

        /**
         * UUID of the member.
         */
        public java.util.UUID memberUuid;

        /**
         * True if the memberUuid is received from the client, false otherwise.
         * If this is false, memberUuid has the default value for its type.
        */
        public boolean isMemberUuidExists;
    }

    public static ClientMessage encodeRequest(java.lang.String wanReplicationName, java.lang.String wanPublisherId, byte newState, java.util.UUID memberUuid) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("MC.ChangeWanReplicationState");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeByte(initialFrame.content, REQUEST_NEW_STATE_FIELD_OFFSET, newState);
        encodeUUID(initialFrame.content, REQUEST_MEMBER_UUID_FIELD_OFFSET, memberUuid);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, wanReplicationName);
        StringCodec.encode(clientMessage, wanPublisherId);
        return clientMessage;
    }

    public static MCChangeWanReplicationStateCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.newState = decodeByte(initialFrame.content, REQUEST_NEW_STATE_FIELD_OFFSET);
        if (initialFrame.content.length >= REQUEST_MEMBER_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES) {
            request.memberUuid = decodeUUID(initialFrame.content, REQUEST_MEMBER_UUID_FIELD_OFFSET);
            request.isMemberUuidExists = true;
        } else {
            request.isMemberUuidExists = false;
        }
        request.wanReplicationName = StringCodec.decode(iterator);
        request.wanPublisherId = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static MCChangeWanReplicationStateCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}
