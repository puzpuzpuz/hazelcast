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

package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCPromoteLiteMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteLiteMemberCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractTargetMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.PromoteLiteMemberOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.UUID;

public class PromoteLiteMemberMessageTask extends AbstractTargetMessageTask<RequestParameters> {

    public PromoteLiteMemberMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected UUID getTargetUuid() {
        return parameters.memberUuid;
    }

    @Override
    protected Operation prepareOperation() {
        if (!parameters.isMemberUuidExists) {
            throw new IllegalArgumentException("Operation was sent from unsupported version of Management Center");
        }
        return new PromoteLiteMemberOperation();
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCPromoteLiteMemberCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCPromoteLiteMemberCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "promoteMember";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.memberUuid};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
