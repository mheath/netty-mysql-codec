/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.mheath.netty.codec.mysql.integration.client;

import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.HandshakeResponse;
import com.github.mheath.netty.codec.mysql.Constants;
import com.github.mheath.netty.codec.mysql.MysqlNativePasswordUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HandshakeTest extends AbstractIntegrationTest {

    @Test
    public void handshake() {
        try (Connection connection = client.connect()) {
            final Handshake handshake = (Handshake) connection.pollServer();
            assertThat(handshake).isNotNull();

            final HandshakeResponse response = HandshakeResponse
                    .create()
                    .addCapabilities(CLIENT_CAPABILITIES)
                    .username(user)
                    .addAuthData(MysqlNativePasswordUtil.hashPassword(password, handshake.getAuthPluginData()))
                    .database(database)
                    .authPluginName(Constants.MYSQL_NATIVE_PASSWORD)
                    .build();
            connection.write(response);
            connection.assertThatNextPacket().isOkResponse().hasAffectedRows(0);
        }
    }

}
