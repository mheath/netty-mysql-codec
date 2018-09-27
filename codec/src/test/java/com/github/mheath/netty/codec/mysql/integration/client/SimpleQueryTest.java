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

import com.github.mheath.netty.codec.mysql.ColumnType;
import com.github.mheath.netty.codec.mysql.MysqlPacketAssert;
import org.junit.jupiter.api.Test;


/**
 *
 */
public class SimpleQueryTest extends AbstractIntegrationTest {

    @Test
    public void simpleLongLongQuery() {
        assertSimpleQuery("SELECT 1 as test", "test", ColumnType.MYSQL_TYPE_LONGLONG).hasValues().first().isEqualTo("1");
    }

    @Test
    public void simpleStringQuery() {
        assertSimpleQuery("SELECT 'Hello' as hello", "hello", ColumnType.MYSQL_TYPE_VAR_STRING).hasValues().first().isEqualTo("Hello");
    }

    @Test
    public void simpleTimestampQuery() {
        assertSimpleQuery("SELECT now() as now", "now", ColumnType.MYSQL_TYPE_DATETIME).hasValues().first().isNotNull();
    }

    private MysqlPacketAssert.ResultsetRowAssert assertSimpleQuery(String query, String alias, ColumnType type) {
        try (Connection connection = client.connect()) {
            connection.authenticate();
            connection.query(query);

            connection.assertThatNextPacket()
                      .isFieldCount()
                      .hasFieldCount(1);

            connection.assertThatNextPacket()
                      .isColumnDefinition()
                      .hasColumnType(type)
                      .hasName(alias);

            connection.assertThatNextPacket()
                      .isEofResponse();

            final MysqlPacketAssert.ResultsetRowAssert resultsetRow = connection.assertThatNextPacket().isResultsetRow();

            connection.assertThatNextPacket()
                      .isEofResponse();

            return resultsetRow;
        }
    }
}
