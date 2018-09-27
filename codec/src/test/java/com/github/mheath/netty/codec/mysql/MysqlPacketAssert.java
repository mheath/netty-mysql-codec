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

package com.github.mheath.netty.codec.mysql;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;

import java.util.List;

/**
 *
 */
public class MysqlPacketAssert extends AbstractAssert<MysqlPacketAssert, MysqlPacket> {

    public MysqlPacketAssert(MysqlPacket actual) {
        super(actual, MysqlPacketAssert.class);
    }

    public static MysqlPacketAssert assertThat(MysqlPacket actual) {
        return new MysqlPacketAssert(actual);
    }

    public ColumnDefinitionAssert isColumnDefinition() {
        isNotNull();
        isInstanceOf(ColumnDefinition.class);
        return new ColumnDefinitionAssert((ColumnDefinition) actual);
    }

    public static class ColumnDefinitionAssert extends AbstractAssert<ColumnDefinitionAssert, ColumnDefinition> {
        public ColumnDefinitionAssert(ColumnDefinition actual) {
            super(actual, ColumnDefinitionAssert.class);
        }

        public ColumnDefinitionAssert hasColumnType(ColumnType type) {
            isNotNull();
            Assertions.assertThat(actual.getType()).isEqualTo(type);
            return this;
        }

        public ColumnDefinitionAssert hasName(String name) {
            isNotNull();
            Assertions.assertThat(actual.getName()).isEqualTo(name);
            return this;
        }
    }

    public EofResponseAssert isEofResponse() {
        isNotNull();
        isInstanceOf(EofResponse.class);
        return new EofResponseAssert((EofResponse)actual);
    }

    public static class EofResponseAssert extends AbstractAssert<EofResponseAssert, EofResponse> {
        public EofResponseAssert(EofResponse actual) {
            super(actual, EofResponseAssert.class);
        }
    }

    public FieldCountAssert isFieldCount() {
        isNotNull();
        isInstanceOf(ColumnCount.class);
        return FieldCountAssert.assertThat((ColumnCount) actual);
    }

    public static class FieldCountAssert extends AbstractAssert<FieldCountAssert, ColumnCount> {
        public FieldCountAssert(ColumnCount actual) {
            super(actual, FieldCountAssert.class);
        }

        public static FieldCountAssert assertThat(ColumnCount actual) {
            return new FieldCountAssert(actual);
        }

        public FieldCountAssert hasFieldCount(int count) {
            isNotNull();
            if (count != actual.getFieldCount()) {
                failWithMessage("Expected a field count of <%i> but is <%i>", count, actual.getFieldCount());
            }
            return this;
        }
    }
    public OkResponseAssert isOkResponse() {
        isNotNull();
        if (actual instanceof ErrorResponse) {
            throw new AssertionError("Expected  OkResponse but got ErrorResponse: " + ((ErrorResponse)actual).getMessage());
        }
        isInstanceOf(OkResponse.class);
        return OkResponseAssert.assertThat((OkResponse) actual);
    }

    public static class OkResponseAssert extends AbstractAssert<OkResponseAssert, OkResponse> {
        public OkResponseAssert(OkResponse actual) {
            super(actual, OkResponseAssert.class);
        }

        public static OkResponseAssert assertThat(OkResponse actual) {
            return new OkResponseAssert(actual);
        }

        public OkResponseAssert hasAffectedRows(long affectedRows) {
            isNotNull();
            if (affectedRows != actual.getAffectedRows()) {
                failWithMessage("Expected affected rows to be <%i> but is <%i>");
            }
            return this;
        }
    }

    public ResultsetRowAssert isResultsetRow() {
        isNotNull();
        isInstanceOf(ResultsetRow.class);
        return new ResultsetRowAssert((ResultsetRow)actual);
    }

    public static class ResultsetRowAssert extends AbstractAssert<ResultsetRowAssert, ResultsetRow> {
        public ResultsetRowAssert(ResultsetRow actual) {
            super(actual, ResultsetRowAssert.class);
        }

        public AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>> hasValues() {
            return Assertions.assertThat(actual.getValues());
        }
    }

}
