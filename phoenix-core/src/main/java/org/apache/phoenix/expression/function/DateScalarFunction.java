/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDataType.PDataCodec;

import static org.apache.phoenix.jdbc.PhoenixConnection.getDateUtilContext;

public abstract class DateScalarFunction extends ScalarFunction {
    protected PDataCodec inputCodec;

    public DateScalarFunction() {
    }

    public DateScalarFunction(List<Expression> children) {
        super(children);
        init();
    }

    protected final PDataCodec getInputCodec() {
        return inputCodec;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }
    
    private void init() {
        PDataType returnType = getChildren().get(0).getDataType();
        inputCodec = getDateUtilContext().getCodecFor(returnType);
    }
}
