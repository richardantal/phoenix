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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.time.DateUtils;
import org.apache.phoenix.expression.Expression;

/**
 * 
 * Floor function that rounds up the {@link Date} to start of month.
 */
public class FloorMonthExpression extends RoundJavaDateExpression {

    public FloorMonthExpression() {
        super();
    }

    public FloorMonthExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public long roundDateTime(Date dateTime) {
        Date date = DateUtils.truncate(dateTime, Calendar.MONTH);

        // TODO Should not be done any modification here

        Calendar calendar = Calendar.getInstance();
        TimeZone tz = calendar.getTimeZone();
        ZoneId zid = tz == null ? ZoneId.systemDefault() : tz.toZoneId();

        calendar.setTime(date);
        LocalDateTime localDate = LocalDateTime.ofInstant(date.toInstant(), zid);
        Date da = Date.from(localDate.toInstant(ZoneOffset.UTC));

        return da.getTime();
    }

}
