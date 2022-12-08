/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.alert.email;

import org.apache.dolphinscheduler.alert.api.AlertChannel;
import org.apache.dolphinscheduler.alert.api.AlertData;
import org.apache.dolphinscheduler.alert.api.AlertInfo;
import org.apache.dolphinscheduler.alert.api.AlertResult;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EmailAlertChannel implements AlertChannel {

    private static final Logger logger = LoggerFactory.getLogger(EmailAlertChannel.class);

    @Override
    public AlertResult process(AlertInfo info) {

        Map<String, String> paramsMap = info.getAlertParams();
        if (null == paramsMap) {
            return AlertResult.error("mail params is null");
        }
        MailSender mailSender = new MailSender(paramsMap);
        AlertResult alertResult = mailSender.sendMails(info);

        boolean flag;

        if (alertResult == null) {
            alertResult = new AlertResult();
            alertResult.setSuccess(false);
            alertResult.setMessage("alert send error.");
            logger.info("alert send error : {}", alertResult.getMessage());
            return alertResult;
        }

        flag = alertResult.isSuccess();

        if (flag) {
            logger.info("alert send success");
            alertResult.setMessage("email send success.");
        } else {
            alertResult.setMessage("alert send error.");
            logger.info("alert send error : {}", alertResult.getMessage());
        }

        return alertResult;
    }
}
