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

package org.apache.dolphinscheduler.alert;

import org.apache.commons.collections.CollectionUtils;
import org.apache.dolphinscheduler.alert.registry.AlertRegistryClient;
import org.apache.dolphinscheduler.alert.sender.AlertSender;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public final class AlertSenderBootstrap extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(AlertSenderBootstrap.class);

    @Autowired
    private AlertDao alertDao;

    @Autowired
    private AlertSender alertSender;

    @Autowired
    private AlertRegistryClient alertRegistryClient;

    @Override
    public synchronized void start() {
        super.setName("AlertSenderService");
        super.setDaemon(true);
        super.start();
    }

    @Override
    public void run() {
        logger.info("AlertSenderBootstrap started...");
        while (!ServerLifeCycleManager.isStopped()) {
            try {
                if (!alertRegistryClient.getAlertLock()) {
                    continue;
                }
                List<Alert> alerts = alertDao.listPendingAlerts();
                if (CollectionUtils.isEmpty(alerts)) {
                    logger.debug("There is not waiting alerts");
                    continue;
                }
                AlertServerMetrics.registerPendingAlertGauge(alerts::size);
                alertSender.sendAlert(alerts);
            } catch (Exception e) {
                logger.error("Alert sender thread meet an exception", e);
            } finally {
                alertRegistryClient.releaseAlertLock();
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS * 5L);
            }
        }
        logger.info("AlertSenderBootstrap stopped...");
    }
}
