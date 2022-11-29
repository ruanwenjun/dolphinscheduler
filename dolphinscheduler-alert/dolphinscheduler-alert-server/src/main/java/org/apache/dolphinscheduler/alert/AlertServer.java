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

import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.registry.AlertRegistryClient;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.dao.PluginDao;
import org.apache.dolphinscheduler.remote.config.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.event.EventListener;

import javax.annotation.PreDestroy;
import java.io.Closeable;

@SpringBootApplication
@ComponentScan(basePackages = "org.apache.dolphinscheduler", excludeFilters = {
        @ComponentScan.Filter(type = FilterType.REGEX, pattern = {
                "org.apache.dolphinscheduler.service.process.*",
                "org.apache.dolphinscheduler.service.queue.*",
        })
})
public class AlertServer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AlertServer.class);

    @Autowired
    private PluginDao pluginDao;
    @Autowired
    private AlertSenderBootstrap alertSenderService;
    @Autowired
    private AlertConfig alertConfig;
    @Autowired
    private AlertRegistryClient alertRegistryClient;

    public static void main(String[] args) {
        Thread.currentThread().setName(Constants.THREAD_NAME_ALERT_SERVER);
        new SpringApplicationBuilder(AlertServer.class).run(args);
    }

    @EventListener
    public void run(ApplicationReadyEvent readyEvent) {
        logger.info("Alert server is staring ...");

        checkTable();
        startServer();
        alertRegistryClient.start();
        alertSenderService.start();
        logger.info("Alert server is started ...");
    }

    @Override
    @PreDestroy
    public void close() {
        destroy("alert server destroy");
    }

    /**
     * gracefully stop
     *
     * @param cause stop cause
     */
    public void destroy(String cause) {

        try {
            // set stop signal is true
            // execute only once
            if (!ServerLifeCycleManager.toStopped()) {
                logger.warn("AlterServer is already stopped");
                return;
            }

            logger.info("Alert server is stopping, cause: {}", cause);

            // thread sleep 3 seconds for thread quietly stop
            ThreadUtils.sleep(Constants.SERVER_CLOSE_WAIT_TIME.toMillis());

            logger.info("Alter server stopped, cause: {}", cause);
        } catch (Exception e) {
            logger.error("Alert server stop failed, cause: {}", cause, e);
        }
    }

    private void checkTable() {
        if (!pluginDao.checkPluginDefineTableExist()) {
            logger.error("Plugin Define Table t_ds_plugin_define Not Exist . Please Create it First !");
            System.exit(1);
        }
    }

    private void startServer() {
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(alertConfig.getListenPort());
    }

}
