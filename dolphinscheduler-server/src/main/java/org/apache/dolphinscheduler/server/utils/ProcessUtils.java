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

package org.apache.dolphinscheduler.server.utils;

import lombok.NonNull;
import static org.apache.dolphinscheduler.common.Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE;
import static org.apache.dolphinscheduler.common.Constants.YARN_APPLICATION_STATUS_ADDRESS;
import static org.apache.dolphinscheduler.common.Constants.YARN_JOB_HISTORY_STATUS_ADDRESS;
import static org.apache.dolphinscheduler.common.Constants.YARN_RESOURCEMANAGER_HA_RM_IDS;
import static org.apache.dolphinscheduler.common.utils.HadoopUtils.HADOOP_RESOURCE_MANAGER_HTTP_ADDRESS_PORT_VALUE;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.exception.BaseException;
import org.apache.dolphinscheduler.common.storage.StorageOperate;
import org.apache.dolphinscheduler.common.utils.CommonUtils;
import org.apache.dolphinscheduler.common.utils.FileUtils;
import org.apache.dolphinscheduler.common.utils.HadoopUtils;
import org.apache.dolphinscheduler.common.utils.HttpUtils;
import org.apache.dolphinscheduler.common.utils.KerberosHttpClient;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.log.LogClient;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * mainly used to get the start command line of a process.
 */
public class ProcessUtils {

    private static final Logger logger = LoggerFactory.getLogger(ProcessUtils.class);
    private static final String RM_HA_IDS = org.apache.dolphinscheduler.spi.utils.PropertyUtils.getString(YARN_RESOURCEMANAGER_HA_RM_IDS);
    private static final String APP_ADDRESS = org.apache.dolphinscheduler.spi.utils.PropertyUtils.getString(YARN_APPLICATION_STATUS_ADDRESS);
    private static final String JOB_HISTORY_ADDRESS = org.apache.dolphinscheduler.spi.utils.PropertyUtils.getString(YARN_JOB_HISTORY_STATUS_ADDRESS);

    /**
     * Initialization regularization, solve the problem of pre-compilation performance,
     * avoid the thread safety problem of multi-thread operation
     */
    private static final Pattern MACPATTERN = Pattern.compile("-[+|-]-\\s(\\d+)");

    /**
     * Expression of PID recognition in Windows scene
     */
    private static final Pattern WINDOWSATTERN = Pattern.compile("\\w+\\((\\d+)\\)");

    /**
     * kill yarn application.
     *
     * @param appIds app id list
     * @param logger logger
     * @param tenantCode tenant code
     * @param executePath execute path
     */
    public static void cancelApplication(List<String> appIds, Logger logger, String tenantCode, String executePath) {
        if (appIds == null || appIds.isEmpty()) {
            return;
        }

        for (String appId : appIds) {
            try {
                StorageOperate storageOperate = SpringApplicationContext.getBean(StorageOperate.class);
                if (storageOperate == null) {
                    logger.info("storage operate is null, will skip kill yarn application");
                    return;
                }
                ExecutionStatus applicationStatus = getApplicationStatus(appId);

                if (!applicationStatus.typeIsFinished()) {
                    String commandFile = String.format("%s/%s.kill", executePath, appId);
                    String cmd = getKerberosInitCommand() + "yarn application -kill " + appId;
                    execYarnKillCommand(logger, tenantCode, appId, commandFile, cmd);
                }
            } catch (Exception e) {
                logger.error("Get yarn application app id [{}}] status failed", appId, e);
            }
        }
    }

    /**
     * get the state of an application
     *
     * @param applicationId application id
     * @return the return may be null or there may be other parse exceptions
     */
    public static ExecutionStatus getApplicationStatus(String applicationId) throws BaseException {
        if (StringUtils.isEmpty(applicationId)) {
            return null;
        }

        String result;
        String applicationUrl = getApplicationUrl(applicationId);
        logger.debug("generate yarn application url, applicationUrl={}", applicationUrl);

        String responseContent = Boolean.TRUE
            .equals(org.apache.dolphinscheduler.spi.utils.PropertyUtils.getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false))
            ? KerberosHttpClient.get(applicationUrl)
            : HttpUtils.get(applicationUrl);
        if (responseContent != null) {
            ObjectNode jsonObject = JSONUtils.parseObject(responseContent);
            if (!jsonObject.has("app")) {
                return ExecutionStatus.FAILURE;
            }
            result = jsonObject.path("app").path("finalStatus").asText();

        } else {
            // may be in job history
            String jobHistoryUrl = getJobHistoryUrl(applicationId);
            logger.debug("generate yarn job history application url, jobHistoryUrl={}", jobHistoryUrl);
            responseContent = Boolean.TRUE
                .equals(org.apache.dolphinscheduler.spi.utils.PropertyUtils.getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false))
                ? KerberosHttpClient.get(jobHistoryUrl)
                : HttpUtils.get(jobHistoryUrl);

            if (null != responseContent) {
                ObjectNode jsonObject = JSONUtils.parseObject(responseContent);
                if (!jsonObject.has("job")) {
                    return ExecutionStatus.FAILURE;
                }
                result = jsonObject.path("job").path("state").asText();
            } else {
                return ExecutionStatus.FAILURE;
            }
        }

        return getExecutionStatus(result);
    }

    private static ExecutionStatus getExecutionStatus(String result) {
        switch (result) {
            case "ACCEPTED":
                return ExecutionStatus.SUBMITTED_SUCCESS;
            case "SUCCEEDED":
            case "ENDED":
                return ExecutionStatus.SUCCESS;
            case "NEW":
            case "NEW_SAVING":
            case "SUBMITTED":
            case "FAILED":
                return ExecutionStatus.FAILURE;
            case "KILLED":
                return ExecutionStatus.KILL;
            case "RUNNING":
            default:
                return ExecutionStatus.RUNNING_EXECUTION;
        }
    }

    private static String getApplicationUrl(String applicationId) throws BaseException {

        String appUrl = StringUtils.isEmpty(RM_HA_IDS) ? APP_ADDRESS : getAppAddress(APP_ADDRESS, RM_HA_IDS);
        if (StringUtils.isBlank(appUrl)) {
            throw new BaseException("yarn application url generation failed");
        }
        logger.debug("yarn application url:{}, applicationId:{}", appUrl, applicationId);
        return String.format(appUrl, HADOOP_RESOURCE_MANAGER_HTTP_ADDRESS_PORT_VALUE, applicationId);
    }

    private static String getJobHistoryUrl(String applicationId) {
        // eg:application_1587475402360_712719 -> job_1587475402360_712719
        String jobId = applicationId.replace("application", "job");
        return String.format(JOB_HISTORY_ADDRESS, jobId);
    }

    private static String getAppAddress(String appAddress, String rmHa) {

        String[] split1 = appAddress.split(org.apache.dolphinscheduler.spi.utils.Constants.DOUBLE_SLASH);

        if (split1.length != 2) {
            return null;
        }

        String start = split1[0] + org.apache.dolphinscheduler.spi.utils.Constants.DOUBLE_SLASH;
        String[] split2 = split1[1].split(org.apache.dolphinscheduler.spi.utils.Constants.COLON);

        if (split2.length != 2) {
            return null;
        }

        String end = org.apache.dolphinscheduler.spi.utils.Constants.COLON + split2[1];

        // get active ResourceManager
        String activeRM = HadoopUtils.YarnHAAdminUtils.getActiveRMName(start, rmHa);

        if (StringUtils.isEmpty(activeRM)) {
            return null;
        }

        return start + activeRM + end;
    }

    /**
     * yarn ha admin utils
     */
    private static final class YarnHAAdminUtils {

        /**
         * get active resourcemanager node
         *
         * @param protocol http protocol
         * @param rmIds    yarn ha ids
         * @return yarn active node
         */
        public static String getActiveRMName(String protocol, String rmIds) {

            String[] rmIdArr = rmIds.split(org.apache.dolphinscheduler.spi.utils.Constants.COMMA);

            String yarnUrl = protocol + "%s:" + HADOOP_RESOURCE_MANAGER_HTTP_ADDRESS_PORT_VALUE + "/ws/v1/cluster/info";

            try {

                /**
                 * send http get request to rm
                 */

                for (String rmId : rmIdArr) {
                    String state = getRMState(String.format(yarnUrl, rmId));
                    if ("ACTIVE".equals(state)) {
                        return rmId;
                    }
                }

            } catch (Exception e) {
                logger.error("yarn ha application url generation failed, message:{}", e.getMessage());
            }
            return null;
        }

        /**
         * get ResourceManager state
         */
        public static String getRMState(String url) {

            String retStr = Boolean.TRUE
                .equals(org.apache.dolphinscheduler.common.utils.PropertyUtils
                    .getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false))
                ? KerberosHttpClient.get(url)
                : HttpUtils.get(url);

            if (StringUtils.isEmpty(retStr)) {
                return null;
            }
            // to json
            ObjectNode jsonObject = org.apache.dolphinscheduler.common.utils.JSONUtils.parseObject(retStr);

            // get ResourceManager state
            if (!jsonObject.has("clusterInfo")) {
                return null;
            }
            return jsonObject.get("clusterInfo").path("haState").asText();
        }
    }

    /**
     * get kerberos init command
     */
    static String getKerberosInitCommand() {
        logger.info("get kerberos init command");
        StringBuilder kerberosCommandBuilder = new StringBuilder();
        boolean hadoopKerberosState =
                PropertyUtils.getBoolean(Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false);
        if (hadoopKerberosState) {
            kerberosCommandBuilder.append("export KRB5_CONFIG=")
                    .append(PropertyUtils.getString(Constants.JAVA_SECURITY_KRB5_CONF_PATH))
                    .append("\n\n")
                    .append(String.format("kinit -k -t %s %s || true",
                            PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_PATH),
                            PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_USERNAME)))
                    .append("\n\n");
            logger.info("kerberos init command: {}", kerberosCommandBuilder);
        }
        return kerberosCommandBuilder.toString();
    }

    /**
     * build kill command for yarn application
     *
     * @param logger logger
     * @param tenantCode tenant code
     * @param appId app id
     * @param commandFile command file
     * @param cmd cmd
     */
    private static void execYarnKillCommand(Logger logger, String tenantCode, String appId, String commandFile,
                                            String cmd) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("#!/bin/sh\n");
            sb.append("BASEDIR=$(cd `dirname $0`; pwd)\n");
            sb.append("cd $BASEDIR\n");
            sb.append("\n\n");
            sb.append(cmd);

            File f = new File(commandFile);

            if (!f.exists()) {
                org.apache.commons.io.FileUtils.writeStringToFile(new File(commandFile), sb.toString(),
                        StandardCharsets.UTF_8);
            }

            String runCmd = String.format("%s %s", Constants.SH, commandFile);
            runCmd = OSUtils.getSudoCmd(tenantCode, runCmd);
            logger.info("kill cmd:{}", runCmd);
            OSUtils.exeCmd(runCmd);
        } catch (Exception e) {
            logger.error(String.format("Kill yarn application app id [%s] failed: [%s]", appId, e.getMessage()));
        }
    }

    /**
     * get pids str.
     *
     * @param processId process id
     * @return pids pid String
     * @throws Exception exception
     */
    public static String getPidsStr(int processId) throws Exception {
        List<String> pidList = new ArrayList<>();
        Matcher mat = null;
        // pstree pid get sub pids
        if (SystemUtils.IS_OS_MAC) {
            String pids = OSUtils.exeCmd(String.format("%s -sp %d", Constants.PSTREE, processId));
            if (null != pids) {
                mat = MACPATTERN.matcher(pids);
            }
        } else {
            String pids = OSUtils.exeCmd(String.format("%s -p %d", Constants.PSTREE, processId));
            mat = WINDOWSATTERN.matcher(pids);
        }

        if (null != mat) {
            while (mat.find()) {
                pidList.add(mat.group(1));
            }
        }

        if (CommonUtils.isSudoEnable() && !pidList.isEmpty()) {
            pidList = pidList.subList(1, pidList.size());
        }
        return String.join(" ", pidList).trim();
    }

    /**
     * find logs and kill yarn tasks.
     *
     * @param taskExecutionContext taskExecutionContext
     * @return yarn application ids
     */
    public static @Nullable List<String> killYarnJob(@NonNull LogClient logClient,
                                                     @NonNull TaskExecutionContext taskExecutionContext) {
        if (taskExecutionContext.getLogPath() == null) {
            return Collections.emptyList();
        }
        try {
            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
            Host host = Host.of(taskExecutionContext.getHost());
            List<String> appIds = logClient.getAppIds(host.getIp(), host.getPort(), taskExecutionContext.getLogPath());
            if (CollectionUtils.isNotEmpty(appIds)) {
                if (StringUtils.isEmpty(taskExecutionContext.getExecutePath())) {
                    taskExecutionContext
                            .setExecutePath(FileUtils.getProcessExecDir(
                                    taskExecutionContext.getTenantCode(),
                                    taskExecutionContext.getProjectCode(),
                                    taskExecutionContext.getProcessDefineCode(),
                                    taskExecutionContext.getProcessDefineVersion(),
                                    taskExecutionContext.getProcessInstanceId(),
                                    taskExecutionContext.getTaskInstanceId()));
                }
                FileUtils.createWorkDirIfAbsent(taskExecutionContext.getExecutePath());
                cancelApplication(appIds, logger, taskExecutionContext.getTenantCode(),
                        taskExecutionContext.getExecutePath());
                return appIds;
            } else {
                logger.info("The current appId is empty, don't need to kill the yarn job, taskInstanceId: {}",
                        taskExecutionContext.getTaskInstanceId());
            }
        } catch (Exception e) {
            logger.error("Kill yarn job failure, taskInstanceId: {}", taskExecutionContext.getTaskInstanceId(), e);
        }
        return Collections.emptyList();
    }
}
