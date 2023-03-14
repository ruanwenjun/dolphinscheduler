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

package org.apache.dolphinscheduler.service.alert;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.DqExecuteResultAlertContent;
import org.apache.dolphinscheduler.alert.api.content.ServerCrashAlertContent;
import org.apache.dolphinscheduler.alert.api.content.TaskResultAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.AlertStatus;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.apache.dolphinscheduler.dao.entity.DqExecuteResult;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.remote.command.alert.TaskAlertRequestCommand;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.spi.utils.DateUtils;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.NonNull;

@Component
public class AlertManager {

    private static final Logger logger = LoggerFactory.getLogger(AlertManager.class);

    @Autowired
    protected AlertDao alertDao;

    @Autowired
    protected ProcessService processService;

    protected boolean WORKER_FINISH_CLOSE_ALERT = PropertyUtils.getBoolean("workflow.finish.close.alert", true);

    protected Map<AlertType, AlertContentFactory<? extends AlertContent>> alertContentFactoryMap = new HashMap<>();

    public AlertManager() {
        ServiceLoader<AlertContentFactory> serviceLoader = ServiceLoader.load(AlertContentFactory.class);
        serviceLoader.forEach(alertContentFactory -> alertContentFactoryMap.put(alertContentFactory.getAlertType(),
                (AlertContentFactory<? extends AlertContent>) alertContentFactory));
    }

    public void workflowInstanceFinishedSendAlert(@NonNull ProcessInstance processInstance) {
        try {
            if (!AlertUtils.isNeedToSendWorkflowAlert(processInstance)) {
                return;
            }
            if (processInstance.getState().typeIsSuccess()) {
                workflowInstanceSuccessSendAlertIfNeeded(processInstance);
                return;
            }
            if (processInstance.getState().typeIsFailure()) {
                workflowInstanceFailedSendAlertIfNeeded(processInstance);
                return;
            }
            if (processInstance.getState().typeIsBlock()) {
                workflowInstanceBlockSendAlertIfNeeded(processInstance);
                return;
            }
        } catch (Exception ex) {
            logger.error("Workflow instance finished, send alert error", ex);
        }
    }

    public void workflowInstanceSuccessSendAlertIfNeeded(@NonNull ProcessInstance processInstance) {
        try {
            // todo: add project Name and user name in process instance
            ProjectUser projectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());
            AlertContent alertContent = alertContentFactoryMap.get(AlertType.PROCESS_INSTANCE_SUCCESS)
                    .generateAlertContent(processInstance, projectUser, null);

            Alert alert = Alert.builder()
                    .title(alertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(alertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(WarningType.SUCCESS)
                    .alertGroupId(processInstance.getWarningGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.PROCESS_INSTANCE_SUCCESS)
                    .build();
            alertDao.addAlert(alert);
            closeHistoryAlertIfNeeded(processInstance, projectUser);
        } catch (Exception ex) {
            logger.error("Workflow instance success, send alert failed", ex);
        }
    }

    public void workflowInstanceFailedSendAlertIfNeeded(@NonNull ProcessInstance processInstance) {
        try {
            // todo: add project Name and user name in process instance
            ProjectUser projectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());
            AlertContent alertContent = alertContentFactoryMap.get(AlertType.PROCESS_INSTANCE_FAILURE)
                    .generateAlertContent(processInstance, projectUser, null);
            Alert alert = Alert.builder()
                    .title(alertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(alertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(WarningType.FAILURE)
                    .alertGroupId(processInstance.getWarningGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.PROCESS_INSTANCE_FAILURE)
                    .build();
            alertDao.addAlert(alert);
        } catch (Exception ex) {
            logger.error("Workflow instance failure, send alert failed", ex);
        }
    }

    // todo: do we need to send failover alert?
    public void workflowInstanceFailoverSendAlertIfNeeded(@NonNull ProcessInstance processInstance) {
        try {
            ProjectUser projectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());
            AlertContent alertContent = alertContentFactoryMap.get(AlertType.WORKFLOW_FAULT_TOLERANCE)
                    .generateAlertContent(processInstance, projectUser, null);
            Alert alert = Alert.builder()
                    .title(alertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(alertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(WarningType.ALL)
                    .alertGroupId(processInstance.getWarningGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.WORKFLOW_FAULT_TOLERANCE)
                    .build();
            alertDao.addAlert(alert);
        } catch (Exception ex) {
            logger.error("Workflow instance failover send alert if needed", ex);
        }
    }

    // todo: do we need to send block alert?
    public void workflowInstanceBlockSendAlertIfNeeded(@NonNull ProcessInstance processInstance) {
        try {
            ProjectUser projectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());
            AlertContent alertContent = alertContentFactoryMap.get(AlertType.PROCESS_INSTANCE_BLOCKED)
                    .generateAlertContent(processInstance, projectUser, null);
            Alert alert = Alert.builder()
                    .title(alertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(alertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(WarningType.ALL)
                    .alertGroupId(processInstance.getWarningGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.PROCESS_INSTANCE_BLOCKED)
                    .build();
            alertDao.addAlert(alert);
            logger.info("processInstance {} block alert send successful!", processInstance.getId());
        } catch (Exception ex) {
            logger.error("Workflow instance timeout send alert error", ex);
        }
    }

    // todo: does the workflow instance will timeout config?
    public void workflowInstanceTimeoutSendAlertIfNeeded(@NonNull ProcessInstance processInstance) {
        try {
            ProjectUser projectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());
            AlertContent alertContent = alertContentFactoryMap.get(AlertType.PROCESS_INSTANCE_TIMEOUT)
                    .generateAlertContent(processInstance, projectUser, null);
            Alert alert = Alert.builder()
                    .title(alertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(alertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(processInstance.getWarningType())
                    .alertGroupId(processInstance.getWarningGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.PROCESS_INSTANCE_TIMEOUT)
                    .build();
            alertDao.addAlert(alert);
        } catch (Exception ex) {
            logger.error("Workflow instance timeout send alert error", ex);
        }
    }

    public void taskTimeoutSendAlertIfNeeded(@NonNull ProcessInstance processInstance,
                                             @NonNull TaskInstance taskInstance) {
        try {
            TaskTimeoutStrategy taskTimeoutStrategy = taskInstance.getTaskDefine().getTimeoutNotifyStrategy();
            if (!(TaskTimeoutStrategy.WARN == taskTimeoutStrategy
                    || TaskTimeoutStrategy.WARNFAILED == taskTimeoutStrategy)) {
                return;
            }
            ProjectUser projectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());
            AlertContent alertContent = alertContentFactoryMap.get(AlertType.TASK_TIMEOUT)
                    .generateAlertContent(processInstance, projectUser, taskInstance);
            Alert alert = Alert.builder()
                    .title(alertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(alertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(WarningType.ALL)
                    .alertGroupId(processInstance.getWarningGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.TASK_TIMEOUT)
                    .build();
            alertDao.addAlert(alert);
        } catch (Exception ex) {
            logger.error("TaskInstance timeout send alert error", ex);
        }
    }

    public void taskFailedSendAlertIfNeeded(@NonNull ProcessInstance processInstance,
                                            @NonNull TaskInstance taskInstance) {
        try {
            ProjectUser projectUser = processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());
            AlertContent alertContent = alertContentFactoryMap.get(AlertType.TASK_FAILURE)
                    .generateAlertContent(processInstance, projectUser, taskInstance);
            Alert alert = Alert.builder()
                    .title(alertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(alertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(processInstance.getWarningType())
                    .alertGroupId(processInstance.getWarningGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.TASK_FAILURE)
                    .build();
            alertDao.addAlert(alert);
        } catch (Exception ex) {
            logger.error("TaskInstance timeout send alert error", ex);
        }
    }

    private void closeHistoryAlertIfNeeded(ProcessInstance processInstance, ProjectUser projectUser) {
        if (!WORKER_FINISH_CLOSE_ALERT) {
            return;
        }

        List<Alert> alerts = alertDao.listAlerts(processInstance.getId());
        if (CollectionUtils.isEmpty(alerts)) {
            // no need to close alert
            return;
        }
        if (processInstance.getWarningGroupId() == null || processInstance.getWarningGroupId() == 0) {
            // no need to close alert if not close alert
            return;
        }
        AlertContent alertContent = alertContentFactoryMap.get(AlertType.CLOSE_ALERT)
                .generateAlertContent(processInstance, projectUser, null);
        Alert alert = new Alert();
        alert.setTitle(alertContent.getAlertTitle());
        alert.setContent(JSONUtils.toJsonString(alertContent));
        alert.setAlertGroupId(processInstance.getWarningGroupId());
        alert.setUpdateTime(new Date());
        alert.setCreateTime(new Date());
        alert.setProjectCode(processInstance.getProcessDefinition().getProjectCode());
        alert.setProcessDefinitionCode(processInstance.getProcessDefinitionCode());
        alert.setProcessInstanceId(processInstance.getId());
        alert.setAlertType(AlertType.CLOSE_ALERT);
        alertDao.addAlert(alert);
    }

    public void sendTaskResultAlert(@NonNull TaskAlertRequestCommand taskAlertRequest) {
        try {
            ProjectUser projectUser =
                    processService.queryProjectWithUserByProcessInstanceId(taskAlertRequest.getWorkflowInstanceId());
            TaskResultAlertContent taskResultAlertContent = TaskResultAlertContent.builder()
                    .projectName(projectUser.getProjectName())
                    .workflowInstanceName(taskAlertRequest.getWorkflowInstanceName())
                    .taskName(taskAlertRequest.getTaskName())
                    .startTime(DateUtils.transformToDate(taskAlertRequest.getStartTime()))
                    .endTime(DateUtils.transformToDate(taskAlertRequest.getEndTime()))
                    .title(taskAlertRequest.getTitle())
                    .result(taskAlertRequest.getContent())
                    .build();
            Alert alert = Alert.builder()
                    .title(taskAlertRequest.getTitle())
                    .content(JSONUtils.toJsonString(taskResultAlertContent))
                    .alertStatus(AlertStatus.WAIT_EXECUTION)
                    .warningType(WarningType.ALL)
                    .alertGroupId(taskAlertRequest.getGroupId())
                    .createTime(new Date())
                    .updateTime(new Date())
                    .projectCode(projectUser.getProjectCode())
                    .alertType(AlertType.TASK_RESULT)
                    .build();
            alertDao.addAlert(alert);
        } catch (Exception ex) {
            logger.error("Send task result alert error", ex);
        }
    }

    public void sendDataQualityTaskExecuteResultAlert(DqExecuteResult result, ProcessInstance processInstance) {
        try {
            ProjectUser projectUser =
                    processService.queryProjectWithUserByProcessInstanceId(processInstance.getId());

            DqExecuteResultAlertContent dqExecuteResultAlertContent = DqExecuteResultAlertContent.builder()
                    .projectName(projectUser.getProjectName())
                    .workflowInstanceName(result.getProcessDefinitionName())
                    .taskName(result.getTaskName())
                    .ruleType(result.getRuleType())
                    .ruleName(result.getRuleName())
                    .statisticsValue(result.getStatisticsValue())
                    .comparisonValue(result.getComparisonValue())
                    .checkType(result.getCheckType())
                    .threshold(result.getThreshold())
                    .operator(result.getOperator())
                    .failureStrategy(result.getFailureStrategy())
                    .userId(result.getUserId())
                    .userName(result.getUserName())
                    .state(result.getState())
                    .errorDataPath(result.getErrorOutputPath())
                    .alertCreateTime(new Date())
                    .startTime(result.getCreateTime())
                    .endTime(result.getUpdateTime())
                    .build();

            Alert alert = Alert.builder()
                    .title(dqExecuteResultAlertContent.getAlertTitle())
                    .content(JSONUtils.toJsonString(dqExecuteResultAlertContent))
                    // todo: we need to use task instance warning group id
                    .alertGroupId(processInstance.getWarningGroupId())
                    .warningType(processInstance.getWarningType())
                    .createTime(new Date())
                    .projectCode(result.getProjectCode())
                    .processDefinitionCode(processInstance.getProcessDefinitionCode())
                    .processInstanceId(processInstance.getId())
                    .alertType(AlertType.DATA_QUALITY_TASK_RESULT)
                    .build();
            alertDao.addAlert(alert);
            logger.info("Add data quality alert to db , alert: {}", alert);
        } catch (Exception ex) {
            logger.error("Send data quality alert error, result: {}", result, ex);
        }
    }

    public void sendServerStoppedAlert(String path) {
        ServerCrashAlertContent serverCrashAlertContent = ServerCrashAlertContent.builder()
                .serverPath(path)
                .alertCreateTime(new Date())
                .build();
        String content = JSONUtils.toJsonString(serverCrashAlertContent);

        Alert alert = Alert.builder()
                .title(serverCrashAlertContent.getAlertTitle())
                .content(content)
                .alertGroupId(Constants.DEFAULT_ALERT_GROUP_ID)
                .warningType(WarningType.FAILURE)
                .createTime(new Date())
                .updateTime(new Date())
                .alertType(AlertType.SERVER_CRASH_ALERT)
                .build();
        alertDao.addAlert(alert);
    }

}
