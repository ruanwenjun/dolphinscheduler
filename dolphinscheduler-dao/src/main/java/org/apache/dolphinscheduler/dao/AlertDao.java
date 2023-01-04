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

package org.apache.dolphinscheduler.dao;

import org.apache.dolphinscheduler.common.enums.AlertStatus;
import org.apache.dolphinscheduler.dao.dto.AlertPluginInstanceDTO;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.apache.dolphinscheduler.dao.entity.AlertPluginInstance;
import org.apache.dolphinscheduler.dao.entity.AlertSendStatus;
import org.apache.dolphinscheduler.dao.mapper.AlertGroupMapper;
import org.apache.dolphinscheduler.dao.mapper.AlertMapper;
import org.apache.dolphinscheduler.dao.mapper.AlertPluginInstanceMapper;
import org.apache.dolphinscheduler.dao.mapper.AlertSendStatusMapper;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.base.Strings;

import lombok.NonNull;

@Component
public class AlertDao {

    private final int QUERY_ALERT_THRESHOLD = 100;

    @Value("${alert.alarm-suppression.crash:60}")
    private Integer crashAlarmSuppression;

    @Autowired
    private AlertMapper alertMapper;

    @Autowired
    private AlertPluginInstanceMapper alertPluginInstanceMapper;

    @Autowired
    private AlertGroupMapper alertGroupMapper;

    @Autowired
    private AlertSendStatusMapper alertSendStatusMapper;

    /**
     * insert alert
     *
     * @param alert alert
     * @return add alert result
     */
    public int addAlert(Alert alert) {
        String sign = generateSign(alert);
        alert.setSign(sign);
        return alertMapper.insert(alert);
    }

    /**
     * update alert sending(execution) status
     *
     * @param alertStatus alertStatus
     * @param log alert results json
     * @param id id
     * @return update alert result
     */
    public int updateAlert(AlertStatus alertStatus, String log, int id) {
        Alert alert = new Alert();
        alert.setId(id);
        alert.setAlertStatus(alertStatus);
        alert.setUpdateTime(new Date());
        alert.setLog(log);
        return alertMapper.updateById(alert);
    }

    /**
     * generate sign for alert
     *
     * @param alert alert
     * @return sign's str
     */
    private String generateSign(Alert alert) {
        return Optional.of(alert)
                .map(Alert::getContent)
                .map(DigestUtils::sha1Hex)
                .map(String::toLowerCase)
                .orElse("");
    }

    /**
     * add AlertSendStatus
     *
     * @param sendStatus alert send status
     * @param log log
     * @param alertId alert id
     * @param alertPluginInstanceId alert plugin instance id
     * @return insert count
     */
    public int addAlertSendStatus(AlertStatus sendStatus, String log, int alertId, int alertPluginInstanceId) {
        AlertSendStatus alertSendStatus = new AlertSendStatus();
        alertSendStatus.setAlertId(alertId);
        alertSendStatus.setAlertPluginInstanceId(alertPluginInstanceId);
        alertSendStatus.setSendStatus(sendStatus);
        alertSendStatus.setLog(log);
        alertSendStatus.setCreateTime(new Date());
        return alertSendStatusMapper.insert(alertSendStatus);
    }

    public int insertAlertSendStatus(List<AlertSendStatus> alertSendStatuses) {
        if (CollectionUtils.isEmpty(alertSendStatuses)) {
            return 0;
        }
        return alertSendStatusMapper.batchInsert(alertSendStatuses);
    }

    /**
     * List alerts that are pending for execution
     */
    public List<Alert> listPendingAlerts() {
        LambdaQueryWrapper<Alert> wrapper = new QueryWrapper<>(new Alert())
                .lambda()
                .eq(Alert::getAlertStatus, AlertStatus.WAIT_EXECUTION)
                .last("limit " + QUERY_ALERT_THRESHOLD);
        return alertMapper.selectList(wrapper);
    }

    public List<Alert> listAlerts(int processInstanceId) {
        LambdaQueryWrapper<Alert> wrapper = new QueryWrapper<>(new Alert()).lambda()
                .eq(Alert::getProcessInstanceId, processInstanceId);
        return alertMapper.selectList(wrapper);
    }

    /**
     * for test
     *
     * @return AlertMapper
     */
    public AlertMapper getAlertMapper() {
        return alertMapper;
    }

    /**
     * list all alert plugin instance by alert group id
     *
     * @param alertGroupId alert group id
     * @return AlertPluginInstance list
     */
    public List<AlertPluginInstance> listInstanceByAlertGroupId(int alertGroupId) {
        String alertInstanceIdsParam = alertGroupMapper.queryAlertGroupInstanceIdsById(alertGroupId);
        if (!Strings.isNullOrEmpty(alertInstanceIdsParam)) {
            String[] idsArray = alertInstanceIdsParam.split(",");
            List<Integer> ids = Arrays.stream(idsArray)
                    .map(s -> Integer.parseInt(s.trim()))
                    .collect(Collectors.toList());
            return alertPluginInstanceMapper.queryByIds(ids);
        }
        return null;
    }

    public List<AlertPluginInstanceDTO> listInstanceDTOByAlertGroupId(int alertGroupId) {
        List<AlertPluginInstance> alertPluginInstances = listInstanceByAlertGroupId(alertGroupId);
        if (CollectionUtils.isEmpty(alertPluginInstances)) {
            return Collections.emptyList();
        }
        return alertPluginInstances.stream()
                .map(AlertPluginInstanceDTO::new)
                .collect(Collectors.toList());
    }

    public AlertPluginInstanceMapper getAlertPluginInstanceMapper() {
        return alertPluginInstanceMapper;
    }

    public void setAlertPluginInstanceMapper(AlertPluginInstanceMapper alertPluginInstanceMapper) {
        this.alertPluginInstanceMapper = alertPluginInstanceMapper;
    }

    public AlertGroupMapper getAlertGroupMapper() {
        return alertGroupMapper;
    }

    public void setAlertGroupMapper(AlertGroupMapper alertGroupMapper) {
        this.alertGroupMapper = alertGroupMapper;
    }

    public void setCrashAlarmSuppression(Integer crashAlarmSuppression) {
        this.crashAlarmSuppression = crashAlarmSuppression;
    }

    public void insertAlertWhenServerCrash(@NonNull Alert alert) {
        // we use this method to avoid insert duplicate alert(issue #5525)
        // we modified this method to optimize performance(issue #9174)
        Date crashAlarmSuppressionStartTime = Date.from(
                LocalDateTime.now().plusMinutes(-crashAlarmSuppression).atZone(ZoneId.systemDefault()).toInstant());
        alertMapper.insertAlertWhenServerCrash(alert, crashAlarmSuppressionStartTime);
    }

    public void deleteAlertByWorkflowInstanceId(int workflowInstanceId) {
        List<Alert> alertList = alertMapper.selectByWorkflowInstanceId(workflowInstanceId);
        if (CollectionUtils.isEmpty(alertList)) {
            return;
        }
        // delete alert send result
        for (Alert alert : alertList) {
            alertSendStatusMapper.deleteByAlertId(alert.getId());
            alertMapper.deleteById(alert.getId());
        }
    }
}
