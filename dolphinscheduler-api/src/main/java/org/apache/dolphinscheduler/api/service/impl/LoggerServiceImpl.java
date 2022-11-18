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

package org.apache.dolphinscheduler.api.service.impl;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.dto.RollViewLogResponse;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.LoggerService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.remote.command.log.RollViewLogResponseCommand;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.service.log.LogClient;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.DOWNLOAD_LOG;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.VIEW_LOG;

/**
 * logger service impl
 */
@Service
public class LoggerServiceImpl extends BaseServiceImpl implements LoggerService {

    private static final Logger logger = LoggerFactory.getLogger(LoggerServiceImpl.class);

    private static final String LOG_HEAD_FORMAT = "[LOG-PATH]: %s, [HOST]:  %s%s";

    @Autowired
    private ProcessService processService;

    @Autowired
    private LogClient logClient;

    @Autowired
    ProjectMapper projectMapper;

    @Autowired
    ProjectService projectService;

    @Autowired
    TaskDefinitionMapper taskDefinitionMapper;

    /**
     * view log
     *
     * @param taskInstId  task instance id
     * @param skipLineNum skip line number
     * @param limit       limit
     * @return log string data
     */
    @Override
    @SuppressWarnings("unchecked")
    public Result<RollViewLogResponse> queryLog(int taskInstId, int skipLineNum, int limit) {

        TaskInstance taskInstance = processService.findTaskInstanceById(taskInstId);

        if (taskInstance == null) {
            return Result.error(Status.TASK_INSTANCE_NOT_FOUND);
        }
        if (StringUtils.isBlank(taskInstance.getHost())) {
            return Result.error(Status.TASK_INSTANCE_HOST_IS_NULL);
        }
        RollViewLogResponse rollViewLogResponse = queryLog(taskInstance, skipLineNum, limit);
        return Result.success(rollViewLogResponse);
    }

    /**
     * get log size
     *
     * @param taskInstId task instance id
     * @return log byte array
     */
    @Override
    public byte[] getLogBytes(int taskInstId) {
        TaskInstance taskInstance = processService.findTaskInstanceById(taskInstId);
        if (taskInstance == null || StringUtils.isBlank(taskInstance.getHost())) {
            throw new ServiceException("task instance is null or host is null");
        }
        return getLogBytes(taskInstance);
    }

    /**
     * query log
     *
     * @param loginUser   login user
     * @param projectCode project code
     * @param taskInstId  task instance id
     * @param skipLineNum skip line number
     * @param limit       limit
     * @return log string data
     */
    @Override
    @SuppressWarnings("unchecked")
    public RollViewLogResponse queryLog(User loginUser, long projectCode, int taskInstId, int skipLineNum, int limit) {
        Project project = projectMapper.queryByCode(projectCode);
        // check user access for project
        projectService.checkProjectAndAuth(loginUser, project, projectCode, VIEW_LOG);
        // check whether the task instance can be found
        TaskInstance task = processService.findTaskInstanceById(taskInstId);
        if (task == null || StringUtils.isBlank(task.getHost())) {
            throw new ServiceException(Status.TASK_INSTANCE_NOT_FOUND);
        }

        TaskDefinition taskDefinition = taskDefinitionMapper.queryByCode(task.getTaskCode());
        if (taskDefinition != null && projectCode != taskDefinition.getProjectCode()) {
            throw new ServiceException(Status.TASK_INSTANCE_NOT_FOUND, taskInstId);
        }
        return queryLog(task, skipLineNum, limit);
    }

    /**
     * get log bytes
     *
     * @param loginUser   login user
     * @param projectCode project code
     * @param taskInstId  task instance id
     * @return log byte array
     */
    @Override
    public byte[] getLogBytes(User loginUser, long projectCode, int taskInstId) {
        Project project = projectMapper.queryByCode(projectCode);
        // check user access for project
        projectService.checkProjectAndAuth(loginUser, project, projectCode, DOWNLOAD_LOG);

        // check whether the task instance can be found
        TaskInstance task = processService.findTaskInstanceById(taskInstId);
        if (task == null || StringUtils.isBlank(task.getHost())) {
            throw new ServiceException("task instance is null or host is null");
        }

        TaskDefinition taskDefinition = taskDefinitionMapper.queryByCode(task.getTaskCode());
        if (taskDefinition != null && projectCode != taskDefinition.getProjectCode()) {
            throw new ServiceException("task instance does not exist in project");
        }
        return getLogBytes(task);
    }

    /**
     * query log
     *
     * @param taskInstance task instance
     * @param skipLineNum  skip line number
     * @param limit        limit
     * @return log string data
     */
    private RollViewLogResponse queryLog(TaskInstance taskInstance, int skipLineNum, int limit) {
        Host host = Host.of(taskInstance.getHost());
        StringBuilder log = new StringBuilder();
        if (skipLineNum == 0) {
            String head =
                    String.format(LOG_HEAD_FORMAT, taskInstance.getLogPath(), host, Constants.SYSTEM_LINE_SEPARATOR);
            log.append(head);
        }
        RollViewLogResponseCommand rollViewLogResponseCommand =
                logClient.rollViewLog(host, taskInstance.getLogPath(), skipLineNum, limit);
        if (rollViewLogResponseCommand.getResponseStatus() != RollViewLogResponseCommand.Status.SUCCESS) {
            log.append(rollViewLogResponseCommand.getResponseStatus().getDesc());
            return RollViewLogResponse.builder()
                    .log(log.toString())
                    .hasNext(false)
                    .build();
        }
        log.append(rollViewLogResponseCommand.getLog());
        // If the task doesn't finish or the log doesn't end can query next
        return RollViewLogResponse.builder()
                .log(log.toString())
                .currentLogLineNumber(rollViewLogResponseCommand.getCurrentLineNumber())
                .hasNext(!taskInstance.getState().typeIsFinished()
                        || rollViewLogResponseCommand.getCurrentLineNumber() < rollViewLogResponseCommand
                                .getCurrentTotalLineNumber())
                .build();
    }

    /**
     * get log bytes
     *
     * @param taskInstance task instance
     * @return log byte array
     */
    private byte[] getLogBytes(TaskInstance taskInstance) {
        Host host = Host.of(taskInstance.getHost());
        byte[] head = String.format(LOG_HEAD_FORMAT,
                taskInstance.getLogPath(),
                host,
                Constants.SYSTEM_LINE_SEPARATOR).getBytes(StandardCharsets.UTF_8);
        return Bytes.concat(head,
                logClient.getLogBytes(host.getIp(), host.getPort(), taskInstance.getLogPath()));
    }
}
