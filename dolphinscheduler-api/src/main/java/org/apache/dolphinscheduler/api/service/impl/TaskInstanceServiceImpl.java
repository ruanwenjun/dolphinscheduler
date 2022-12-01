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

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ProcessInstanceService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.TaskInstanceService;
import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.dto.ListingItem;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskInstanceMapper;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionLogDao;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionLogDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.FORCED_SUCCESS;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.TASK_INSTANCE;

/**
 * task instance service impl
 */
@Service
public class TaskInstanceServiceImpl extends BaseServiceImpl implements TaskInstanceService {

    @Autowired
    ProjectMapper projectMapper;

    @Autowired
    ProjectService projectService;

    @Autowired
    ProcessService processService;

    @Autowired
    TaskInstanceMapper taskInstanceMapper;

    @Autowired
    ProcessInstanceService processInstanceService;

    @Autowired
    UsersService usersService;

    @Autowired
    TaskDefinitionMapper taskDefinitionMapper;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private ProcessInstanceDao processInstanceDao;

    @Autowired
    private ProcessDefinitionLogDao processDefinitionLogDao;

    @Autowired
    private TaskDefinitionLogDao taskDefinitionLogDao;

    /**
     * query task list by project, process instance, task name, task start time, task end time, task status, keyword paging
     *
     * @param loginUser         login user
     * @param projectCode       project code
     * @param processInstanceId process instance id
     * @param searchVal         search value
     * @param taskName          task name
     * @param stateType         state type
     * @param host              host
     * @param startDate         start time
     * @param endDate           end time
     * @param pageNo            page number
     * @param pageSize          page size
     * @return task list page
     */
    @Override
    public PageInfo<TaskInstance> queryTaskListPaging(User loginUser,
                                                      long projectCode,
                                                      Integer processInstanceId,
                                                      String processInstanceName,
                                                      String taskName,
                                                      String executorName,
                                                      String startDate,
                                                      String endDate,
                                                      String searchVal,
                                                      ExecutionStatus stateType,
                                                      String host,
                                                      Integer pageNo,
                                                      Integer pageSize) {
        PageInfo<TaskInstance> result = new PageInfo<>(pageNo, pageSize);

        Project project = projectMapper.queryByCode(projectCode);
        // check user access for project
        projectService.checkProjectAndAuth(loginUser, project, projectCode, TASK_INSTANCE);
        List<Long> processDefinitionCodes =
                processDefinitionLogDao.queryProcessDefinitionCodeByProjectCode(projectCode);
        if (CollectionUtils.isEmpty(processDefinitionCodes)) {
            return result;
        }
        Date start = getTimeFormStringWithException(startDate);
        Date end = getTimeFormStringWithException(endDate);
        List<Integer> statusCondition =
                stateType != null ? Lists.newArrayList(stateType.getCode()) : Collections.emptyList();
        List<Long> taskDefinitionCodes = taskDefinitionLogDao.queryTaskDefinitionCodesByProjectCodes(projectCode);
        int executorId = usersService.getUserIdByName(executorName);

        if (StringUtils.isNotBlank(processInstanceName)) {
            // due to the performance, we didn't support fuzzy query by process instance name
            Optional<ProcessInstance> processInstanceOptional =
                    processInstanceDao.queryProcessInstanceByName(processInstanceName);
            if (!processInstanceOptional.isPresent()) {
                return result;
            }
            processInstanceId = processInstanceOptional.get().getId();
        }

        ListingItem<TaskInstance> taskInstanceIPage = taskInstanceDao.queryTaskListPaging(
                new Page<>(pageNo, pageSize),
                taskDefinitionCodes,
                processInstanceId,
                processInstanceName,
                searchVal,
                taskName,
                executorId,
                statusCondition,
                host,
                start,
                end);

        List<TaskInstance> taskInstanceList = taskInstanceIPage.getItems();
        for (TaskInstance taskInstance : taskInstanceList) {
            taskInstance.setDuration(DateUtils.format2Duration(taskInstance.getStartTime(), taskInstance.getEndTime()));
            User user = usersService.queryUser(taskInstance.getExecutorId());
            if (user != null) {
                taskInstance.setExecutorName(user.getUserName());
            }
            processInstanceDao.queryProcessInstanceById(taskInstance.getProcessInstanceId())
                    .ifPresent(processInstance -> {
                        taskInstance.setProcessInstanceName(processInstance.getName());
                    });
        }
        result.setTotal((int) taskInstanceIPage.getTotalCount());
        result.setTotalList(taskInstanceList);
        return result;
    }

    /**
     * change one task instance's state from failure to forced success
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param taskInstanceId task instance id
     * @return the result code and msg
     */
    @Override
    public Map<String, Object> forceTaskSuccess(User loginUser, long projectCode, Integer taskInstanceId) {
        Project project = projectMapper.queryByCode(projectCode);
        Map<String, Object> result = new HashMap<>();
        // check user access for project
        projectService.checkProjectAndAuth(loginUser, project, projectCode, FORCED_SUCCESS);

        // check whether the task instance can be found
        TaskInstance task = taskInstanceMapper.selectById(taskInstanceId);
        if (task == null) {
            putMsg(result, Status.TASK_INSTANCE_NOT_FOUND);
            return result;
        }

        TaskDefinition taskDefinition = taskDefinitionMapper.queryByCode(task.getTaskCode());
        if (taskDefinition != null && projectCode != taskDefinition.getProjectCode()) {
            putMsg(result, Status.TASK_INSTANCE_NOT_FOUND, taskInstanceId);
            return result;
        }

        // check whether the task instance state type is failure or cancel
        if (!task.getState().typeIsFailure() && !task.getState().typeIsCancel()) {
            putMsg(result, Status.TASK_INSTANCE_STATE_OPERATION_ERROR, taskInstanceId, task.getState().toString());
            return result;
        }

        // change the state of the task instance
        task.setState(ExecutionStatus.FORCED_SUCCESS);
        int changedNum = taskInstanceMapper.updateById(task);
        if (changedNum > 0) {
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.FORCE_TASK_SUCCESS_ERROR);
        }
        return result;
    }
}
