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

import com.google.common.collect.Lists;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.dolphinscheduler.api.dto.CommandStateCount;
import org.apache.dolphinscheduler.api.dto.DefineUserDto;
import org.apache.dolphinscheduler.api.dto.TaskCountDto;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.DataAnalysisService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.CommandCount;
import org.apache.dolphinscheduler.dao.entity.DefinitionGroupByUser;
import org.apache.dolphinscheduler.dao.entity.ExecuteStatusCount;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.CommandMapper;
import org.apache.dolphinscheduler.dao.mapper.ErrorCommandMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessInstanceMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskInstanceMapper;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT_OVERVIEW;

/**
 * data analysis service impl
 */
@Service
public class DataAnalysisServiceImpl extends BaseServiceImpl implements DataAnalysisService {

    private static final Logger logger = LoggerFactory.getLogger(DataAnalysisServiceImpl.class);

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectDao projectDao;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private ProcessInstanceMapper processInstanceMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private CommandMapper commandMapper;

    @Autowired
    private ErrorCommandMapper errorCommandMapper;

    @Autowired
    private TaskInstanceMapper taskInstanceMapper;

    @Autowired
    private ProcessService processService;

    /**
     * statistical task instance status data
     *
     * @param loginUser   login user
     * @param projectCode project code
     * @param startDate   start date
     * @param endDate     end date
     * @return task state count data
     */
    @Override
    public TaskCountDto countTaskStateByProject(@NonNull User loginUser,
                                                long projectCode,
                                                String startDate,
                                                String endDate) {
        List<Long> authorityProjectCodes;
        if (projectCode != 0) {
            Project project = projectMapper.queryByCode(projectCode);
            projectService.checkProjectAndAuth(loginUser, project, projectCode, PROJECT_OVERVIEW);
            authorityProjectCodes = Lists.newArrayList(projectCode);
        } else {
            authorityProjectCodes = getAuthorityProjectCodeByUser(loginUser);
        }
        if (CollectionUtils.isEmpty(authorityProjectCodes)) {
            return TaskCountDto.empty();
        }

        Date start = getTimeFormStringWithException(startDate);
        Date end = getTimeFormStringWithException(endDate);

        List<ExecuteStatusCount> processInstanceStateCounts =
                countTaskInstanceAllStatesByProjectCodes(start, end, authorityProjectCodes);
        return new TaskCountDto(processInstanceStateCounts);
    }

    /**
     * statistical process instance status data
     *
     * @param loginUser   login user
     * @param projectCode project code
     * @param startDate   start date
     * @param endDate     end date
     * @return process instance state count data
     */
    @Override
    public TaskCountDto countProcessInstanceStateByProject(@NonNull User loginUser,
                                                           long projectCode,
                                                           String startDate,
                                                           String endDate) {
        List<Long> authorityProjectCodes;
        if (projectCode != 0) {
            Project project = projectMapper.queryByCode(projectCode);
            projectService.checkProjectAndAuth(loginUser, project, projectCode, PROJECT_OVERVIEW);
            authorityProjectCodes = Lists.newArrayList(projectCode);
        } else {
            authorityProjectCodes = getAuthorityProjectCodeByUser(loginUser);
        }
        if (CollectionUtils.isEmpty(authorityProjectCodes)) {
            return TaskCountDto.empty();
        }

        Date start = getTimeFormStringWithException(startDate);
        Date end = getTimeFormStringWithException(endDate);

        List<ExecuteStatusCount> processInstanceStateCounts = processInstanceMapper
                .countInstanceStateByProjectCodes(start, end, authorityProjectCodes.toArray(new Long[0]));
        return new TaskCountDto(processInstanceStateCounts);
    }

    /**
     * statistics the process definition quantities of a certain person
     * <p>
     * We only need projects which users have permission to see to determine whether the definition belongs to the user or not.
     *
     * @param loginUser   login user
     * @param projectCode project code
     * @return definition count data
     */
    @Override
    public Map<String, Object> countDefinitionByUser(User loginUser, long projectCode) {
        Map<String, Object> result = new HashMap<>();
        if (projectCode != 0) {
            Project project = projectMapper.queryByCode(projectCode);
            projectService.checkProjectAndAuth(loginUser, project, projectCode, PROJECT_OVERVIEW);
        }

        List<DefinitionGroupByUser> defineGroupByUsers = new ArrayList<>();
        Pair<Set<Integer>, Map<String, Object>> projectIds = getProjectIds(loginUser, result);
        if (projectIds.getRight() != null) {
            List<DefinitionGroupByUser> emptyList = new ArrayList<>();
            DefineUserDto dto = new DefineUserDto(emptyList);
            result.put(Constants.DATA_LIST, dto);
            putMsg(result, Status.SUCCESS);
            return result;
        }
        Long[] projectCodeArray =
                projectCode == 0 ? getProjectCodesArrays(projectIds.getLeft()) : new Long[]{projectCode};
        if (projectCodeArray.length != 0 || loginUser.getUserType() == UserType.ADMIN_USER) {
            defineGroupByUsers = processDefinitionMapper.countDefinitionByProjectCodes(projectCodeArray);
        }

        DefineUserDto dto = new DefineUserDto(defineGroupByUsers);
        result.put(Constants.DATA_LIST, dto);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * statistical command status data
     *
     * @param loginUser login user
     * @return command state count data
     */
    @Override
    public Map<String, Object> countCommandState(User loginUser) {
        Map<String, Object> result = new HashMap<>();

        /**
         * find all the task lists in the project under the user
         * statistics based on task status execution, failure, completion, wait, total
         */
        Date start = null;
        Date end = null;
        Pair<Set<Integer>, Map<String, Object>> projectIds = getProjectIds(loginUser, result);
        if (projectIds.getRight() != null) {
            List<CommandStateCount> noData = Arrays.stream(CommandType.values())
                    .map(commandType -> new CommandStateCount(0, 0, commandType)).collect(Collectors.toList());
            result.put(Constants.DATA_LIST, noData);
            putMsg(result, Status.SUCCESS);
            return result;
        }
        Long[] projectCodeArray = getProjectCodesArrays(projectIds.getLeft());
        // count normal command state
        Map<CommandType, Integer> normalCountCommandCounts =
                commandMapper.countCommandState(start, end, projectCodeArray)
                        .stream()
                        .collect(Collectors.toMap(CommandCount::getCommandType, CommandCount::getCount));

        // count error command state
        Map<CommandType, Integer> errorCommandCounts =
                errorCommandMapper.countCommandState(start, end, projectCodeArray)
                        .stream()
                        .collect(Collectors.toMap(CommandCount::getCommandType, CommandCount::getCount));

        List<CommandStateCount> list = Arrays.stream(CommandType.values())
                .map(commandType -> new CommandStateCount(
                        errorCommandCounts.getOrDefault(commandType, 0),
                        normalCountCommandCounts.getOrDefault(commandType, 0),
                        commandType))
                .collect(Collectors.toList());

        result.put(Constants.DATA_LIST, list);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    private Pair<Set<Integer>, Map<String, Object>> getProjectIds(User loginUser, Map<String, Object> result) {
        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), logger);
        if (projectIds.isEmpty()) {
            List<ExecuteStatusCount> taskInstanceStateCounts = new ArrayList<>();
            result.put(Constants.DATA_LIST, new TaskCountDto(taskInstanceStateCounts));
            putMsg(result, Status.SUCCESS);
            return Pair.of(null, result);
        }
        return Pair.of(projectIds, null);
    }

    private Long[] getProjectCodesArrays(Set<Integer> projectIds) {
        List<Project> projects = projectMapper.selectBatchIds(projectIds);
        List<Long> codeList = projects.stream().map(Project::getCode).collect(Collectors.toList());
        Long[] projectCodeArray = new Long[codeList.size()];
        codeList.toArray(projectCodeArray);
        return projectCodeArray;
    }

    /**
     * count queue state
     *
     * @return queue state count data
     */
    @Override
    public Map<String, Object> countQueueState(User loginUser) {
        Map<String, Object> result = new HashMap<>();

        // TODO need to add detail data info
        Map<String, Integer> dataMap = new HashMap<>();
        dataMap.put("taskQueue", 0);
        dataMap.put("taskKill", 0);
        result.put(Constants.DATA_LIST, dataMap);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public List<ExecuteStatusCount> countTaskInstanceAllStatesByProjectCodes(Date startTime,
                                                                             Date endTime,
                                                                             List<Long> projectCodes) {
        return taskInstanceMapper.countTaskInstanceStateByProjectCodes(startTime, endTime, projectCodes);
    }

    private List<Long> getAuthorityProjectCodeByUser(@NonNull User loginUser) {
        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), logger);
        return projectDao.queryProjectCodeByIds(projectIds);
    }

}
