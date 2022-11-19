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

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.permission.ResourcePermissionCheckService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.api.vo.project.ProjectListingVO;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils.CodeGenerateException;
import org.apache.dolphinscheduler.dao.dto.ListingItem;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectUserMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT_CREATE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT_DELETE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT_UPDATE;

@Service
public class ProjectServiceImpl extends BaseServiceImpl implements ProjectService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectServiceImpl.class);

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectDao projectDao;

    @Autowired
    private ProjectUserMapper projectUserMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private ResourcePermissionCheckService resourcePermissionCheckService;

    /**
     * create project
     *
     * @param loginUser login user
     * @param name project name
     * @param desc description
     * @return returns an error if it exists
     */
    @Override
    @Transactional
    public Map<String, Object> createProject(User loginUser, String name, String desc) {
        Map<String, Object> result = new HashMap<>();

        if (checkLengthIllegal(name, Constants.NAME_LENGTH_GO_ONLINE)) {
            putMsg(result, Status.PROJECT_NAME_TOO_LONG_ERROR);
            return result;
        }

        if (checkLengthIllegal(desc, Constants.DESC_LENGTH_GO_ONLINE)) {
            putMsg(result, Status.DESCRIPTION_TOO_LONG_ERROR);
            return result;
        }
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.PROJECTS, PROJECT_CREATE)) {
            putMsg(result, Status.USER_NO_OPERATION_PERM);
            return result;
        }
        Project project = projectMapper.queryByName(name);
        if (project != null) {
            putMsg(result, Status.PROJECT_ALREADY_EXISTS, name);
            return result;
        }

        Date now = new Date();

        try {
            project = Project.builder()
                    .name(name)
                    .code(CodeGenerateUtils.getInstance().genCode())
                    .description(desc)
                    .userId(loginUser.getId())
                    .userName(loginUser.getUserName())
                    .createTime(now)
                    .updateTime(now)
                    .build();
        } catch (CodeGenerateException e) {
            putMsg(result, Status.CREATE_PROJECT_ERROR);
            return result;
        }

        if (projectMapper.insert(project) > 0) {
            result.put(Constants.DATA_LIST, project);
            putMsg(result, Status.SUCCESS);
            permissionPostHandle(AuthorizationType.PROJECTS, loginUser.getId(),
                    Collections.singletonList(project.getId()), logger);
        } else {
            putMsg(result, Status.CREATE_PROJECT_ERROR);
        }
        logger.info("create project complete and id is :{}", project.getId());
        return result;
    }

    /**
     * check project description
     *
     * @param result
     * @param desc   desc
     */
    public static void checkDesc(Result result, String desc) {
        if (!StringUtils.isEmpty(desc) && desc.codePointCount(0, desc.length()) > 255) {
            result.setCode(Status.REQUEST_PARAMS_NOT_VALID_ERROR.getCode());
            result.setMsg(MessageFormat.format(Status.REQUEST_PARAMS_NOT_VALID_ERROR.getMsg(), "desc length"));
        } else {
            result.setCode(Status.SUCCESS.getCode());
        }
    }

    /**
     * query project details by code
     *
     * @param projectCode project code
     * @return project detail information
     */
    @Override
    public Map<String, Object> queryByCode(User loginUser, long projectCode) {
        Map<String, Object> result = new HashMap<>();
        Project project = projectMapper.queryByCode(projectCode);
        hasProjectAndPerm(loginUser, project, result, PROJECT);
        if (project != null) {
            result.put(Constants.DATA_LIST, project);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    @Override
    public Map<String, Object> queryByName(User loginUser, String projectName) {
        Map<String, Object> result = new HashMap<>();
        Project project = projectMapper.queryByName(projectName);
        hasProjectAndPerm(loginUser, project, result, PROJECT);

        if (project != null) {
            result.put(Constants.DATA_LIST, project);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * check project and authorization
     *
     * @param loginUser   login user
     * @param project     project
     * @param projectCode project code
     * @return true if the login user have permission to see the project
     */
    @Override
    public void checkProjectAndAuth(User loginUser, Project project, long projectCode, String perm) {
        if (project == null) {
            throw new ServiceException(Status.PROJECT_NOT_EXIST);
        } else if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS,
                perm)) {
            // check read permission
            Project checkProject = projectMapper.queryByCode(projectCode);
            throw new ServiceException(Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(),
                    Objects.nonNull(checkProject) ? project.getName() : projectCode);
        }
    }

    @Override
    public void hasProjectAndPerm(User loginUser, Project project, Map<String, Object> result, String perm) {
        if (project == null) {
            throw new ServiceException(Status.PROJECT_NOT_FOUND, "");
        } else if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS,
                perm)) {
            throw new ServiceException(Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(),
                    project.getName());
        }
    }

    @Override
    public boolean hasProjectAndPerm(User loginUser, Project project, Result result) {
        boolean checkResult = false;
        if (project == null) {
            putMsg(result, Status.PROJECT_NOT_FOUND, "");
        } else if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS,
                PROJECT)) {
            putMsg(result, Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(), project.getName());
        } else {
            checkResult = true;
        }
        return checkResult;
    }

    /**
     * admin can view all projects
     *
     * @param loginUser login user
     * @param searchVal search value
     * @param pageSize page size
     * @param pageNo page number
     * @return project list which the login user have permission to see
     */
    @Override
    public PageInfo<ProjectListingVO> queryProjectListPaging(User loginUser, Integer pageSize, Integer pageNo,
                                                             String searchVal) {
        PageInfo<ProjectListingVO> pageInfo = new PageInfo<>(pageNo, pageSize);
        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), logger);
        if (projectIds.isEmpty()) {
            return pageInfo;
        }

        ListingItem<Project> projectListingItem = projectDao.listingProjects(pageSize, pageNo, projectIds, searchVal);
        List<ProjectListingVO> projectListingVOList = projectListingItem.getItems()
                .stream()
                .map(project -> {
                    ProjectListingVO projectListingVO = new ProjectListingVO(project);
                    User user = userMapper.selectById(project.getUserId());
                    projectListingVO.setUserName(user == null ? null : user.getUserName());
                    // todo: if we add project code in process instance then we can directly query by projectCode
                    if (loginUser.getUserType() != UserType.ADMIN_USER) {
                        projectListingVO.setPerm(Constants.DEFAULT_ADMIN_PERMISSION);
                    }
                    return projectListingVO;
                })
                .collect(Collectors.toList());

        // todo: change to long
        pageInfo.setTotal((int) projectListingItem.getTotalCount());
        pageInfo.setTotalList(projectListingVOList);
        return pageInfo;
    }

    /**
     * delete project by code
     *
     * @param loginUser login user
     * @param projectCode project code
     * @return delete result code
     */
    @Override
    public Map<String, Object> deleteProject(User loginUser, Long projectCode) {
        Map<String, Object> result = new HashMap<>();
        Project project = projectMapper.queryByCode(projectCode);

        getCheckResult(loginUser, project, PROJECT_DELETE);

        List<ProcessDefinition> processDefinitionList =
                processDefinitionMapper.queryAllDefinitionList(project.getCode());

        if (!processDefinitionList.isEmpty()) {
            putMsg(result, Status.DELETE_PROJECT_ERROR_DEFINES_NOT_NULL);
            return result;
        }
        int delete = projectMapper.deleteById(project.getId());
        if (delete > 0) {
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.DELETE_PROJECT_ERROR);
        }
        return result;
    }

    /**
     * get check result
     *
     * @param loginUser login user
     * @param project project
     * @return check result
     */
    private void getCheckResult(User loginUser, Project project, String perm) {
        checkProjectAndAuth(loginUser, project, project == null ? 0L : project.getCode(), perm);
    }

    /**
     * updateProcessInstance project
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param projectName project name
     * @param desc description
     * @param userName project owner
     * @return update result code
     */
    @Override
    public Map<String, Object> update(User loginUser, Long projectCode, String projectName, String desc,
                                      String userName) {
        Map<String, Object> result = new HashMap<>();

        if (checkLengthIllegal(projectName, Constants.NAME_LENGTH_GO_ONLINE)) {
            putMsg(result, Status.PROJECT_NAME_TOO_LONG_ERROR);
            return result;
        }

        if (checkLengthIllegal(desc, Constants.DESC_LENGTH_GO_ONLINE)) {
            putMsg(result, Status.DESCRIPTION_TOO_LONG_ERROR);
            return result;
        }

        Project project = projectMapper.queryByCode(projectCode);
        hasProjectAndPerm(loginUser, project, result, PROJECT_UPDATE);

        Project tempProject = projectMapper.queryByName(projectName);
        if (tempProject != null && tempProject.getCode() != project.getCode()) {
            putMsg(result, Status.PROJECT_ALREADY_EXISTS, projectName);
            return result;
        }
        User user = userMapper.queryByUserNameAccurately(userName);
        if (user == null) {
            putMsg(result, Status.USER_NOT_EXIST, userName);
            return result;
        }
        project.setName(projectName);
        project.setDescription(desc);
        project.setUpdateTime(new Date());
        project.setUserId(user.getId());
        int update = projectMapper.updateById(project);
        if (update > 0) {
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.UPDATE_PROJECT_ERROR);
        }
        return result;
    }

    /**
     * query unauthorized project
     *
     * @param loginUser login user
     * @param userId user id
     * @return the projects which user have not permission to see
     */
    @Override
    public Map<String, Object> queryUnauthorizedProject(User loginUser, Integer userId) {
        Map<String, Object> result = new HashMap<>();

        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), logger);
        if (projectIds.isEmpty()) {
            result.put(Constants.DATA_LIST, Collections.emptyList());
            putMsg(result, Status.SUCCESS);
            return result;
        }
        List<Project> projectList = projectMapper.listAuthorizedProjects(
                loginUser.getUserType().equals(UserType.ADMIN_USER) ? 0 : loginUser.getId(),
                new ArrayList<>(projectIds));

        List<Project> resultList = new ArrayList<>();
        Set<Project> projectSet;
        if (projectList != null && !projectList.isEmpty()) {
            projectSet = new HashSet<>(projectList);

            List<Project> authedProjectList = projectMapper.queryAuthedProjectListByUserId(userId);

            resultList = getUnauthorizedProjects(projectSet, authedProjectList);
        }
        result.put(Constants.DATA_LIST, resultList);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * get unauthorized project
     *
     * @param projectSet project set
     * @param authedProjectList authed project list
     * @return project list that authorization
     */
    private List<Project> getUnauthorizedProjects(Set<Project> projectSet, List<Project> authedProjectList) {
        List<Project> resultList;
        Set<Project> authedProjectSet;
        if (authedProjectList != null && !authedProjectList.isEmpty()) {
            authedProjectSet = new HashSet<>(authedProjectList);
            projectSet.removeAll(authedProjectSet);

        }
        resultList = new ArrayList<>(projectSet);
        return resultList;
    }

    /**
     * query authorized project
     *
     * @param loginUser login user
     * @param userId user id
     * @return projects which the user have permission to see, Except for items created by this user
     */
    @Override
    public Map<String, Object> queryAuthorizedProject(User loginUser, Integer userId) {
        Map<String, Object> result = new HashMap<>();

        List<Project> projects = projectMapper.queryAuthedProjectListByUserId(userId);
        result.put(Constants.DATA_LIST, projects);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    /**
     * query authorized user
     *
     * @param loginUser     login user
     * @param projectCode   project code
     * @return users        who have permission for the specified project
     */
    @Override
    public Map<String, Object> queryAuthorizedUser(User loginUser, Long projectCode) {
        Map<String, Object> result = new HashMap<>();

        // 1. check read permission
        Project project = this.projectMapper.queryByCode(projectCode);
        hasProjectAndPerm(loginUser, project, result, PROJECT);

        // 2. query authorized user list
        List<User> users = this.userMapper.queryAuthedUserListByProjectId(project.getId());
        result.put(Constants.DATA_LIST, users);
        this.putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query authorized project
     *
     * @param loginUser login user
     * @return projects which the user have permission to see, Except for items created by this user
     */
    @Override
    public Map<String, Object> queryProjectCreatedByUser(User loginUser) {
        Map<String, Object> result = new HashMap<>();

        List<Project> projects = projectMapper.queryProjectCreatedByUser(loginUser.getId());
        result.put(Constants.DATA_LIST, projects);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    /**
     * query authorized and user create project list by user
     *
     * @param loginUser login user
     * @return project list
     */
    @Override
    public Map<String, Object> queryProjectCreatedAndAuthorizedByUser(User loginUser) {
        Map<String, Object> result = new HashMap<>();

        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), logger);
        if (projectIds.isEmpty()) {
            result.put(Constants.DATA_LIST, Collections.emptyList());
            putMsg(result, Status.SUCCESS);
            return result;
        }
        List<Project> projects = projectMapper.selectBatchIds(projectIds);

        result.put(Constants.DATA_LIST, projects);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    /**
     * check whether have read permission
     *
     * @param user user
     * @param project project
     * @return true if the user have permission to see the project, otherwise return false
     */
    private boolean checkReadPermission(User user, Project project) {
        int permissionId = queryPermission(user, project);
        return (permissionId & Constants.READ_PERMISSION) != 0;
    }

    /**
     * query permission id
     *
     * @param user user
     * @param project project
     * @return permission
     */
    private int queryPermission(User user, Project project) {
        if (user.getUserType() == UserType.ADMIN_USER) {
            return Constants.READ_PERMISSION;
        }

        if (project.getUserId() == user.getId()) {
            return Constants.ALL_PERMISSIONS;
        }

        ProjectUser projectUser = projectUserMapper.queryProjectRelation(project.getId(), user.getId());

        if (projectUser == null) {
            return 0;
        }

        return projectUser.getPerm();

    }

    /**
     * query all project list
     * @param user
     * @return project list
     */
    @Override
    public Map<String, Object> queryAllProjectList(User user) {
        Map<String, Object> result = new HashMap<>();
        List<Project> projects;
        if (user.getUserType().equals(UserType.ADMIN_USER)) {
            projects = projectMapper.queryAllProject(user.getUserType() == UserType.ADMIN_USER ? 0 : user.getId());
        } else {
            Set<Integer> projectIds = resourcePermissionCheckService
                    .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, user.getId(), logger);
            if (projectIds.isEmpty()) {
                result.put(Constants.DATA_LIST, Collections.emptyList());
                putMsg(result, Status.SUCCESS);
                return result;
            }
            projects = projectMapper.selectBatchIds(projectIds);
        }

        result.put(Constants.DATA_LIST, projects);
        putMsg(result, Status.SUCCESS);
        return result;
    }

}
