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

package org.apache.dolphinscheduler.plugin.task.remoteshell;

import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.datasource.ssh.param.SSHConnectionParam;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.ResourceType;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.DataSourceParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParamUtils;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParameterUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.OSUtils;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;

/**
 * shell task
 */
public class RemoteShellTask extends AbstractTaskExecutor {

    final static String TASK_ID_PREFIX = "dolphinscheduler-remoteshell-";

    /**
     * shell parameters
     */
    private RemoteShellParameters remoteShellParameters;

    /**
     * taskExecutionContext
     */
    private TaskExecutionContext taskExecutionContext;

    private RemoteExecutor remoteExecutor;

    private String taskId;

    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    public RemoteShellTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);

        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() {
        logger.info("shell task params {}", taskExecutionContext.getTaskParams());

        remoteShellParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), RemoteShellParameters.class);

        if (!remoteShellParameters.checkParameters()) {
            throw new RuntimeException("sell task params is not valid");
        }


        taskId = taskExecutionContext.getAppIds();
        if (taskId == null) {
            taskId = TASK_ID_PREFIX + taskExecutionContext.getTaskInstanceId();
        }
        setAppIds(taskId);
        taskExecutionContext.setAppIds(taskId);

        initRemoteExecutor();
    }

    @Override
    public void handle() throws TaskException {
        try {
            // construct process
            String localFile = buildCommand();
            int exitCode = remoteExecutor.run(taskId, localFile);
            setExitStatusCode(exitCode);
            remoteShellParameters.dealOutParam(remoteExecutor.getVarPool());
        } catch (Exception e) {
            logger.error("shell task error", e);
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw new TaskException("Execute shell task error", e);
        }
    }

    @Override
    public void cancelApplication(boolean cancelApplication) throws Exception {
        logger.info("kill remote task {}", taskId);
        remoteExecutor.kill(taskId);
    }

    /**
     * create command
     *
     * @return file name
     * @throws Exception exception
     */
    private String buildCommand() throws Exception {
        // generate scripts
        String fileName = String.format("%s/%s_node.%s",
            taskExecutionContext.getExecutePath(),
            taskExecutionContext.getTaskAppId(), OSUtils.isWindows() ? "bat" : "sh");

        File file = new File(fileName);
        Path path = file.toPath();

        if (Files.exists(path)) {
            // this shouldn't happen
            logger.warn("The command file: {} is already exist", path);
            return fileName;
        }

        String script = remoteShellParameters.getRawScript().replaceAll("\\r\\n", "\n");
        script = parseScript(script);

        String environment = taskExecutionContext.getEnvironmentConfig();
        if (environment != null) {
            environment = environment.replaceAll("\\r\\n", "\n");
            script = environment + "\n" + script;
        }
        script = String.format(RemoteExecutor.COMMAND.HEADER) + script;
        script += String.format(RemoteExecutor.COMMAND.ADD_STATUS_COMMAND, RemoteExecutor.STATUS_TAG_MESSAGE);

        FileUtils.createFileWith755(path);
        Files.write(path, script.getBytes(), StandardOpenOption.APPEND);
        logger.info("raw script : {}", script);
        return fileName;
    }

    @Override
    public AbstractParameters getParameters() {
        return remoteShellParameters;
    }

    private String parseScript(String script) {
        // combining local and global parameters
        Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();
        return ParameterUtils.convertParameterPlaceholders(script, ParamUtils.convert(paramsMap));
    }


    private void initRemoteExecutor() {
        DataSourceParameters dbSource = (DataSourceParameters) taskExecutionContext.getResourceParametersHelper().getResourceParameters(ResourceType.DATASOURCE, remoteShellParameters.getDatasource());
        taskExecutionContext.getResourceParametersHelper().getResourceParameters(ResourceType.DATASOURCE, remoteShellParameters.getDatasource());
        SSHConnectionParam sshConnectionParam = (SSHConnectionParam) DataSourceUtils.buildConnectionParams(
            DbType.valueOf(remoteShellParameters.getType()),
            dbSource.getConnectionParams());
        remoteExecutor = new RemoteExecutor(sshConnectionParam);
    }
}
