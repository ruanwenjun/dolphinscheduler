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

package org.apache.dolphinscheduler.plugin.task.hivecli;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractRemoteTask;
import org.apache.dolphinscheduler.plugin.task.api.ShellCommandExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParamUtils;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParameterUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.dolphinscheduler.plugin.task.api.utils.FileUtils;

public class HiveCliTask extends AbstractRemoteTask {

    private HiveCliParameters hiveCliParameters;

    private final ShellCommandExecutor shellCommandExecutor;

    private final TaskExecutionContext taskExecutionContext;

    public HiveCliTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;

        this.shellCommandExecutor = new ShellCommandExecutor(this::logHandle,
                taskExecutionContext,
                logger);
    }

    @Override
    public List<String> getApplicationIds() throws TaskException {
        return Collections.emptyList();
    }

    @Override
    public void init() {
        logger.info("hiveCli task params {}", taskExecutionContext.getTaskParams());

        hiveCliParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), HiveCliParameters.class);

        if (!hiveCliParameters.checkParameters()) {
            throw new TaskException("hiveCli task params is not valid");
        }
    }

    // todo split handle to submit and track
    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        try {
            final TaskResponse taskResponse = shellCommandExecutor.run(buildCommand());
            setExitStatusCode(taskResponse.getExitStatusCode());
            setAppIds(taskResponse.getAppIds());
            setProcessId(taskResponse.getProcessId());
            hiveCliParameters.dealOutParam(shellCommandExecutor.getTaskOutputParams());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("The current HiveCLI Task has been interrupted", e);
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw new TaskException("The current HiveCLI Task has been interrupted", e);
        } catch (Exception e) {
            logger.error("hiveCli task failure", e);
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw new TaskException("run hiveCli task error", e);
        }
    }

    @Override
    public void submitApplication() throws TaskException {

    }

    @Override
    public void trackApplicationStatus() throws TaskException {

    }

    protected String buildCommand() {

        final List<String> args = new ArrayList<>();

        String fileContent = HiveSqlScriptReader.readHiveSqlContent(taskExecutionContext.getExecutePath(), hiveCliParameters);
        fileContent = ParameterUtils.convertParameterPlaceholders(fileContent, ParamUtils.convert(taskExecutionContext.getPrepareParamsMap()));
        String sqlFilePath = generateSqlScriptFile(fileContent);

        args.add(HiveCliConstants.HIVE_CLI_EXECUTE_FILE);
        args.add(sqlFilePath);

        final String hiveCliOptions = hiveCliParameters.getHiveCliOptions();
        if (StringUtils.isNotEmpty(hiveCliOptions)) {
            args.add(hiveCliOptions);
        }

        String command = String.join(" ", args);

        logger.info("hiveCli task command: {}", command);

        return command;

    }

    protected String generateSqlScriptFile(String rawScript) {
        String scriptFileName = Paths.get(taskExecutionContext.getExecutePath(), "hive_cli.sql").toString();

        try {
            File file = new File(scriptFileName);
            Path path = file.toPath();
            if (Files.exists(path)) {
                logger.warn("The HiveCli sql file: {} is already exist, will delete it", scriptFileName);
                Files.deleteIfExists(path);
            }
            if (!Files.exists(path)) {
                org.apache.dolphinscheduler.plugin.task.api.utils.FileUtils.createFileWith755(path);
                Files.write(path, rawScript.getBytes(), StandardOpenOption.APPEND);
            }
            return scriptFileName;
        } catch (Exception ex) {
            throw new TaskException("Generate sql script file: " + scriptFileName + " failed", ex);
        }
    }

    @Override
    public AbstractParameters getParameters() {
        return hiveCliParameters;
    }

    @Override
    public void cancelApplication() throws TaskException {
        try {
            shellCommandExecutor.cancelApplication();
        } catch (Exception e) {
            throw new TaskException("cancel application error", e);
        }
    }

}
