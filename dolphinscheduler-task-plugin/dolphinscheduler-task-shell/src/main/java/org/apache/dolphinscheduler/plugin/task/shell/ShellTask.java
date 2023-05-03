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

package org.apache.dolphinscheduler.plugin.task.shell;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecuteResult;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecutor;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecutorFactory;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellTask extends AbstractTask {

    protected final Logger logger =
        LoggerFactory.getLogger(String.format(TaskConstants.TASK_LOG_LOGGER_NAME_FORMAT, getClass()));

    private ShellParameters shellParameters;

    private ShellExecutor shellExecutor;

    public ShellTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
    }

    public void init() {
        shellParameters = JSONUtils.parseObject(taskRequest.getTaskParams(), ShellParameters.class);
        logger.info("Success initialize shell task params: {}", JSONUtils.toPrettyJsonString(shellParameters));
        if (shellParameters == null || !shellParameters.checkParameters()) {
            throw new RuntimeException("shell task params is not valid");
        }
        this.shellExecutor = ShellExecutorFactory.createShellExecutor(shellParameters, taskRequest);
    }

    @Override
    public void handle() throws TaskException {
        try {
            ShellExecuteResult shellExecuteResult = shellExecutor.execute();
            setResult(shellExecuteResult);
        } catch (Exception e) {
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw new TaskException("Execute shell task error", e);
        }
    }

    @Override
    public void cancelApplication(boolean cancelApplication) throws TaskException {
        try {
            if (cancelApplication && shellExecutor != null) {
                shellExecutor.cancelTask();
            }
        } catch (Exception e) {
            throw new TaskException("cancel application error", e);
        }
    }

    @Override
    public AbstractParameters getParameters() {
        return shellParameters;
    }

    private void setResult(ShellExecuteResult shellExecuteResult) {
        setExitStatusCode(shellExecuteResult.getExitStatusCode());
        setAppIds(shellExecuteResult.getAppIds());
        setProcessId(shellExecuteResult.getProcessId());
        shellParameters.dealOutParam(shellExecuteResult.getVarPool());
    }
}
