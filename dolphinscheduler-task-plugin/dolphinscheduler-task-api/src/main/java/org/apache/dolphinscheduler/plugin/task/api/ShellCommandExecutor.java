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

package org.apache.dolphinscheduler.plugin.task.api;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.OSUtils;
import org.apache.dolphinscheduler.spi.utils.ShellUtils;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

@Slf4j
public class ShellCommandExecutor extends AbstractCommandExecutor {

    /**
     * For Unix-like, using sh
     */
    private static final String SH = "sh";

    /**
     * For Windows, using cmd.exe
     */
    private static final String CMD = "cmd.exe";

    /**
     * constructor
     *
     * @param logHandler logHandler
     * @param taskRequest taskRequest
     * @param logger logger
     */
    public ShellCommandExecutor(Consumer<LinkedBlockingQueue<String>> logHandler,
                                TaskExecutionContext taskRequest,
                                Logger logger) {
        super(logHandler, taskRequest, logger);
    }

    public ShellCommandExecutor(LinkedBlockingQueue<String> logBuffer) {
        super(logBuffer);
    }

    @Override
    protected String buildCommandFilePath() {
        // command file
        return String.format("%s/%s.%s", taskRequest.getExecutePath(), taskRequest.getTaskAppId(),
            OSUtils.isWindows() ? "bat" : "command");
    }

    /**
     * create command file if not exists
     *
     * @param shellScriptFilePath exec command
     * @param commandFile         command file
     * @throws IOException io exception
     */
    @Override
    protected void createCommandFileIfNotExists(String shellScriptFilePath, String commandFile) throws IOException {
        // create if non existence
        log.info("Begin to create command file: {}", commandFile);

        Path commandFilePath = Paths.get(commandFile);
        if (Files.exists(commandFilePath)) {
            log.warn("The command file: {} is already exist, will not create a again", commandFile);
            return;
        }

        StringBuilder sb = new StringBuilder();
        if (OSUtils.isWindows()) {
            sb.append("@echo off\n");
            sb.append("cd /d %~dp0\n");
            if (CollectionUtils.isNotEmpty(ShellUtils.ENV_SOURCE_LIST)) {
                for (String envSourceFile : ShellUtils.ENV_SOURCE_LIST) {
                    sb.append("call ").append(envSourceFile).append("\n");
                }
            }
            if (StringUtils.isNotBlank(taskRequest.getEnvironmentConfig())) {
                sb.append(taskRequest.getEnvironmentConfig()).append("\n");
            }
        } else {
            sb.append("#!/bin/sh\n");
            sb.append("BASEDIR=$(cd `dirname $0`; pwd)\n");
            sb.append("cd $BASEDIR\n");
            if (CollectionUtils.isNotEmpty(ShellUtils.ENV_SOURCE_LIST)) {
                for (String envSourceFile : ShellUtils.ENV_SOURCE_LIST) {
                    sb.append("source ").append(envSourceFile).append("\n");
                }
            }
            if (StringUtils.isNotBlank(taskRequest.getEnvironmentConfig())) {
                sb.append(taskRequest.getEnvironmentConfig()).append("\n");
            }
        }
        sb.append(shellScriptFilePath);
        String commandContent = sb.toString();

        FileUtils.createFileWith755(commandFilePath);
        Files.write(commandFilePath, commandContent.getBytes(), StandardOpenOption.APPEND);

        log.info("Success create command file:\n {}", commandContent);
    }

    @Override
    protected String commandInterpreter() {
        return OSUtils.isWindows() ? CMD : SH;
    }

}
