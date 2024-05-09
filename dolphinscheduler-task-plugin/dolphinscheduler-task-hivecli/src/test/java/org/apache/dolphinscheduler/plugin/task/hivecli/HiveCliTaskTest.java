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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveCliTaskTest {

    @Test
    public void hiveCliTaskExecuteSqlFromScript() throws Exception {
        String hiveCliTaskParameters = buildHiveCliTaskExecuteSqlFromScriptParameters();
        HiveCliTask hiveCliTask = prepareHiveCliTaskForTest(hiveCliTaskParameters);
        hiveCliTask.init();
        Assert.assertEquals("hive -f /tmp/hive_cli.sql", hiveCliTask.buildCommand());
    }

    @Test
    public void hiveCliTaskExecuteWithOptions() {
        String hiveCliTaskParameters = buildHiveCliTaskExecuteWithOptionsParameters();
        HiveCliTask hiveCliTask = prepareHiveCliTaskForTest(hiveCliTaskParameters);
        hiveCliTask.init();
        Assert.assertEquals("hive -f /tmp/hive_cli.sql --verbose", hiveCliTask.buildCommand());
    }

    private HiveCliTask prepareHiveCliTaskForTest(final String hiveCliTaskParameters) {
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        when(taskExecutionContext.getTaskParams()).thenReturn(hiveCliTaskParameters);
        when(taskExecutionContext.getExecutePath()).thenReturn("/tmp");
        HiveCliTask hiveCliTask = spy(new HiveCliTask(taskExecutionContext));
        return hiveCliTask;
    }

    private String buildHiveCliTaskExecuteSqlFromScriptParameters() {
        final HiveCliParameters hiveCliParameters = new HiveCliParameters();
        hiveCliParameters.setHiveCliTaskExecutionType("SCRIPT");
        hiveCliParameters.setHiveSqlScript("SHOW DATABASES;");
        return JSONUtils.toJsonString(hiveCliParameters);
    }

    private String buildHiveCliTaskExecuteWithOptionsParameters() {
        final HiveCliParameters hiveCliParameters = new HiveCliParameters();
        hiveCliParameters.setHiveCliTaskExecutionType("SCRIPT");
        hiveCliParameters.setHiveSqlScript("SHOW DATABASES;");
        hiveCliParameters.setHiveCliOptions("--verbose");
        return JSONUtils.toJsonString(hiveCliParameters);
    }

}
