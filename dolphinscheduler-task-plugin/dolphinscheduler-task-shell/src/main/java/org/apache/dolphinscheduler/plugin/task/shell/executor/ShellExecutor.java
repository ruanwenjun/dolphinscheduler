package org.apache.dolphinscheduler.plugin.task.shell.executor;

public interface ShellExecutor {

    ShellExecuteResult execute();

    String getVarPool();

    void cancelTask();
}
