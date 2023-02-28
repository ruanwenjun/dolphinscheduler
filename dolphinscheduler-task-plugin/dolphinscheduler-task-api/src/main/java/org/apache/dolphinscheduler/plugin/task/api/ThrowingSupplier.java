package org.apache.dolphinscheduler.plugin.task.api;

@FunctionalInterface
public interface ThrowingSupplier<T> {

    T get() throws Exception;
}
