package org.apache.dolphinscheduler.plugin.task.api;

import java.io.IOException;

@FunctionalInterface
public interface SupplierWithIO<T> {

    T get() throws IOException;
}
