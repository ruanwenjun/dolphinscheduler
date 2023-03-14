package org.apache.dolphinscheduler.api.service;

public interface OperationService {

    /**
     * Check if the operation has been executed.
     * If the operation has been executed, throw an exception.
     * If the operation has not been executed, return the operation id.
     * If the operation id is null, generate a new operation id.
     *
     * @param operationId given operation id.
     * @return
     */
    Long checkOrCreateOperationId(Long operationId);
}
