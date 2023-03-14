package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.ErrorCommand;

import java.util.List;

public interface ErrorCommandDao {

    List<ErrorCommand> findErrorCommandByOperationId(Long operationId);
}
