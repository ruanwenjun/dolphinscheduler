package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.Command;

import java.util.List;

public interface CommandDao {

    List<Command> findCommandByOperationId(Long operationId);

}
