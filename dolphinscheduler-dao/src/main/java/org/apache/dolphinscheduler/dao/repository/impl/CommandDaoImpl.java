package org.apache.dolphinscheduler.dao.repository.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.mapper.CommandMapper;
import org.apache.dolphinscheduler.dao.repository.CommandDao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class CommandDaoImpl implements CommandDao {

    @Autowired
    private CommandMapper commandMapper;

    @Override
    public List<Command> findCommandByOperationId(Long operationId) {
        checkNotNull(operationId, "operationId cannot be null");
        return commandMapper.selectByOperationId(operationId);
    }

}
