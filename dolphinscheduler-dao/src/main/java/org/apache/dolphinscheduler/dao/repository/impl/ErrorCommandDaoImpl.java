package org.apache.dolphinscheduler.dao.repository.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.dao.entity.ErrorCommand;
import org.apache.dolphinscheduler.dao.mapper.ErrorCommandMapper;
import org.apache.dolphinscheduler.dao.repository.ErrorCommandDao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class ErrorCommandDaoImpl implements ErrorCommandDao {

    @Autowired
    private ErrorCommandMapper errorCommandMapper;

    @Override
    public List<ErrorCommand> findErrorCommandByOperationId(Long operationId) {
        checkNotNull(operationId);
        return errorCommandMapper.selectErrorCommandByOperationId(operationId);
    }
}
