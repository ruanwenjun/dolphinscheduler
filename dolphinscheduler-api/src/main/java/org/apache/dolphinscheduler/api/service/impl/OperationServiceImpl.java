package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.OperationService;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.dao.repository.CommandDao;
import org.apache.dolphinscheduler.dao.repository.ErrorCommandDao;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;

import org.apache.commons.collections4.CollectionUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OperationServiceImpl implements OperationService {

    @Autowired
    private CommandDao commandDao;

    @Autowired
    private ErrorCommandDao errorCommandDao;

    @Autowired
    private ProcessInstanceDao processInstanceDao;

    @Override
    public Long checkOrCreateOperationId(Long operationId) {
        if (operationId == null) {
            return CodeGenerateUtils.getInstance().genCode();
        }
        // todo: check if the operation has been executed
        // check the command
        if (checkCommandExist(operationId)) {
            throw new ServiceException(Status.OPERATION_ALREADY_EXIST);
        }
        if (checkErrorCommandExist(operationId)) {
            throw new ServiceException(Status.OPERATION_ALREADY_EXIST);
        }
        if (checkProcessInstanceExist(operationId)) {
            throw new ServiceException(Status.OPERATION_ALREADY_EXIST);
        }
        return operationId;
    }

    private boolean checkProcessInstanceExist(Long operationId) {
        return CollectionUtils.isNotEmpty(processInstanceDao.queryProcessInstanceByOperationId(operationId));
    }

    private boolean checkCommandExist(Long operationId) {
        return CollectionUtils.isNotEmpty(commandDao.findCommandByOperationId(operationId));
    }

    private boolean checkErrorCommandExist(Long operationId) {
        return CollectionUtils.isNotEmpty(errorCommandDao.findErrorCommandByOperationId(operationId));
    }

}
