package org.apache.dolphinscheduler.dao.repository.impl;

import org.apache.dolphinscheduler.dao.entity.TaskMainInfo;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionDao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class TaskDefinitionDaoImpl implements TaskDefinitionDao {

    @Autowired
    private TaskDefinitionMapper taskDefinitionMapper;

    @Override
    public List<TaskMainInfo> queryByDataSourceId(int dataSourceId) {
        return taskDefinitionMapper.queryByDataSourceId(dataSourceId);
    }
}
