package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.TaskMainInfo;

import java.util.List;

public interface TaskDefinitionDao {

    public List<TaskMainInfo> queryByDataSourceId(int dataSourceId);
}
