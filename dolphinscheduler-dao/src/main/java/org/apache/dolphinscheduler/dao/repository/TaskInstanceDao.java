package org.apache.dolphinscheduler.dao.repository;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.NonNull;
import org.apache.dolphinscheduler.dao.dto.ListingItem;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.exception.RepositoryException;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

import java.util.Date;
import java.util.List;

public interface TaskInstanceDao {

    List<TaskInstance> queryTaskInstanceByIds(List<Integer> taskInstanceId);

    void deleteTaskInstanceByWorkflowInstanceId(int workflowInstanceId);

    /**
     * Update the taskInstance, if update failed will throw exception.
     *
     * @param taskInstance need to update
     */
    void updateTaskInstance(@NonNull TaskInstance taskInstance) throws RepositoryException;

    /**
     * Update the taskInstance, if update success will return true, else return true.
     * <p>
     * This method will never throw exception.
     *
     * @param taskInstance need to update
     */
    boolean updateTaskInstanceSafely(@NonNull TaskInstance taskInstance);

    ListingItem<TaskInstance> queryTaskListPaging(Page<TaskInstance> page,
                                                  List<Long> taskDefinitionCodes,
                                                  Integer processInstanceId,
                                                  String searchVal,
                                                  String taskName,
                                                  int executorId,
                                                  List<Integer> statusCondition,
                                                  String host,
                                                  Date start,
                                                  Date end);

    List<TaskInstance> queryByWorkflowInstanceId(Integer workflowInstanceId);
}
