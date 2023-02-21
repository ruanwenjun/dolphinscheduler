package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.Resource;

import java.util.List;

public interface ResourceDao {

    /**
     * list all children id
     *
     * @param resource    resource
     * @param containSelf whether add self to children list
     * @return all children id
     */
    List<Integer> listAllChildren(Resource resource, boolean containSelf);
}
