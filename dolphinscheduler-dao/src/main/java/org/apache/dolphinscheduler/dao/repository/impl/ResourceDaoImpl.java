package org.apache.dolphinscheduler.dao.repository.impl;

import org.apache.dolphinscheduler.dao.entity.Resource;
import org.apache.dolphinscheduler.dao.repository.ResourceDao;
import org.apache.dolphinscheduler.dao.mapper.ResourceMapper;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class ResourceDaoImpl implements ResourceDao {

    @Autowired
    private ResourceMapper resourcesMapper;

    /**
     * list all children id
     *
     * @param resource    resource
     * @param containSelf whether add self to children list
     * @return all children id
     */
    public List<Integer> listAllChildren(Resource resource, boolean containSelf) {
        List<Integer> childList = new ArrayList<>();
        if (resource.getId() != -1 && containSelf) {
            childList.add(resource.getId());
        }

        if (resource.isDirectory()) {
            listAllChildren(resource.getId(), childList);
        }
        return childList;
    }

    /**
     * list all children id
     *
     * @param resourceId resource id
     * @param childList  child list
     */
    void listAllChildren(int resourceId, List<Integer> childList) {
        List<Integer> children = resourcesMapper.listChildren(resourceId);
        for (int childId : children) {
            childList.add(childId);
            listAllChildren(childId, childList);
        }
    }

}
