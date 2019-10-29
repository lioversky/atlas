package org.apache.atlas.discovery;

import org.apache.atlas.SortOrder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;

/**
 * Create by hongxun on 2019/10/29
 */
public interface AtlasQualityService {

  AtlasEntitiesWithExtInfo searchProcessQualities(String guid, String relation, String sortByAttribute, SortOrder sortOrder, boolean excludeDeletedEntities, int limit, int offset) throws AtlasBaseException;

}
