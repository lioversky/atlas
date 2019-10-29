package org.apache.atlas.discovery;

import javax.inject.Inject;
import org.apache.atlas.SortOrder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;

/**
 * Create by hongxun on 2019/10/29
 */
public class EntityQualityService implements AtlasQualityService {


  private final AtlasEntityStore entitiesStore;
  private final AtlasDiscoveryService discoveryService;

  @Inject
  public EntityQualityService(AtlasEntityStore entitiesStore,
      AtlasDiscoveryService discoveryService) {
    this.entitiesStore = entitiesStore;
    this.discoveryService = discoveryService;
  }

  @Override
  public AtlasEntitiesWithExtInfo searchProcessQualities(String guid, String relation,
      String sortByAttribute, SortOrder sortOrder, boolean excludeDeletedEntities, int limit,
      int offset) throws AtlasBaseException {
    AtlasSearchResult atlasSearchResult = discoveryService
        .searchRelatedEntities(guid, relation, sortByAttribute, sortOrder, excludeDeletedEntities,
            limit, offset);
    AtlasEntitiesWithExtInfo ret = new AtlasEntitiesWithExtInfo();
    for (AtlasEntityHeader header : atlasSearchResult.getEntities()) {
      ret.addEntity(entitiesStore.getById(header.getGuid(), true, true).getEntity());
    }

    return ret;
  }
}
