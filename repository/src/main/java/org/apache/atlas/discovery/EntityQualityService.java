/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.discovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.atlas.SortOrder;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.commons.configuration.Configuration;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EntityQualityService implements AtlasQualityService {

  private static final Logger LOG = LoggerFactory.getLogger(EntityQualityService.class);

  private final AtlasEntityStore entitiesStore;
  private final AtlasDiscoveryService discoveryService;
  private Configuration configuration;

  private static final String INFLUXDB_ADDRESS = "atlas.quality.influxdb.address";
  private static final String INFLUXDB_PROCESS_DATABASE = "atlas.quality.influxdb.process.database";
  private static final String INFLUXDB_DATASET_DATABASE = "atlas.quality.influxdb.dataset.database";
  private InfluxDB influxDB;

  @Inject
  public EntityQualityService(AtlasEntityStore entitiesStore,
      AtlasDiscoveryService discoveryService,
      Configuration configuration) {
    this.entitiesStore = entitiesStore;
    this.discoveryService = discoveryService;
    this.configuration = configuration;
    this.influxDB = InfluxDBFactory.connect(configuration.getString(INFLUXDB_ADDRESS));
  }

  @Override
  @GraphTransaction
  public AtlasEntitiesWithExtInfo searchProcessQualities(String processName, int limit,
      int offset) throws AtlasBaseException {
    String database = configuration.getString(INFLUXDB_PROCESS_DATABASE, "process-quality");
    String queryStr = String
        .format("select * from \"%s\" order by time desc limit %d", processName, limit);
    List<AtlasEntity> resultList = parseFromInfluxdb(database,queryStr);

    return new AtlasEntitiesWithExtInfo(resultList);
  }

  @Override
  public AtlasEntitiesWithExtInfo searchDatasetQualities(String datasetName, int limit, int offset)
      throws AtlasBaseException {
    String database = configuration.getString(INFLUXDB_DATASET_DATABASE, "data-quality");
    String queryStr = String
        .format(
            "select * from \"%s\" "
                + "where time> now() - 2d and "
                + "(\"key\" ='Abnormal' or \"key\" = 'Empty' or \"key\" = 'Null') "
                + "order by time desc limit %d",
            datasetName, limit);
    List<AtlasEntity> resultList = parseFromInfluxdb(database,queryStr);

    return new AtlasEntitiesWithExtInfo(resultList);

  }

  private List<AtlasEntity> parseFromInfluxdb(String database, String queryStr) {
    List<AtlasEntity> resultList = new ArrayList<>();
    LOG.info("Select influxdb sql: {}", queryStr);
    try {
      QueryResult queryResult = influxDB
          .query(new Query(queryStr, database), TimeUnit.MILLISECONDS);
      for (Result result : queryResult.getResults()) {
        for (Series series : result.getSeries()) {
          List<String> columns = series.getColumns();
          List<List<Object>> values = series.getValues();
          for (List<Object> list : values) {
            AtlasEntity atlasEntity = new AtlasEntity();
            for (int i = 0; i < columns.size(); i++) {
              if(list.get(i)==null) break;
              Object value =
                  list.get(i) instanceof Number ? ((Number) list.get(i)).longValue() : list.get(i);
              atlasEntity.setAttribute(columns.get(i), value);

            }
            resultList.add(atlasEntity);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Select influxdb error.", e);
    }
    return resultList;
  }

}
