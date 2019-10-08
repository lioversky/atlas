/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.pinot.bridge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.pinot.model.PinotDataTypes;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by hongxun on 2019/9/5
 */
public class PinotBridge {

  private static final Logger LOG = LoggerFactory.getLogger(PinotBridge.class);

  private static final int EXIT_CODE_SUCCESS = 0;
  private static final int EXIT_CODE_FAILED = 1;
  private static final String ATLAS_ENDPOINT = "atlas.rest.address";
  private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";
  private static final String DEFAULT_CLUSTER_NAME = "primary";
  private static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
  private static final String PINOT_CLUSTER_NAME = "atlas.cluster.name";
  private static final String FORMAT_PINOT_TABLE_QUALIFIED_NAME = "%s@%s";
  private static final String FORMAT_PINOT_FIELD_QUALIFIED_NAME = "%s.%s@%s";
  private static final String NAME = "name";
  private static final String TABLE = "table";
  private static final String FIELD_ATTR_DATATYPE = "dataType";
  private static final String FIELD_ATTR_TIMETYPE = "timeType";
  private static final String FIELD_ATTR_SINGLEVALUEFIELD = "singleValueField";
  private static final String FIELD_ATTR_INCOMING = "incomingGranularitySpec";

  private static final String HTTP_ACCEPT = "application/json";
  private static final String PINOT_JSON_TABLES = "tables";
  private static final String PINOT_JSON_REALTIME = "REALTIME";
  private static final String PINOT_JSON_OFFLINE = "OFFLINE";

  private static final String PINOT_JSON_SEGMENTSCONFIG = "segmentsConfig";
  private static final String PINOT_JSON_RETENTIONTIMEUNIT = "retentionTimeUnit";
  private static final String PINOT_JSON_SEGMENTPUSHFREQUENCY = "segmentPushFrequency";
  private static final String PINOT_JSON_REPLICATION = "replication";
  private static final String PINOT_JSON_RETENTIONTIMEVALUE = "retentionTimeValue";
  private static final String PINOT_JSON_SEGMENTPUSHTYPE = "segmentPushType";
  private static final String PINOT_JSON_SEGMENTASSIGNMENTSTRATEGY = "segmentAssignmentStrategy";
  private static final String PINOT_JSON_REPLICASPERPARTITION = "replicasPerPartition";
  private static final String PINOT_JSON_TABLENAME = "tableName";


  private static final String PINOT_TABLE_ATTR_REALTIME = "realtime";
  private static final String PINOT_TABLE_ATTR_OFFLINE = "offline";
  private static final String PINOT_TABLE_ATTR_FORMAT = "%s.%s.%s";
  private static final String PINOT_TABLE_ATTR_DIMENSIONFIELDSPECS = "dimensionFieldSpecs";
  private static final String PINOT_TABLE_ATTR_METRICFIELDSPECS = "metricFieldSpecs";
  private static final String PINOT_TABLE_ATTR_TIMEFIELDSPEC = "timeFieldSpec";
  private static final String PINOT_TABLE_ATTR_TIMEFIELD = "timeField";


  // es rest url
  private static final String PINOT_REST_ADDRESS = "pinot.rest.address";
  private static final String DEFAULT_PINOT_REST_ADDRESS = "http://localhost:9000/tables";

  private static final TypeReference<HashMap<String, Object>> TYPE_MAP = new TypeReference<HashMap<String, Object>>() {
  };

  private final String clusterName;
  private final AtlasClientV2 atlasClientV2;
  private final Client client;
  private WebResource service;

  public PinotBridge(Configuration configuration, AtlasClientV2 atlasClientV2) {
    this.clusterName = configuration.getString(PINOT_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    this.atlasClientV2 = atlasClientV2;

    int readTimeout = configuration.getInt("atlas.client.readTimeoutMSecs", 60000);
    int connectTimeout = configuration.getInt("atlas.client.connectTimeoutMSecs", 60000);
    this.client = new Client();
    client.setReadTimeout(readTimeout);
    client.setConnectTimeout(connectTimeout);
    String pinotAddress = configuration.getString(PINOT_REST_ADDRESS,DEFAULT_PINOT_REST_ADDRESS);
    service = client.resource(pinotAddress);

  }

  public static void main(String[] args) {
    int exitCode = EXIT_CODE_FAILED;
    AtlasClientV2 atlasClientV2 = null;

    try {
      Options options = new Options();
      options.addOption("t", "table", true, "table");
      options.addOption("f", "filename", true, "filename");
      options.addOption("c", "conf", true, "configfile");

      CommandLineParser parser = new BasicParser();
      CommandLine cmd = parser.parse(options, args);
      String tableToImport = cmd.getOptionValue("t");
      String fileToImport = cmd.getOptionValue("f");
      String confFile = cmd.getOptionValue("c");

      Configuration atlasConf;
      if (confFile != null && !"".equals(confFile)) {
        File conf = new File(confFile);
        System.setProperty(ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY,
            conf.getCanonicalFile().getParent());
        atlasConf = ApplicationProperties.get(conf.getName());
      } else {
        atlasConf = ApplicationProperties.get();
      }
      String[] urls = atlasConf.getStringArray(ATLAS_ENDPOINT);

      if (urls == null || urls.length == 0) {
        urls = new String[]{DEFAULT_ATLAS_URL};
      }

      if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
        String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();

        atlasClientV2 = new AtlasClientV2(urls, basicAuthUsernamePassword);
      } else {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

        atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), urls);
      }

      PinotBridge importer = new PinotBridge(atlasConf, atlasClientV2);

      if (StringUtils.isNotEmpty(fileToImport)) {
        File f = new File(fileToImport);

        if (f.exists() && f.canRead()) {
          BufferedReader br = new BufferedReader(new FileReader(f));
          String line = null;

          while ((line = br.readLine()) != null) {
            tableToImport = line.trim();

            importer.importPinotMetadata(tableToImport);
          }

          exitCode = EXIT_CODE_SUCCESS;
        } else {
          LOG.error("Failed to read the file");
        }
      } else {
        importer.importPinotMetadata(tableToImport);

        exitCode = EXIT_CODE_SUCCESS;
      }
    } catch (ParseException e) {
      LOG.error("Failed to parse arguments. Error: ", e.getMessage());
      printUsage();
    } catch (Exception e) {
      System.out.println(
          "Import Pinot entities failed. Please check the log file for the detailed error message");
      e.printStackTrace();
      LOG.error("Import Pinot entities failed", e);
    } finally {
      if (atlasClientV2 != null) {
        atlasClientV2.close();
      }
    }
    System.exit(exitCode);
  }

  public void importPinotMetadata(String tableToImport) throws AtlasServiceException {
    ClientResponse tableResponse = service.accept(HTTP_ACCEPT)
        .get(ClientResponse.class);
    String textEntity = tableResponse.getEntity(String.class);
    Map<String, Object> resultMap = AtlasJson.fromJson(textEntity, TYPE_MAP);

    List<String> tableList = (List<String>) resultMap.get(PINOT_JSON_TABLES);
    tableList = tableList.stream()
        .filter(table -> Strings.isNullOrEmpty(tableToImport) || table.contains(tableToImport))
        .collect(Collectors.toList());
    importTables(tableList);
  }

  public void importTables(List<String> tables) throws AtlasServiceException {

    for (String table : tables) {
      ClientResponse tableResponse = service.path(table).accept(HTTP_ACCEPT)
          .get(ClientResponse.class);
      String tableEntity = tableResponse.getEntity(String.class);
      Map<String, Object> resultMap = AtlasJson.fromJson(tableEntity, TYPE_MAP);

      ClientResponse schemaResponse = service.path(String.format("%s/schema", table))
          .get(ClientResponse.class);
      String schemaEntity = schemaResponse.getEntity(String.class);
      Map<String, Object> schemas = AtlasJson.fromJson(schemaEntity, TYPE_MAP);
      AtlasEntityWithExtInfo tableEntityInAtlas = findTableEntityInAtlas(table);
      if (tableEntityInAtlas != null) {
        AtlasEntityWithExtInfo entityWithExtInfo = getTable(table, tableEntityInAtlas.getEntity(),
            resultMap, schemas);
        updateInstanceInAtlas(entityWithExtInfo);
      } else {
        AtlasEntityWithExtInfo entityWithExtInfo = getTable(table, null, resultMap, schemas);
        createEntityInAtlas(entityWithExtInfo);
      }
    }
  }

  @VisibleForTesting
  AtlasEntityWithExtInfo getTable(String tableName, AtlasEntity tableEntity,
      Map<String, Object> resultMap, Map<String, Object> schemas) {

    if (tableEntity == null) {
      tableEntity = new AtlasEntity(PinotDataTypes.PINOT_TABLE.getName());
    }

    String tableQualifiedName = getTableQualifiedName(clusterName, tableName);
    tableEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tableQualifiedName);
    tableEntity.setAttribute(NAME, tableName);

    if (resultMap.containsKey(PINOT_JSON_REALTIME)) {
      Map<String, Object> realtimeMap = (Map<String, Object>) resultMap.get(PINOT_JSON_REALTIME);
      Map<String, Object> realtimeSegmentsConfig = (Map<String, Object>) realtimeMap
          .get(PINOT_JSON_SEGMENTSCONFIG);
      setConfigAttribute(tableEntity, PINOT_TABLE_ATTR_REALTIME, realtimeSegmentsConfig);
      tableEntity
          .setAttribute(String.format("%s.%s", PINOT_TABLE_ATTR_REALTIME, PINOT_JSON_TABLENAME),
              realtimeMap.get(PINOT_JSON_TABLENAME));
    }

    if (resultMap.containsKey(PINOT_JSON_OFFLINE)) {
      Map<String, Object> realtimeMap = (Map<String, Object>) resultMap.get(PINOT_JSON_OFFLINE);
      Map<String, Object> offlineSegmentsConfig = (Map<String, Object>) realtimeMap
          .get(PINOT_JSON_SEGMENTSCONFIG);
      setConfigAttribute(tableEntity, PINOT_TABLE_ATTR_OFFLINE, offlineSegmentsConfig);
      tableEntity
          .setAttribute(String.format("%s.%s", PINOT_TABLE_ATTR_OFFLINE, PINOT_JSON_TABLENAME),
              realtimeMap.get(PINOT_JSON_TABLENAME));
    }

    AtlasEntityWithExtInfo table = new AtlasEntityWithExtInfo(tableEntity);
    // dimensionField
    if (schemas.containsKey(PINOT_TABLE_ATTR_DIMENSIONFIELDSPECS)) {
      List<AtlasEntity> dimensionFieldSpecs = getDimensionFieldEntities(tableName, tableEntity,
          ((List<Map<String, Object>>) schemas.get(PINOT_TABLE_ATTR_DIMENSIONFIELDSPECS)));
      tableEntity
          .setAttribute(PINOT_TABLE_ATTR_DIMENSIONFIELDSPECS,
              AtlasTypeUtil.getAtlasObjectIds(dimensionFieldSpecs));
      for (AtlasEntity entity : dimensionFieldSpecs) {
        table.addReferredEntity(entity);
      }
    }
    // metricField
    if (schemas.containsKey(PINOT_TABLE_ATTR_METRICFIELDSPECS)) {

      List<AtlasEntity> metricFieldSpecs = getMetricFieldEntities(tableName, tableEntity,
          ((List<Map<String, Object>>) schemas.get(PINOT_TABLE_ATTR_METRICFIELDSPECS)));

      tableEntity
          .setAttribute(PINOT_TABLE_ATTR_METRICFIELDSPECS,
              AtlasTypeUtil.getAtlasObjectIds(metricFieldSpecs));
      for (AtlasEntity entity : metricFieldSpecs) {
        table.addReferredEntity(entity);
      }
    }
    // timeField
    AtlasEntity timeFieldEntity = getTimeFieldEntity(tableName, tableEntity,
        (Map<String, Object>) schemas.get(PINOT_TABLE_ATTR_TIMEFIELDSPEC));
    tableEntity
        .setAttribute(PINOT_TABLE_ATTR_TIMEFIELD, AtlasTypeUtil.getAtlasObjectId(timeFieldEntity));
    table.addReferredEntity(timeFieldEntity);

    return table;
  }

  @VisibleForTesting
  List<AtlasEntity> getDimensionFieldEntities(String tableName, AtlasEntity tableEntity,
      List<Map<String, Object>> dimensionFieldSpecs) {
    List<AtlasEntity> fieldEntityList = new ArrayList<>();
    for (Map<String, Object> fields : dimensionFieldSpecs) {
      String name = (String) fields.get(NAME);
      AtlasEntity fieldEntity = new AtlasEntity(PinotDataTypes.PINOT_DIMENSION_FIELD.getName());
      String fieldQualifiedName = getFieldQualifiedName(clusterName, tableName, name);
      fieldEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, fieldQualifiedName);

      fieldEntity.setAttribute(NAME, name);
      fieldEntity.setAttribute(TABLE, AtlasTypeUtil.getAtlasObjectId(tableEntity));
      fieldEntity.setAttribute(FIELD_ATTR_DATATYPE, fields.get(FIELD_ATTR_DATATYPE));
      fieldEntity.setAttribute(FIELD_ATTR_SINGLEVALUEFIELD,
          fields.get(FIELD_ATTR_SINGLEVALUEFIELD));

      fieldEntityList.add(fieldEntity);

    }
    return fieldEntityList;
  }

  @VisibleForTesting
  List<AtlasEntity> getMetricFieldEntities(String tableName, AtlasEntity tableEntity,
      List<Map<String, Object>> metricFieldSpecs) {

    List<AtlasEntity> fieldEntityList = new ArrayList<>();
    for (Map<String, Object> fields : metricFieldSpecs) {
      String name = (String) fields.get(NAME);
      AtlasEntity fieldEntity = new AtlasEntity(PinotDataTypes.PINOT_METRIC_FIELD.getName());
      String fieldQualifiedName = getFieldQualifiedName(clusterName, tableName, name);
      fieldEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, fieldQualifiedName);

      fieldEntity.setAttribute(NAME, name);
      fieldEntity.setAttribute(TABLE, AtlasTypeUtil.getAtlasObjectId(tableEntity));
      fieldEntity.setAttribute(FIELD_ATTR_DATATYPE, fields.get(FIELD_ATTR_DATATYPE));
      fieldEntity.setAttribute(FIELD_ATTR_SINGLEVALUEFIELD,
          fields.get(FIELD_ATTR_SINGLEVALUEFIELD));

      fieldEntityList.add(fieldEntity);

    }
    return fieldEntityList;
  }

  @VisibleForTesting
  AtlasEntity getTimeFieldEntity(String tableName, AtlasEntity tableEntity,
      Map<String, Object> metricFieldSpecs) {
    Map<String, Object> incomingMap = (Map<String, Object>) metricFieldSpecs
        .get(FIELD_ATTR_INCOMING);
    AtlasEntity timeFieldEntity = new AtlasEntity(PinotDataTypes.PINOT_TIME_FIELD.getName());
    String name = (String) incomingMap.get(NAME);
    String fieldQualifiedName = getFieldQualifiedName(clusterName, tableName, name);
    timeFieldEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, fieldQualifiedName);
    timeFieldEntity.setAttribute(NAME, name);
    timeFieldEntity.setAttribute(TABLE, AtlasTypeUtil.getAtlasObjectId(tableEntity));
    timeFieldEntity
        .setAttribute(FIELD_ATTR_DATATYPE, incomingMap.get(FIELD_ATTR_DATATYPE));
    timeFieldEntity
        .setAttribute(FIELD_ATTR_TIMETYPE, incomingMap.get(FIELD_ATTR_DATATYPE));

    return timeFieldEntity;
  }


  private AtlasEntity setConfigAttribute(AtlasEntity tableEntity, String tableType,
      Map<String, Object> map) {
    tableEntity.setAttribute(
        String
            .format(PINOT_TABLE_ATTR_FORMAT, tableType, PINOT_JSON_SEGMENTSCONFIG,
                PINOT_JSON_RETENTIONTIMEUNIT),
        map.get(PINOT_JSON_RETENTIONTIMEUNIT));
    tableEntity.setAttribute(
        String.format(PINOT_TABLE_ATTR_FORMAT, tableType, PINOT_JSON_SEGMENTSCONFIG,
            PINOT_JSON_SEGMENTPUSHFREQUENCY),
        map.get(PINOT_JSON_SEGMENTPUSHFREQUENCY));
    tableEntity.setAttribute(
        String.format(PINOT_TABLE_ATTR_FORMAT, tableType, PINOT_JSON_SEGMENTSCONFIG,
            PINOT_JSON_REPLICATION),
        map.get(PINOT_JSON_REPLICATION));
    tableEntity.setAttribute(
        String.format(PINOT_TABLE_ATTR_FORMAT, tableType, PINOT_JSON_SEGMENTSCONFIG,
            PINOT_JSON_RETENTIONTIMEVALUE),
        map.get(PINOT_JSON_RETENTIONTIMEVALUE));
    tableEntity.setAttribute(
        String.format(PINOT_TABLE_ATTR_FORMAT, tableType, PINOT_JSON_SEGMENTSCONFIG,
            PINOT_JSON_SEGMENTPUSHTYPE),
        map.get(PINOT_JSON_SEGMENTPUSHTYPE));
    tableEntity.setAttribute(String
            .format(PINOT_TABLE_ATTR_FORMAT, tableType, PINOT_JSON_SEGMENTSCONFIG,
                PINOT_JSON_SEGMENTASSIGNMENTSTRATEGY),
        map.get(PINOT_JSON_SEGMENTASSIGNMENTSTRATEGY));
    tableEntity.setAttribute(
        String.format(PINOT_TABLE_ATTR_FORMAT, tableType, PINOT_JSON_SEGMENTSCONFIG,
            PINOT_JSON_REPLICASPERPARTITION),
        map.get(PINOT_JSON_REPLICASPERPARTITION));

    return tableEntity;
  }

  @VisibleForTesting
  AtlasEntityWithExtInfo findTableEntityInAtlas(String tableName) {

    try {
      AtlasEntityWithExtInfo ret = findEntityInAtlas(PinotDataTypes.PINOT_TABLE.getName(),
          getTableQualifiedName(clusterName, tableName));
      clearRelationshipAttributes(ret);
      return ret;
    } catch (Exception e) {
      return null;
    }

  }

  private AtlasEntityWithExtInfo findEntityInAtlas(String typeName, String qualifiedName)
      throws Exception {
    Map<String, String> attributes = Collections
        .singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);

    return atlasClientV2.getEntityByAttribute(typeName, attributes);
  }

  private AtlasEntityWithExtInfo createEntityInAtlas(AtlasEntityWithExtInfo entity)
      throws AtlasServiceException {
    AtlasEntityWithExtInfo ret = null;
    EntityMutationResponse response = atlasClientV2.createEntity(entity);
    List<AtlasEntityHeader> entities = response.getCreatedEntities();

    if (CollectionUtils.isNotEmpty(entities)) {
      AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2
          .getEntityByGuid(entities.get(0).getGuid());

      ret = getByGuidResponse;

      LOG.info("Created {} entity: name={}, guid={}", ret.getEntity().getTypeName(),
          ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
    }

    return ret;
  }


  private void updateInstanceInAtlas(AtlasEntityWithExtInfo entity) throws AtlasServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("updating {} entity: {}", entity.getEntity().getTypeName(), entity);
    }

    atlasClientV2.updateEntity(entity);

    LOG.info("Updated {} entity: name={}, guid={}", entity.getEntity().getTypeName(),
        entity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), entity.getEntity().getGuid());
  }


  private void clearRelationshipAttributes(AtlasEntityWithExtInfo entity) {
    if (entity != null) {
      clearRelationshipAttributes(entity.getEntity());

      if (entity.getReferredEntities() != null) {
        clearRelationshipAttributes(entity.getReferredEntities().values());
      }
    }
  }

  private void clearRelationshipAttributes(Collection<AtlasEntity> entities) {
    if (entities != null) {
      for (AtlasEntity entity : entities) {
        clearRelationshipAttributes(entity);
      }
    }
  }

  private void clearRelationshipAttributes(AtlasEntity entity) {
    if (entity != null && entity.getRelationshipAttributes() != null) {
      entity.getRelationshipAttributes().clear();
    }
  }


  String getTableQualifiedName(String clusterName, String table) {
    return String.format(FORMAT_PINOT_TABLE_QUALIFIED_NAME, table.toLowerCase(), clusterName);
  }

  String getFieldQualifiedName(String clusterName, String table, String field) {
    return String
        .format(FORMAT_PINOT_FIELD_QUALIFIED_NAME, table.toLowerCase(), field.toLowerCase(),
            clusterName);
  }

  static void printUsage() {
    System.out.println("Usage 1: import-pinot-rest.sh");
    System.out.println(
        "Usage 2: import-pinot.sh [-i <table regex> OR --table <table regex>]");
    System.out.println("Usage 3: import-pinot.sh [-f <filename>]");
    System.out.println("   Format:");
    System.out.println("        table1 OR table1 regex");
    System.out.println("        table2 OR table2 regex");
    System.out.println("        table3 OR table3 regex");
  }
}
