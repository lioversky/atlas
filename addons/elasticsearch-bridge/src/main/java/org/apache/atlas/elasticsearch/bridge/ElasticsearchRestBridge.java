/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.elasticsearch.bridge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.elasticsearch.model.ElasticsearchDataTypes;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
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
 * Create by hongxun on 2019/8/30
 */
public class ElasticsearchRestBridge {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchRestBridge.class);

  private static final int EXIT_CODE_SUCCESS = 0;
  private static final int EXIT_CODE_FAILED = 1;
  private static final String ATLAS_ENDPOINT = "atlas.rest.address";
  private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";
  private static final String DEFAULT_CLUSTER_NAME = "primary";
  private static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
  private static final String DESCRIPTION_ATTR = "description";
  private static final String PARTITION_COUNT = "partitionCount";
  private static final String NAME = "name";
  private static final String URI = "uri";
  private static final String CLUSTERNAME = "clusterName";
  private static final String INDEX = "index";
  private static final String FORMAT_ES_INDEX_QUALIFIED_NAME = "%s@%s";
  private static final String FORMAT_ES_FIELD_QUALIFIED_NAME = "%s.%s@%s";
  private static final String ELASTICSEARCH_CLUSTER_NAME = "atlas.cluster.name";

  // es rest url
  private static final String ES_REST_ADDRESS = "elasticsearch.rest.address";
  private static final String DEFAULT_ES_REST_ADDRESS = "http://localhost:9200";
  private static final String ES_REST_INDICES = "_cat/indices";
  private static final String ES_REST_MAPPING_SETTINGS = "%s-*/_mappings,_settings";
  // es rest result json fields
  private static final String ES_JSON_MAPPINGS = "mappings";
  private static final String ES_JSON_PROPERTIES = "properties";
  private static final String ES_JSON_TYPE = "type";
  private static final String ES_JSON_FORMAT = "format";
  private static final String ES_JSON_SETTINGS = "settings";
  private static final String ES_JSON_INDEX = "index";
  private static final String ES_JSON_CREATION_DATE = "creation_date";
  //es store in atlas
  private static final String ES_INDEX_ATTR_HEALTH = "health";
  private static final String ES_INDEX_ATTR_STATUS = "status";
  private static final String ES_INDEX_ATTR_UUID = "uuid";
  private static final String ES_INDEX_ATTR_NUMBEROFSHARDS = "numberOfShards";
  private static final String ES_INDEX_ATTR_REPLICAS = "replicas";
  private static final String ES_INDEX_ATTR_DOCSCOUNT = "docsCount";
  private static final String ES_INDEX_ATTR_DOCSDELETED = "docsDeleted";
  private static final String ES_INDEX_ATTR_STORESIZE = "storeSize";
  private static final String ES_INDEX_ATTR_PRISTORESIZE = "priStoreSize";
  private static final String ES_INDEX_ATTR_PREFIX = "prefix";
  private static final String ES_INDEX_ATTR_CREATIONDATE = "creationDate";
  private static final String ES_INDEX_ATTR_FIELDS = "fields";

  private static final String ES_FIELD_ATTR_TYPE = "type";
  private static final String ES_FIELD_ATTR_INDEX_TYPE = "index_type";
  private static final String ES_FIELD_ATTR_FORMAT = "format";

  private static final String INDEX_SUXFIX_PATTERN = "index.suxfix.pattern";
  private static final String INDEX_SUXFIX_PATTERN_DEFAULT = "-\\d{4,8}$;-\\d{4}-\\d{2}-\\d{2}$";


  private static final TypeReference<HashMap<String, Object>> TYPE_MAP = new TypeReference<HashMap<String, Object>>() {
  };
  private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

  private final String clusterName;
  private final AtlasClientV2 atlasClientV2;
  private final Client client;
  private WebResource service;
  private Pattern[] suxfixPatterns;

  public ElasticsearchRestBridge(Configuration configuration, AtlasClientV2 atlasClientV2,
      String[] userInfo) {
    this.clusterName = configuration.getString(ELASTICSEARCH_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    this.atlasClientV2 = atlasClientV2;

    int readTimeout = configuration.getInt("atlas.client.readTimeoutMSecs", 60000);
    int connectTimeout = configuration.getInt("atlas.client.connectTimeoutMSecs", 60000);
    this.client = new Client();
    client.setReadTimeout(readTimeout);
    client.setConnectTimeout(connectTimeout);
    final HTTPBasicAuthFilter authFilter = new HTTPBasicAuthFilter(userInfo[0], userInfo[1]);
    client.addFilter(authFilter);
    String esAddress = configuration.getString(ES_REST_ADDRESS, DEFAULT_ES_REST_ADDRESS);
    service = client.resource(esAddress);
    String suxfixPatternValue = configuration
        .getString(INDEX_SUXFIX_PATTERN, INDEX_SUXFIX_PATTERN_DEFAULT);
    String[] regexes = suxfixPatternValue.split(";");
    suxfixPatterns = new Pattern[regexes.length];
    for (int i = 0; i < regexes.length; i++) {
      suxfixPatterns[i] = Pattern.compile(regexes[i]);
    }
  }

  public ElasticsearchRestBridge(Configuration configuration, AtlasClientV2 atlasClientV2) {
    this(configuration, atlasClientV2, getEsRestUserInfo());
  }

  private static String[] getEsRestUserInfo() {
    String username = null;
    String password = null;

    try {
      Console console = System.console();
      if (console == null) {
        System.err.println("Couldn't get a console object for user input");
        System.exit(1);
      }

      username = console.readLine("Enter username for elasticsearch :- ");
      char[] pwdChar = console.readPassword("Enter password for elasticsearch :- ", new Object[0]);
      if (pwdChar != null) {
        password = new String(pwdChar);
      }
    } catch (Exception var4) {
      System.out.print("Error while reading user input");
      System.exit(1);
    }

    return new String[]{username, password};
  }

  public static void main(String[] args) {
    int exitCode = EXIT_CODE_FAILED;
    AtlasClientV2 atlasClientV2 = null;

    try {
      Options options = new Options();
      options.addOption("", "index", true, "index");
      options.addOption("f", "filename", true, "filename");
      options.addOption("c", "conf", true, "configfile");

      CommandLineParser parser = new BasicParser();
      CommandLine cmd = parser.parse(options, args);
      String indexToImport = cmd.getOptionValue("i");
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

      ElasticsearchRestBridge importer = new ElasticsearchRestBridge(atlasConf, atlasClientV2);

      if (StringUtils.isNotEmpty(fileToImport)) {
        File f = new File(fileToImport);

        if (f.exists() && f.canRead()) {
          BufferedReader br = new BufferedReader(new FileReader(f));
          String line = null;

          while ((line = br.readLine()) != null) {
            indexToImport = line.trim();

            importer.importElasticsearchMetadata(indexToImport);
          }

          exitCode = EXIT_CODE_SUCCESS;
        } else {
          LOG.error("Failed to read the file");
        }
      } else {
        importer.importElasticsearchMetadata(indexToImport);

        exitCode = EXIT_CODE_SUCCESS;
      }
    } catch (ParseException e) {
      LOG.error("Failed to parse arguments. Error: ", e.getMessage());
      printUsage();
    } catch (Exception e) {
      System.out.println(
          "ImportElasticsearchEntities failed. Please check the log file for the detailed error message");
      e.printStackTrace();
      LOG.error("ImportElasticsearchEntities failed", e);
    } finally {
      if (atlasClientV2 != null) {
        atlasClientV2.close();
      }
    }
    System.exit(exitCode);
  }


  public void importElasticsearchMetadata(String indexToImport) {

    ClientResponse indexResponse = service.path(ES_REST_INDICES).get(ClientResponse.class);
    String textEntity = indexResponse.getEntity(String.class);
    String[] lines = textEntity.split("\n");
    //health status index uuid  pri rep docs.count docs.deleted store.size pri.store.size

    ListMultimap<String, IndexInfo> indicesMultimap = ArrayListMultimap.create();

    Arrays.stream(lines).map(line -> {
      try {
        return new IndexInfo(line);
      } catch (Exception e) {
        return null;
      }
    }).filter(indexInfo -> {
      if (indexInfo == null || indexInfo.getIndex().startsWith(".")) {
        return false;
      }
      if (!Strings.isNullOrEmpty(indexToImport) && !indexInfo.getIndex().contains(indexToImport)) {
        return false;
      }
      return true;
    }).forEach(indexInfo -> {
      boolean findFlag = false;
      String indexKey = null;
      for (Pattern p : suxfixPatterns) {
        Matcher matcher = p.matcher(indexInfo.getIndex());
        if (matcher.find()) {
          indexKey = matcher.replaceAll("");
          findFlag = true;
          break;
        }
      }
      if (findFlag) {
        indicesMultimap.put(indexKey, indexInfo);
      } else {
        indicesMultimap.put(indexInfo.getIndex(), indexInfo);
      }
    });
    importIndexList(indicesMultimap);

  }

  @VisibleForTesting
  void importIndexList(ListMultimap<String, IndexInfo> indicesMultimap) {

    // 判断prefix是否存在，存在获取 不存在创建
    Set<String> prefixSet = indicesMultimap.keySet();
    for (String prefix : prefixSet) {
      LOG.info("Start to import prefix {}", prefix);
      AtlasEntity prefixEntity;
      try {
        AtlasEntityWithExtInfo prefixEntityInAtlas = findIndexPrefixEntityInAtlas(prefix);
        if (prefixEntityInAtlas != null) {
          prefixEntity = prefixEntityInAtlas.getEntity();
        } else {
          prefixEntity = getIndexPrefixEntity(prefix);
          AtlasEntityWithExtInfo extInfo = createEntityInAtlas(
              new AtlasEntityWithExtInfo(prefixEntity));
          prefixEntity = extInfo.getEntity();
        }
        //遍历索引，如果存在获取并更新，不存在创建
        List<IndexInfo> infoList = indicesMultimap.get(prefix);
        Comparator<IndexInfo> comparator = new Comparator<IndexInfo>() {
          @Override
          public int compare(IndexInfo o1, IndexInfo o2) {
            return o1.getIndex().compareTo(o2.getIndex());
          }
        };
        Collections.sort(infoList, comparator);
        //获取index对应mapping信息
        infoList = getElasticsearchFields(prefix, infoList);
        for (IndexInfo info : infoList) {

          String indexName = info.getIndex();
          AtlasEntityWithExtInfo indexEntityInAtlas = findIndexEntityInAtlas(indexName);
          if (indexEntityInAtlas != null) {
            AtlasEntity indexEntity = indexEntityInAtlas.getEntity();
            updateIndex(prefixEntity, indexEntity, info);
          } else {
            LOG.info("Import index {}", info.getIndex());
            AtlasEntityWithExtInfo index = getIndex(prefixEntity, info, null);
            createEntityInAtlas(index);
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * 通过rest获取index mapping info.
   * 如果在infoList中存在，但在mapping不存在，删除此index.
   *
   * @param prefix index prefix
   * @param infoList 要获取信息的索引
   * @return 索引对应的字段信息
   */
  @VisibleForTesting
  List<IndexInfo> getElasticsearchFields(String prefix, List<IndexInfo> infoList) {
    //store index which have mapping
    List<IndexInfo> resultList = new ArrayList<>();
    ClientResponse response = service.path(String.format(ES_REST_MAPPING_SETTINGS, prefix))
        .get(ClientResponse.class);
    String textEntity = response.getEntity(String.class);
    Map<String, Object> resultMap = AtlasJson.fromJson(textEntity, TYPE_MAP);
    Iterator<IndexInfo> iterator = infoList.iterator();
    //iterate index
    while (iterator.hasNext()) {
      IndexInfo info = iterator.next();
      Map<String, Object> indexData = (Map<String, Object>) resultMap.get(info.getIndex());
      //no mapping
      if (indexData == null) {
        continue;
      }
      Map<String, Object> mappingData = (Map<String, Object>) indexData.get(ES_JSON_MAPPINGS);
      List<IndexField> fieldList = new ArrayList<>();
      // parse json
      for (String typeName : mappingData.keySet()) {
        if (typeName.startsWith("_")) {
          continue;
        }
        Map<String, Object> typeMap = (Map<String, Object>) mappingData.get(typeName);
        Map<String, Object> properties = (Map<String, Object>) typeMap.get(ES_JSON_PROPERTIES);
        // get fields info
        for (String field : properties.keySet()) {
          Map<String, Object> fieldMap = (Map<String, Object>) properties.get(field);
          String fieldType = (String) fieldMap.get(ES_JSON_TYPE);
          String fieldFormat = (String) fieldMap.get(ES_JSON_FORMAT);
          fieldList.add(new IndexField(info.getIndex(), field, fieldType, typeName, fieldFormat));
        }

      }
      // get index creation_date
      Map<String, Object> settingData = (Map<String, Object>) indexData.get(ES_JSON_SETTINGS);
      Map<String, Object> settingIndexData = (Map<String, Object>) settingData.get(ES_JSON_INDEX);
      Long creationDate = Long.parseLong((String) settingIndexData.get(ES_JSON_CREATION_DATE));

      info.setCreationDate(format.format(new Date(creationDate)));
      info.setFields(fieldList);
      resultList.add(info);
    }

    return resultList;

  }

  /**
   * find index prefix in atlas.
   *
   * @param prefixName prefixName
   */
  AtlasEntityWithExtInfo findIndexPrefixEntityInAtlas(String prefixName) {
    try {
      AtlasEntityWithExtInfo ret = findEntityInAtlas(
          ElasticsearchDataTypes.ELASTICSEARCH_PREFIX.getName(),
          getIndexQualifiedName(clusterName, prefixName));

      return ret;
    } catch (Exception e) {
      return null;
    }

  }

  AtlasEntityWithExtInfo findIndexEntityInAtlas(String indexName) {

    try {
      AtlasEntityWithExtInfo ret = findEntityInAtlas(
          ElasticsearchDataTypes.ELASTICSEARCH_INDEX.getName(),
          getIndexQualifiedName(clusterName, indexName));
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


  String getIndexQualifiedName(String clusterName, String index) {
    return String.format(FORMAT_ES_INDEX_QUALIFIED_NAME, index.toLowerCase(), clusterName);
  }

  String getFieldQualifiedName(String clusterName, String index, String field) {
    return String.format(FORMAT_ES_FIELD_QUALIFIED_NAME, index.toLowerCase(), field.toLowerCase(),
        clusterName);
  }


  private AtlasEntity getIndexPrefixEntity(String prefixName) {

    AtlasEntity indexPrefix = new AtlasEntity(
        ElasticsearchDataTypes.ELASTICSEARCH_PREFIX.getName());

//    long        createTime         = BaseHiveEvent.getTableCreateTime(hiveTable);
//    long        lastAccessTime     = hiveTable.getLastAccessTime() > 0 ? hiveTable.getLastAccessTime() : createTime;

    String qualifiedName = getIndexQualifiedName(clusterName, prefixName);
    indexPrefix.setAttribute(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
    indexPrefix.setAttribute(NAME, prefixName);
    indexPrefix.setAttribute(CLUSTERNAME, clusterName);

    return indexPrefix;
  }


  private AtlasEntityWithExtInfo getIndex(AtlasEntity prefixEntity, IndexInfo info,
      AtlasEntity indexEntity) {

    if (indexEntity == null) {
      indexEntity = new AtlasEntity(
          ElasticsearchDataTypes.ELASTICSEARCH_INDEX.getName());
    }

    String indexQualifiedName = getIndexQualifiedName(clusterName, info.getIndex());
    indexEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, indexQualifiedName);
    indexEntity.setAttribute(NAME, info.getIndex());
    indexEntity.setAttribute(ES_INDEX_ATTR_HEALTH, info.getHealth());
    indexEntity.setAttribute(ES_INDEX_ATTR_STATUS, info.getStatus());
    indexEntity.setAttribute(ES_INDEX_ATTR_UUID, info.getUuid());
    indexEntity.setAttribute(ES_INDEX_ATTR_NUMBEROFSHARDS, info.getNumberOfShards());
    indexEntity.setAttribute(ES_INDEX_ATTR_REPLICAS, info.getRep());
    indexEntity.setAttribute(ES_INDEX_ATTR_DOCSCOUNT, info.getDocsCount());
    indexEntity.setAttribute(ES_INDEX_ATTR_DOCSDELETED, info.getDocsDeleted());
    indexEntity.setAttribute(ES_INDEX_ATTR_STORESIZE, info.getStoreSize());
    indexEntity.setAttribute(ES_INDEX_ATTR_PRISTORESIZE, info.getPriStoreSize());
    indexEntity.setAttribute(ES_INDEX_ATTR_PREFIX, AtlasTypeUtil.getAtlasObjectId(prefixEntity));
    indexEntity.setAttribute(ES_INDEX_ATTR_CREATIONDATE, info.getCreationDate());

    AtlasEntityWithExtInfo index = new AtlasEntityWithExtInfo(indexEntity);
    List<AtlasEntity> fieldEntityList = new ArrayList<>();
    for (IndexField field : info.getFields()) {
      AtlasEntity fieldEntity = getIndexFieldEntity(field, indexEntity);
      index.addReferredEntity(fieldEntity);
      fieldEntityList.add(fieldEntity);
    }

    indexEntity
        .setAttribute(ES_INDEX_ATTR_FIELDS, AtlasTypeUtil.getAtlasObjectIds(fieldEntityList));
    return index;
  }

  private void updateIndex(AtlasEntity prefixEntity, AtlasEntity indexEntityInAtlas, IndexInfo info)
      throws AtlasServiceException {
    if (!compareIndex(indexEntityInAtlas, info)) {
      AtlasEntityWithExtInfo index = getIndex(prefixEntity, info, indexEntityInAtlas);
      updateInstanceInAtlas(index);
      LOG.info("Update index {}", info.getIndex());
    }
  }

  /**
   * compare atlas entity attribute with index info.
   *
   * @param entity index in atlas
   * @param indexInfo indix info
   * @return equals
   */
  private boolean compareIndex(AtlasEntity entity, IndexInfo indexInfo) {
    Map<String, Object> attributes = entity.getAttributes();
    if (indexInfo.health.equals(attributes.get(ES_INDEX_ATTR_HEALTH)) &&
        indexInfo.status.equals(attributes.get(ES_INDEX_ATTR_STATUS)) &&
        indexInfo.numberOfShards.equals(attributes.get(ES_INDEX_ATTR_NUMBEROFSHARDS)) &&
//        indexInfo.creationDate.equals(attributes.get("creationDate")) &&

        indexInfo.docsCount.equals(((Number) attributes.get(ES_INDEX_ATTR_DOCSCOUNT)).longValue())
        &&
        indexInfo.docsDeleted
            .equals(((Number) attributes.get(ES_INDEX_ATTR_DOCSDELETED)).longValue()) &&
        indexInfo.storeSize.equals(attributes.get(ES_INDEX_ATTR_STORESIZE)) &&
        indexInfo.priStoreSize.equals(attributes.get(ES_INDEX_ATTR_PRISTORESIZE)) &&
        indexInfo.uuid.equals(attributes.get(ES_INDEX_ATTR_UUID)) &&
        indexInfo.rep.equals(attributes.get(ES_INDEX_ATTR_REPLICAS))) {
      return true;
    }
    return false;

  }

  private AtlasEntityWithExtInfo createEntityInAtlas(AtlasEntityWithExtInfo entity)
      throws Exception {
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

  private AtlasEntity getIndexFieldEntity(IndexField field, AtlasEntity indexEntity) {

    AtlasEntity fieldEntity = new AtlasEntity(
        ElasticsearchDataTypes.ELASTICSEARCH_FIELD.getName());
    String fieldQualifiedName = getFieldQualifiedName(clusterName, field.index, field.name);
    fieldEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, fieldQualifiedName);

    fieldEntity.setAttribute(NAME, field.name);
    fieldEntity.setAttribute(INDEX, AtlasTypeUtil.getAtlasObjectId(indexEntity));
    fieldEntity.setAttribute(ES_FIELD_ATTR_TYPE, field.type);
    fieldEntity.setAttribute(ES_FIELD_ATTR_INDEX_TYPE, field.index_type);
    fieldEntity.setAttribute(ES_FIELD_ATTR_FORMAT, field.format);

    return fieldEntity;
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

  static void printUsage() {
    System.out.println("Usage 1: import-elasticsearch-rest.sh");
    System.out.println(
        "Usage 2: import-elasticsearch.sh [-i <index regex> OR --index <index regex>]");
    System.out.println("Usage 3: import-elasticsearch.sh [-f <filename>]");
    System.out.println("   Format:");
    System.out.println("        index1 OR index1 regex");
    System.out.println("        index2 OR index2 regex");
    System.out.println("        index3 OR index3 regex");
  }

  static class IndexInfo {

    private String health;
    private String status;
    private String index;
    private String uuid;
    private Integer numberOfShards;
    private Integer rep;
    private Long docsCount;
    private Long docsDeleted;
    private String storeSize;
    private String priStoreSize;
    private String creationDate;
    private List<IndexField> fields;

    public IndexInfo(String[] fields) {
      if (fields.length != 10) {
        throw new IllegalArgumentException("Index fields must be 10.");
      }
      this.health = fields[0];
      this.status = fields[1];
      this.index = fields[2];
      this.uuid = fields[3];
      this.numberOfShards = Integer.parseInt(fields[4]);
      this.rep = Integer.parseInt(fields[5]);
      this.docsCount = Long.parseLong(fields[6]);
      this.docsDeleted = Long.parseLong(fields[7]);
      this.storeSize = fields[8];
      this.priStoreSize = fields[9];
    }

    public IndexInfo(String info) {
      this(info.split("\\s+", -1));
    }

    public String getHealth() {
      return health;
    }

    public String getStatus() {
      return status;
    }

    public String getIndex() {
      return index;
    }

    public String getUuid() {
      return uuid;
    }

    public Integer getNumberOfShards() {
      return numberOfShards;
    }

    public Integer getRep() {
      return rep;
    }

    public Long getDocsCount() {
      return docsCount;
    }

    public Long getDocsDeleted() {
      return docsDeleted;
    }

    public String getStoreSize() {
      return storeSize;
    }

    public String getPriStoreSize() {
      return priStoreSize;
    }

    public String getCreationDate() {
      return creationDate;
    }

    public void setCreationDate(String creationDate) {
      this.creationDate = creationDate;
    }

    public List<IndexField> getFields() {
      return fields;
    }

    public void setFields(
        List<IndexField> fields) {
      this.fields = fields;
    }
  }

  static class IndexField {

    private String index;
    private String name;
    private String type;
    private String index_type;
    private String format;

    public IndexField(String index, String name, String type, String index_type,
        String format) {
      this.index = index;
      this.name = name;
      this.type = type;
      this.index_type = index_type;
      this.format = format;
    }
  }

}
