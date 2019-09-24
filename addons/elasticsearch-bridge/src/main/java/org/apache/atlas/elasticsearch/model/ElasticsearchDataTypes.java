package org.apache.atlas.elasticsearch.model;

/**
 * Create by hongxun on 2019/8/30
 */
public enum ElasticsearchDataTypes {

  ELASTICSEARCH_PREFIX,
  ELASTICSEARCH_INDEX,
  ELASTICSEARCH_FIELD;

  public String getName() {
    return name().toLowerCase();
  }
}
