package org.apache.atlas.pinot.model;

/**
 * Create by hongxun on 2019/9/5
 */
public enum PinotDataTypes {
  PINOT_TABLE,
  PINOT_DIMENSION_FIELD,
  PINOT_METRIC_FIELD,
  PINOT_TIME_FIELD;

  public String getName() {
    return name().toLowerCase();
  }
}
