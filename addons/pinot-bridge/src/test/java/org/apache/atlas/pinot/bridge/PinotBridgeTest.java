package org.apache.atlas.pinot.bridge;

import com.google.common.collect.Lists;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

/**
 * Create by hongxun on 2019/9/5
 */
public class PinotBridgeTest {

  public static final String ATLAS_ENDPOINT = "atlas.rest.address";
  private static final String DEFAULT_ATLAS_URL = "localhost:21000/";

  PinotBridge bridge;

  @Before
  public void setup() {
    try {

      Configuration atlasConf = ApplicationProperties.get();
      String[] atlasEndpoint = atlasConf.getStringArray(ATLAS_ENDPOINT);

      if (atlasEndpoint == null || atlasEndpoint.length == 0) {
        atlasEndpoint = new String[]{DEFAULT_ATLAS_URL};
      }
      AtlasClientV2 atlasClientV2 = new AtlasClientV2(atlasEndpoint,
          new String[]{"admin", "admin"});
      bridge = new PinotBridge(atlasConf, atlasClientV2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void importTable(){
    try {
      bridge.importTables(Lists.newArrayList("ab_test"));
    } catch (AtlasServiceException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void importMetadata(){
    try {
      bridge.importPinotMetadata("");
    } catch (AtlasServiceException e) {
      e.printStackTrace();
    }
  }

}
