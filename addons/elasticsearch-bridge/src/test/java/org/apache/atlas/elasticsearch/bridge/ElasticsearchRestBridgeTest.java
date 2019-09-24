package org.apache.atlas.elasticsearch.bridge;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

/**
 * Create by hongxun on 2019/9/2
 */
public class ElasticsearchRestBridgeTest {

  private static final String ATLAS_ENDPOINT = "atlas.rest.address";
  private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";
  private ElasticsearchRestBridge bridge;

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
      bridge = new ElasticsearchRestBridge(atlasConf, atlasClientV2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Test
  public void importElasticsearchMetadata() {
    bridge.importElasticsearchMetadata("");
  }


}
