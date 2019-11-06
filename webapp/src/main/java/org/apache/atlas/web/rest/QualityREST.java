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

package org.apache.atlas.web.rest;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.atlas.SortOrder;
import org.apache.atlas.discovery.AtlasQualityService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.springframework.stereotype.Service;

/**
 * REST interface for data quality
 */
@Path("v2/quality")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class QualityREST {

  private final AtlasQualityService qualityService;

  @Inject
  public QualityREST(AtlasQualityService qualityService) {
    this.qualityService = qualityService;
  }


  @GET
  @Path("process")
  public AtlasEntitiesWithExtInfo searchProcessQualities(
      @QueryParam("processName") String processName,
      @QueryParam("limit") int limit,
      @QueryParam("offset") int offset) throws AtlasBaseException {
    Servlets.validateQueryParamLength("processName", processName);

    AtlasPerfTracer perf = null;

    try {
      return qualityService.searchProcessQualities(processName, limit, offset);
    } finally {
      AtlasPerfTracer.log(perf);
    }
  }

  @GET
  @Path("dataset")
  public AtlasEntitiesWithExtInfo searchDatasetQualities(
      @QueryParam("datasetName") String datasetName,
      @QueryParam("limit") int limit,
      @QueryParam("offset") int offset) throws AtlasBaseException {
    Servlets.validateQueryParamLength("datasetName", datasetName);

    AtlasPerfTracer perf = null;

    try {
      return qualityService.searchDatasetQualities(datasetName, limit, offset);
    } finally {
      AtlasPerfTracer.log(perf);
    }
  }

}
