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

define(['require',
    'backbone',
    'hbs!tmpl/quality/ProcessTableLayoutView_tmpl',
    'collection/VProcessList',
    'modules/Modal',
    'utils/Utils',
    'utils/CommonViewFunction',
    'utils/Messages',
    'utils/Globals',
    'utils/Enums',
    'utils/UrlLinks'
], function(require, Backbone, ProcessTableLayoutViewTmpl, VProcessList, Modal, Utils, CommonViewFunction, Messages, Globals, Enums, UrlLinks) {
    'use strict';

    var ProcessTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends ProcessTableLayoutView */
        {
            _viewName: 'ProcessTableLayoutView',

            template: ProcessTableLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RProcessTableLayoutView: "#r_processTableLayoutView",
            },
            /** ui selector cache */
            ui: {
                processName: '[data-id="processName"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};

                return events;
            },
            /**
             * intialize a new ProcessTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'nodeInfo', 'fetchCollection'));
                this.processCollection = new VProcessList();
                this.processTableAttribute = ["id", "startTime", "duration",
                "numOutputRows", "numOutputBytes", "readMetrics"];

                this.commonTableOptions = {
                    collection: this.processCollection,
                    includeFilter: false,
                    includePagination: true,
                    includePageSize: true,
                    includeGotoPage: true,
                    includeFooterRecords: true,
                    includeOrderAbleColumns: false,
                    includeAtlasTableSorting: true,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.bradCrumbList = [];
                var that = this,
                    modalObj = {
                      title: 'Process Quality',
                      content: this,
                      okText: 'OK',
                      okCloses: true,
                      mainClass: 'modal-lg',
                      allowCancel: true,
                    };
                this.modal = new Modal(modalObj)
                this.modal.open();
              this.modal.$el.find('button.cancel').remove();
                this.on('ok', function() {
                  this.modal.trigger('cancel');
                });
                this.on('closeModal', function() {
                  this.modal.trigger('cancel');
                });
            },
            onRender: function() {
              $.extend(this.processCollection.queryParams, { count: this.limit });
                this.fetchCollection();
            },
            fetchCollection: function(options) {
              var that = this;
              if("spark_process" == that.nodeInfo.typeName){

                var queryParam = {
                  processName: that.nodeInfo.name,
                  limit: 25,
                  offset: 0
                }
                this.$('.fontLoader').show();
                this.$('.tableOverlay').show();

                this.processCollection.getProcess({
                  skipDefaultError: true,
                  queryParam: queryParam,
                  success: function (data) {
                    if (data.attributes) {
                      _.each(data.entities, function (entity) {
                        that.processCollection.push(entity);
                      })
                    }

                    that.renderTableLayoutView();
                    that.$('.fontLoader').hide();
                    that.$('.tableOverlay').hide();
                  }
                })
              }

            },
            showLoader: function() {
                this.$('.fontLoader').show();
                this.$('.tableOverlay').show()
                this.modal.$el.find('button.ok').attr("disabled", true);
                this.$('.overlay').removeClass('hide').addClass('show');
            },
            hideLoader: function(argument) {
                this.$('.fontLoader').hide();
                this.$('.tableOverlay').hide();
                var buttonDisabled = options && options.buttonDisabled;
                this.modal.$el.find('button.ok').attr("disabled", buttonDisabled ? buttonDisabled : false);
                this.$('.overlay').removeClass('show').addClass('hide');
            },
            renderTableLayoutView: function() {
                this.ui.processName.html(this.nodeInfo.name);
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columnCollection = Backgrid.Columns.extend({}),
                        columns = new columnCollection(that.getProcessTableColumns());
                    that.RProcessTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: columns
                    })));
                    that.$('.multiSelectTag').hide();
                    Utils.generatePopover({
                        el: that.$('[data-id="showMoreLess"]'),
                        contentClass: 'popover-tag-term',
                        viewFixedPopover: true,
                        popoverOptions: {
                            container: null,
                            content: function() {
                                return $(this).find('.popup-tag-term').children().clone();
                            }
                        }
                    });
                });
            },
            getProcessTableColumns: function() {
                var that = this,
                    col = {
                      // id: {
                      //   label: "Id",
                      //   cell: "html",
                      //   editable: false,
                      //   formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                      //     fromRaw: function(rawValue, model) {
                      //       return model.get('attributes')["id"];
                      //     }
                      //   })
                      // },
                      startTime: {
                        label: "StartTime",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                          fromRaw: function(rawValue, model) {
                            if (model.get('attributes')["endTime"] && model.get(
                                'attributes')["durationMs"]) {
                              return new Date(model.get('attributes')["endTime"]
                                  - model.get('attributes')["durationMs"]);
                            } else {
                              return null;
                            }
                          }
                        })
                      },
                      duration: {
                        label: "DurationMs",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                          fromRaw: function (rawValue, model) {
                            if (model.get('attributes')["durationMs"]) {
                              return model.get('attributes')["durationMs"];
                            } else {
                              return null;
                            }
                          }
                        })
                      },
                      numOutputRows: {
                        label: "NumOutputRows",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                          fromRaw: function(rawValue, model) {
                            if (model.get('attributes')["writeMetrics"]) {
                              return model.get(
                                  'attributes')["writeMetrics"]["numOutputRows"];
                            } else if (model.get('attributes')["metrics"]) {
                              return model.get(
                                  'attributes')["metrics"]["write.numOutputRows"];
                            } else {
                              return null;
                            }
                          }
                        })
                      },
                      numOutputBytes: {
                        label: "numOutputBytes",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                          fromRaw: function(rawValue, model) {
                            if (model.get('attributes')["writeMetrics"]) {
                              return model.get(
                                  'attributes')["writeMetrics"]["numOutputBytes"];
                            } else if (model.get('attributes')["metrics"]) {
                              return model.get(
                                  'attributes')["metrics"]["write.numOutputBytes"];
                            } else {
                              return null;
                            }
                          }
                        })
                      },
                      readMetrics: {
                        label: "readMetrics",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                          fromRaw: function(rawValue, model) {
                            if(model.get('attributes')["readMetrics"])
                            return JSON.stringify(model.get('attributes')["readMetrics"]);
                            else {
                              var readStr = ""
                              _.each(_.keys(model.get('attributes')["metrics"]), function(key) {
                                if (!(key.lastIndexOf("write", 0) === 0)) {
                                  if (readStr.length > 0) {
                                    readStr = readStr + ", " + key + "=" + model.get('attributes')["metrics"][key]
                                  } else {
                                    readStr = key + "=" + model.get('attributes')["metrics"][key]
                                  }
                                }

                              })
                                return readStr;
                            }
                          }
                        })
                      }
                    }
                return this.processCollection.constructor.getTableCols(col, this.processCollection);
            }
        });
    return ProcessTableLayoutView;
});