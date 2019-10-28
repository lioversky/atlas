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
    'collection/VEntityList',
    'modules/Modal',
    'utils/Utils',
    'utils/CommonViewFunction',
    'utils/Messages',
    'utils/Globals',
    'utils/Enums',
    'utils/UrlLinks'
], function(require, Backbone, ProcessTableLayoutViewTmpl, VProcessList, VEntityList, Modal, Utils, CommonViewFunction, Messages, Globals, Enums, UrlLinks) {
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
                tagClick: '[data-id="tagClick"]',
                addTag: "[data-id='addTag']",
                addAssignTag: "[data-id='addAssignTag']",
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
                this.ui.processName.html(fromEntity.displayText + " <span class='navigation-font'><i class='fa fa-long-arrow-right fa-color'></i></span> " + this.nodeInfo.name);

                this.processCollection = new VEntityList();
                this.processTableAttribute = ["id", "startTime", "duration",
                "numOutputRows", "numOutputBytes", "readMetrics"];
                this.processCollection.url = UrlLinks.entityCollectionaudit(this.guid);
                this.processCollection.modelAttrName = "events";
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
              this.modal.$el.find('button.ok').remove();
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
              this.$('.fontLoader').show();
              this.$('.tableOverlay').show();

              this.processCollection.fetch({
                success: function() {

                  that.processCollection.sort();
                  that.renderTableLayoutView();
                  that.$('.fontLoader').hide();
                  that.$('.tableOverlay').hide();
                },
                silent: true
              });
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
                      user: {
                        label: "Users",
                        cell: "html",
                        editable: false,
                      },
                      timestamp: {
                        label: "Timestamp",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                          fromRaw: function(rawValue, model) {
                            return new Date(rawValue);
                          }
                        })
                      },
                      action: {
                        label: "Actions",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                          fromRaw: function(rawValue, model) {
                            if (Enums.auditAction[rawValue]) {
                              return Enums.auditAction[rawValue];
                            } else {
                              return rawValue;
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