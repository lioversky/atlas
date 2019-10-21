/*
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
    'hbs!tmpl/entity/EntityUserDefineItemView_tmpl'

], function(require, Backbone, EntityUserDefineItemView_tmpl) {
    'use strict';

    return Backbone.Marionette.ItemView.extend({
        _viewName: 'EntityUserDefineItemView',

        template: EntityUserDefineItemView_tmpl,

        templateHelpers: function() {
            return {
                items: this.items
            };
        },

        /** Layout sub regions */
        regions: {},

        /** ui selector cache */
        ui: {
            itemKey: "[data-type='key']",
            itemValue: "[data-type='value']",
            addItem: "[data-id='addItem']",
            deleteItem: "[data-id='deleteItem']"
        },
        /** ui events hash */
        events: function() {
            var events = {};
            events['input ' + this.ui.itemKey] = 'onItemKeyChange';
            events['input ' + this.ui.itemValue] = 'onItemValueChange';
            events['click ' + this.ui.addItem] = 'onAddItemClick';
            events['click ' + this.ui.deleteItem] = 'onDeleteItemClick';
            return events;
        },

        /**
         * intialize a new GlobalExclusionComponentView Layout
         * @constructs
         */
        initialize: function(options) {
            var that = this;
            this.editMode = options.mode;
            if (options.items.length === 0) {
                this.items = [{ key: "", value: "", mode: this.editMode}];

            } else {
                this.items = options.items.map(function(m) {
                    m.mode = that.editMode;
                    return m;
                });
            }
        },
        onRender: function() {

        },
        onAddItemClick: function(e) {
            var el = e.currentTarget;
            this.items.splice(parseInt(el.dataset.index) + 1, 0, { key: "", value: "", mode: this.editMode});
            this.render();
        },
        onDeleteItemClick: function(e) {
            var el = e.currentTarget;
            this.items.splice(el.dataset.index, 1);
            this.render();
        },
        onItemKeyChange: function (e) {
            var el = e.currentTarget;
            var val = el.value;
            this.items[ el.dataset.index].key = val;
        },
        onItemValueChange: function (e) {
            var el = e.currentTarget;
            var val = el.value;
            this.items[ el.dataset.index].value = el.value;
        }
    });

});
