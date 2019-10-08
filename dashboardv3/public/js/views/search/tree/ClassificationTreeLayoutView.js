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
define([
    "require",
    "hbs!tmpl/search/tree/ClassificationTreeLayoutView_tmpl",
    "utils/Utils",
    "utils/Messages",
    "utils/Globals",
    "utils/UrlLinks",
    "utils/CommonViewFunction",
    "collection/VSearchList",
    "collection/VGlossaryList",
    "jstree"
], function(require, ClassificationTreeLayoutViewTmpl, Utils, Messages, Globals, UrlLinks, CommonViewFunction, VSearchList, VGlossaryList) {
    "use strict";

    var ClassificationTreeLayoutView = Marionette.LayoutView.extend({
        template: ClassificationTreeLayoutViewTmpl,

        regions: {},
        ui: {
            //refresh
            refreshTree: '[data-id="refreshTree"]',
            groupOrFlatTree: '[data-id="groupOrFlatTreeView"]',

            // menuItems: '.menu-items>ul>li',

            // tree el
            classificationSearchTree: '[data-id="classificationSearchTree"]',

            // Show/hide empty values in tree
            showEmptyClassifications: '[data-id="showEmptyClassifications"]',

            // Create
            createTag: '[data-id="createTag"]',
            wildCardClick: '[data-id="wildCardClick"]',
            wildCardSearch: '[data-id="wildCardSearch"]',
            wildCardValue: '[data-id="wildCardValue"]',
            clearWildCard: '[data-id="clearWildCard"]'
        },
        templateHelpers: function() {
            return {
                apiBaseUrl: UrlLinks.apiBaseUrl
            };
        },
        events: function() {
            var events = {},
                that = this;
            // refresh individual tree
            events["click " + this.ui.refreshTree] = function(e) {
                var type = $(e.currentTarget).data("type");
                e.stopPropagation();
                that.refresh({ type: type });
            };

            events["click " + this.ui.createTag] = function() {
                that.onClickCreateTag();
            };

            events["click " + this.ui.showEmptyClassifications] = function(e) {
                var getTreeData, displayText;
                e.stopPropagation();
                this.isEmptyClassification = !this.isEmptyClassification;
                this.classificationSwitchBtnUpdate();
            };

            events["click " + this.ui.groupOrFlatTree] = function(e) {
                var type = $(e.currentTarget).data("type");
                e.stopPropagation();
                this.isGroupView = !this.isGroupView;
                // this.ui.groupOrFlatTree.attr("data-original-title", (this.isGroupView ? "Show flat tree" : "Show group tree"));
                this.ui.groupOrFlatTree.tooltip('hide');
                // this.ui.groupOrFlatTree.find("i").toggleClass("group-tree-deactivate");
                this.ui.groupOrFlatTree.find("i").toggleClass("fa-sitemap fa-list-ul");
                this.ui.groupOrFlatTree.find("span").html(this.isGroupView ? "Show flat tree" : "Show group tree");
                that.ui[type + "SearchTree"].jstree(true).destroy();
                that.renderClassificationTree();
            };
            events["click " + this.ui.wildCardClick] = function(e) {
                e.stopPropagation();
            };
            events["click " + this.ui.wildCardSearch] = function(e) {
                e.stopPropagation();
                var tagValue = this.ui.wildCardValue.val();
                that.findSearchResult(tagValue);
            };
            events["click " + this.ui.wildCardValue] = function(e) {
                e.stopPropagation();
            }
            events["click " + this.ui.clearWildCard] = function(e) {
                e.stopPropagation();
                that.ui.wildCardValue.val("");
            }
            events["keyup " + this.ui.wildCardValue] = function(e) {
                e.stopPropagation();
                var code = e.which;
                if (this.ui.wildCardValue.val().length > 0) {
                    this.ui.clearWildCard.removeClass('hide-icon');
                } else {
                    this.ui.clearWildCard.addClass('hide-icon');
                }
                if (code == 13) {
                    var tagValue = this.ui.wildCardValue.val();
                    that.findSearchResult(tagValue);
                }
            };

            return events;
        },
        initialize: function(options) {
            this.options = options;
            _.extend(
                this,
                _.pick(
                    options,
                    "typeHeaders",
                    "searchVent",
                    "entityDefCollection",
                    "enumDefCollection",
                    "classificationDefCollection",
                    "searchTableColumns",
                    "searchTableFilters",
                    "metricCollection"
                )
            );
            this.bindEvents();
            this.entityCountObj = _.first(this.metricCollection.toJSON());
            this.isEmptyClassification = true;
            this.entityTreeData = {};
            this.tagId = null;
            this.isGroupView = true;
        },
        onRender: function() {
            this.renderClassificationTree();
            this.createClassificationAction();
            this.ui.clearWildCard.addClass('hide-icon');
        },
        bindEvents: function() {
            var that = this;
            this.listenTo(
                this.classificationDefCollection.fullCollection,
                "reset add remove",
                function() {
                    if (this.ui.classificationSearchTree.jstree(true)) {
                        that.ui.classificationSearchTree.jstree(true).refresh();
                    } else {
                        this.renderClassificationTree();
                    }
                },
                this
            );
            $('body').on('click', '.classificationPopoverOptions li', function(e) {
                that.$('.classificationPopover').popover('hide');
                that[$(this).find('a').data('fn') + "Classification"](e)
            });
            this.searchVent.on("Classification:Count:Update", function(options) {
                var opt = options || {};
                if (opt && !opt.metricData) {
                    that.metricCollection.fetch({
                        skipDefaultError: true,
                        complete: function() {
                            that.entityCountObj = _.first(that.metricCollection.toJSON());
                            that.classificationTreeUpdate = true;
                            that.ui.classificationSearchTree.jstree(true).refresh();
                        }
                    });
                } else {
                    that.entityCountObj = opt.metricData;
                    that.ui.classificationSearchTree.jstree(true).refresh();
                }
            });
        },
        findSearchResult: function(tagValue) {
            if (tagValue) {
                var params = {
                    searchType: "basic",
                    dslChecked: false
                };
                if (this.options.value) {
                    params["tag"] = tagValue;
                }
                var searchParam = _.extend({}, this.options.value, params);
                this.triggerSearch(searchParam);
            } else {
                Utils.notifyInfo({
                    content: "Search should not be empty!"
                });
                return;
            }

        },
        onSearchClassificationNode: function(showEmptyTag) {
            // on tree search by text, searches for all classification node, called by searchfilterBrowserLayoutView.js
            this.isEmptyClassification = showEmptyTag;
            this.classificationSwitchBtnUpdate();
        },
        classificationSwitchBtnUpdate: function() {
            // this.ui.showEmptyClassifications.attr("title", (this.isEmptyClassification ? "Show" : "Hide") + " unused classification");
            this.ui.showEmptyClassifications.attr("data-original-title", (this.isEmptyClassification ? "Show" : "Hide") + " unused classification");
            this.ui.showEmptyClassifications.tooltip('hide');
            this.ui.showEmptyClassifications.find("i").toggleClass("fa-toggle-on fa-toggle-off");
            this.ui.showEmptyClassifications.find("span").html((this.isEmptyClassification ? "Show" : "Hide") + " unused classification");
            this.ui.classificationSearchTree.jstree(true).refresh();
        },
        createClassificationAction: function() {
            var that = this;
            Utils.generatePopover({
                el: this.$el,
                contentClass: 'classificationPopoverOptions',
                popoverOptions: {
                    selector: '.classificationPopover',
                    content: function() {
                        var type = $(this).data('detail'),
                            liString = "<li><i class='fa fa-list-alt'></i><a href='javascript:void(0)' data-fn='onViewEdit'>View/Edit</a></li><li><i class='fa fa-trash-o'></i><a href='javascript:void(0)' data-fn='onDelete'>Delete</a></li><li><i class='fa fa-search'></i><a href='javascript:void(0)' data-fn='onSelectedSearch'>Search</a></li>"
                        return "<ul>" + liString + "</ul>";
                    }
                }
            });
        },
        renderClassificationTree: function() {
            this.generateSearchTree({
                $el: this.ui.classificationSearchTree
            });
        },
        manualRender: function(options) {
            var that = this;
            _.extend(this.options, options);
            if (this.options.value === undefined) {
                this.options.value = {};
            }
            if (!this.options.value.tag) {
                this.ui.classificationSearchTree.jstree(true).deselect_all();
                this.tagId = null;
            } else {
                var dataFound = this.classificationDefCollection.fullCollection.find(function(obj) {
                    return obj.get("name") === that.options.value.tag
                });
                if (dataFound) {
                    if ((this.tagId && this.tagId !== dataFound.get("guid")) || this.tagId === null) {
                        if (this.tagId) {
                            this.ui.classificationSearchTree.jstree(true).deselect_node(this.tagId);
                        }
                        this.fromManualRender = true;
                        this.tagId = dataFound.get("guid");
                        this.ui.classificationSearchTree.jstree(true).select_node(dataFound.get("guid"));
                    }
                }
            }
        },
        onNodeSelect: function(options) {
            if (this.classificationTreeUpdate) {
                this.classificationTreeUpdate = false;
                return;
            }
            var name, type, selectedNodeId, that = this;
            that.ui.wildCardValue.val("");
            if (options) {
                name = options.node.original.name;
                selectedNodeId = options.node.id;
            } else {
                name = this.options.value.type || this.options.value.tag;
            }
            var tagValue = null,
                params = {
                    searchType: "basic",
                    dslChecked: false
                };
            if (this.options.value) {
                if (this.options.value.tag) {
                    params["tag"] = this.options.value.tag;
                }

                if (this.options.value.isCF) {
                    this.options.value.isCF = null;
                }
                if (this.options.value.tagFilters) {
                    params["tagFilters"] = null;
                }
            }

            if (that.tagId != selectedNodeId) {
                that.tagId = selectedNodeId;
                tagValue = name;
                params['tag'] = tagValue;
            } else {
                that.options.value.tag = that.tagId = params["tag"] = null;
                that.ui.classificationSearchTree.jstree(true).deselect_all(true);
                if (!that.options.value.type && !that.options.value.tag && !that.options.value.term && !that.options.value.query) {
                    var defaultUrl = '#!/search';
                    that.onClassificationUpdate(defaultUrl);
                    return;
                }
            }
            var searchParam = _.extend({}, this.options.value, params);
            this.triggerSearch(searchParam);
        },
        triggerSearch: function(params, url) {
            var serachUrl = url ? url : '#!/search/searchResult';
            Utils.setUrl({
                url: serachUrl,
                urlParams: params,
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: true
            });
        },
        onClassificationUpdate: function(url) {
            Utils.setUrl({
                url: url,
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: true
            });
        },
        refresh: function(options) {
            var that = this,
                apiCount = 2,
                renderTree = function() {
                    if (apiCount === 0) {
                        that.classificationDefCollection.fullCollection.comparator = function(model) {
                            return model.get('name').toLowerCase();
                        };
                        that.classificationDefCollection.fullCollection.sort({ silent: true });
                        that.classificationTreeUpdate = true
                        that.ui.classificationSearchTree.jstree(true).refresh();
                    }
                };
            this.classificationDefCollection.fetch({
                skipDefaultError: true,
                silent: true,
                complete: function() {
                    --apiCount;
                    renderTree();
                }
            });
            this.metricCollection.fetch({
                skipDefaultError: true,
                complete: function() {
                    --apiCount;
                    that.entityCountObj = _.first(that.metricCollection.toJSON());
                    renderTree();
                }
            });
        },
        getClassificationTree: function(options) {
            var that = this,
                collection = (options && options.collection) || this.classificationDefCollection.fullCollection,
                listOfParents = [],
                listWithEmptyParents = [],
                listWithEmptyParentsFlatView = [],
                flatViewList = [],
                isSelectedChild = false,
                openClassificationNodesState = function(treeDate) {
                    // if (treeDate.length == 1) {
                    _.each(treeDate, function(model) {
                        model.state['opened'] = true;
                    })
                    // }
                },
                generateNode = function(nodeOptions, options, isChild) {
                    var nodeStructure = {
                        text: nodeOptions.name,
                        name: nodeOptions.name,
                        children: that.isGroupView ? getChildren({
                            children: isChild ? nodeOptions.model.subTypes : nodeOptions.model.get("subTypes"),
                            parent: isChild ? options.parentName : nodeOptions.name
                        }) : null,
                        type: isChild ? nodeOptions.children.get("category") : nodeOptions.model.get("category"),
                        id: isChild ? nodeOptions.children.get("guid") : nodeOptions.model.get("guid"),
                        icon: "fa fa-tag",
                        gType: "Classification",
                    }
                    return nodeStructure;
                },

                getChildren = function(options) {
                    var children = options.children,
                        data = [],
                        dataWithoutEmptyTag = [];
                    if (children && children.length) {
                        _.each(children, function(name) {
                            var child = collection.find({
                                name: name
                            });
                            var tagEntityCount = that.entityCountObj.tag.tagEntities[name];
                            var tagname = tagEntityCount ? name + " (" + _.numberFormatWithComa(tagEntityCount) + ")" : name;

                            if (that.options.value) {
                                isSelectedChild = that.options.value.tag ? that.options.value.tag == name : false;
                                if (!that.tagId) {
                                    that.tagId = isSelectedChild ? child.get("guid") : null;
                                }
                            }
                            var modelJSON = child.toJSON();
                            if (child) {
                                var nodeDetails = {
                                        name: name,
                                        model: modelJSON,
                                        children: child,
                                        isSelectedChild: isSelectedChild
                                    },
                                    nodeProperties = {
                                        parent: options.parentName,
                                        text: tagname,
                                        guid: child.get("guid"),
                                        model: child,
                                        state: { selected: isSelectedChild, opened: true }
                                    },
                                    isChild = true,
                                    getNodeDetails = generateNode(nodeDetails, options, isChild),
                                    classificationNode = (_.extend(getNodeDetails, nodeProperties));
                                data.push(classificationNode);
                                if (that.isEmptyClassification) {
                                    var isTagEntityCount = _.isNaN(tagEntityCount) ? 0 : tagEntityCount;
                                    if (isTagEntityCount) {
                                        dataWithoutEmptyTag.push(classificationNode);
                                    }
                                }
                            }
                        });
                    }
                    var tagData = that.isEmptyClassification ? dataWithoutEmptyTag : data;
                    return tagData;
                }
            collection.each(function(model) {
                var modelJSON = model.toJSON();
                var name = modelJSON.name;
                var tagEntityCount = that.entityCountObj.tag.tagEntities[name];
                var tagname = tagEntityCount ? name + " (" + _.numberFormatWithComa(tagEntityCount) + ")" : name,
                    isSelected = false;

                if (that.options.value) {
                    isSelected = that.options.value.tag ? that.options.value.tag == name : false;
                    if (!that.tagId) {
                        that.tagId = isSelected ? model.get("guid") : null;
                    }
                }
                var parentNodeDetails = {
                        name: name,
                        model: model,
                        isSelectedChild: isSelectedChild
                    },
                    parentNodeProperties = {
                        text: tagname,
                        state: {
                            disabled: tagEntityCount == 0 ? true : false,
                            selected: isSelected,
                            opened: true
                        }
                    },
                    isChild = false,
                    getParentNodeDetails,
                    classificationParentNode, getParentFlatView, classificationParentFlatView;
                if (modelJSON.superTypes.length == 0) {
                    getParentNodeDetails = generateNode(parentNodeDetails, model, isChild);
                    classificationParentNode = (_.extend(getParentNodeDetails, parentNodeProperties));
                    listOfParents.push(classificationParentNode);
                }
                getParentFlatView = generateNode(parentNodeDetails, model);
                classificationParentFlatView = (_.extend(getParentFlatView, parentNodeProperties));
                flatViewList.push(classificationParentFlatView);
                if (that.isEmptyClassification) {
                    var isTagEntityCount = _.isNaN(tagEntityCount) ? 0 : tagEntityCount;
                    if (isTagEntityCount) {
                        if (modelJSON.superTypes.length == 0) {
                            listWithEmptyParents.push(classificationParentNode);
                        }
                        listWithEmptyParentsFlatView.push(classificationParentFlatView);
                    }

                }
            });
            var classificationTreeData = that.isEmptyClassification ? listWithEmptyParents : listOfParents;
            var flatViewClassificaton = that.isEmptyClassification ? listWithEmptyParentsFlatView : flatViewList;
            var classificationData = that.isGroupView ? classificationTreeData : flatViewClassificaton;
            // openClassificationNodesState(classificationData);
            return classificationData;
        },
        generateSearchTree: function(options) {
            var $el = options && options.$el,
                type = options && options.type,
                that = this,
                getEntityTreeConfig = function(opt) {
                    return {
                        plugins: ["search", "core", "sort", "conditionalselect", "changed", "wholerow", "node_customize"],
                        conditionalselect: function(node) {
                            var type = node.original.type;
                            if (type == "ENTITY" || type == "GLOSSARY") {
                                if (node.children.length || type == "GLOSSARY") {
                                    return false;
                                } else {
                                    return true;
                                }
                            } else {
                                return true;
                            }
                        },
                        state: { opened: true },
                        search: {
                            show_only_matches: true,
                            case_sensitive: false
                        },
                        node_customize: {
                            default: function(el) {
                                // if ($(el).find(".fa-ellipsis-h").length === 0) {
                                $(el).append('<div class="tools"><i class="fa fa-ellipsis-h classificationPopover" rel="popover"></i></div>');
                                // }
                            }
                        },
                        core: {
                            multiple: false,
                            data: function(node, cb) {
                                if (node.id === "#") {
                                    cb(that.getClassificationTree());
                                }
                            }
                        }
                    };
                };

            $el.jstree(
                getEntityTreeConfig({
                    type: ""
                })
            ).on("open_node.jstree", function(e, data) {
                that.isTreeOpen = true;
            }).on("select_node.jstree", function(e, data) {
                if (that.fromManualRender !== true) {
                    that.onNodeSelect(data);
                } else {
                    that.fromManualRender = false;
                }
            }).on("search.jstree", function(nodes, str, res) {
                if (str.nodes.length === 0) {
                    $el.jstree(true).hide_all();
                    $el.parents(".panel").addClass("hide");
                } else {
                    $el.parents(".panel").removeClass("hide");
                }
            })
        },
        onClickCreateTag: function(e) {
            var that = this;
            //nodeName = that.options.value && that.options.value.tag;
            require(["views/tag/CreateTagLayoutView", "modules/Modal"], function(CreateTagLayoutView, Modal) {

                // var name = !(nodeName == "BUTTON") ? that.query[that.viewType].tagName : null;
                //var name = nodeName;

                var view = new CreateTagLayoutView({ tagCollection: that.options.classificationDefCollection, enumDefCollection: enumDefCollection }),
                    modal = new Modal({
                        title: "Create a new classification",
                        content: view,
                        cancelText: "Cancel",
                        okCloses: false,
                        okText: "Create",
                        allowCancel: true
                    }).open();
                modal.$el.find("button.ok").attr("disabled", "true");
                view.ui.tagName.on("keyup", function(e) {
                    modal.$el.find("button.ok").removeAttr("disabled");
                });
                view.ui.tagName.on("keyup", function(e) {
                    if ((e.keyCode == 8 || e.keyCode == 32 || e.keyCode == 46) && e.currentTarget.value.trim() == "") {
                        modal.$el.find("button.ok").attr("disabled", "true");
                    }
                });
                modal.on("shownModal", function() {
                    view.ui.parentTag.select2({
                        multiple: true,
                        placeholder: "Search Classification",
                        allowClear: true
                    });
                });
                modal.on("ok", function() {
                    modal.$el.find("button.ok").attr("disabled", "true");
                    that.onCreateTagButton(view, modal);
                });
                modal.on("closeModal", function() {
                    modal.trigger("cancel");
                    // that.ui.createTag.removeAttr("disabled");
                });
            });
        },
        onCreateTagButton: function(ref, modal) {
            var that = this;
            var validate = true;
            if (modal.$el.find(".attributeInput").length > 0) {
                modal.$el.find(".attributeInput").each(function() {
                    if ($(this).val() === "") {
                        $(this).css("borderColor", "red");
                        validate = false;
                    }
                });
            }
            modal.$el.find(".attributeInput").keyup(function() {
                $(this).css("borderColor", "#e8e9ee");
                modal.$el.find("button.ok").removeAttr("disabled");
            });
            if (!validate) {
                Utils.notifyInfo({
                    content: "Please fill the attributes or delete the input box"
                });
                return;
            }

            var name = ref.ui.tagName.val(),
                description = ref.ui.description.val(),
                superTypes = [],
                parentTagVal = ref.ui.parentTag.val();
            if (parentTagVal && parentTagVal.length) {
                superTypes = parentTagVal;
            }
            var attributeObj = ref.collection.toJSON();
            if (ref.collection.length === 1 && ref.collection.first().get("name") === "") {
                attributeObj = [];
            }

            if (attributeObj.length) {
                var superTypesAttributes = [];
                _.each(superTypes, function(name) {
                    var parentTags = that.options.classificationDefCollection.fullCollection.findWhere({ name: name });
                    superTypesAttributes = superTypesAttributes.concat(parentTags.get("attributeDefs"));
                });

                var duplicateAttributeList = [];
                _.each(attributeObj, function(obj) {
                    var duplicateCheck = _.find(superTypesAttributes, function(activeTagObj) {
                        return activeTagObj.name.toLowerCase() === obj.name.toLowerCase();
                    });
                    if (duplicateCheck) {
                        duplicateAttributeList.push(obj.name);
                    }
                });
                var notifyObj = {
                    modal: true,
                    confirm: {
                        confirm: true,
                        buttons: [{
                                text: "Ok",
                                addClass: "btn-atlas btn-md",
                                click: function(notice) {
                                    notice.remove();
                                }
                            },
                            null
                        ]
                    }
                };
                if (duplicateAttributeList.length) {
                    if (duplicateAttributeList.length < 2) {
                        var text = "Attribute <b>" + duplicateAttributeList.join(",") + "</b> is duplicate !";
                    } else {
                        if (attributeObj.length > duplicateAttributeList.length) {
                            var text = "Attributes: <b>" + duplicateAttributeList.join(",") + "</b> are duplicate !";
                        } else {
                            var text = "All attributes are duplicate !";
                        }
                    }
                    notifyObj["text"] = text;
                    Utils.notifyConfirm(notifyObj);
                    return false;
                }
            }
            this.json = {
                classificationDefs: [{
                    name: name.trim(),
                    description: description.trim(),
                    superTypes: superTypes.length ? superTypes : [],
                    attributeDefs: attributeObj
                }],
                entityDefs: [],
                enumDefs: [],
                structDefs: []
            };
            new this.options.classificationDefCollection.model().set(this.json).save(null, {
                success: function(model, response) {
                    var classificationDefs = model.get("classificationDefs");
                    // that.ui.createTag.removeAttr("disabled");
                    that.createTag = true;
                    if (classificationDefs[0]) {
                        _.each(classificationDefs[0].superTypes, function(superType) {
                            var superTypeModel = that.options.classificationDefCollection.fullCollection.find({ name: superType }),
                                subTypes = [];
                            if (superTypeModel) {
                                subTypes = superTypeModel.get("subTypes");
                                subTypes.push(classificationDefs[0].name);
                                superTypeModel.set({ subTypes: _.uniq(subTypes) });
                            }
                        });
                    }
                    that.options.classificationDefCollection.fullCollection.add(classificationDefs);
                    Utils.notifySuccess({
                        content: "Classification " + name + Messages.addSuccessMessage
                    });
                    modal.trigger("cancel");
                    that.typeHeaders.fetch({ reset: true });
                    var url = "#!/tag/tagAttribute/" + name;
                    this.onClassificationUpdate(url);
                }
            });
        },
        onViewEditClassification: function() {
            var selectedNode = this.ui.classificationSearchTree.jstree("get_selected", true);
            if (selectedNode && selectedNode[0]) {
                var url = "#!/tag/tagAttribute/" + selectedNode[0].original.name;
                this.onClassificationUpdate(url);
            }
        },
        onDeleteClassification: function() {
            var that = this,
                notifyObj = {
                    modal: true,
                    ok: function(argument) {
                        that.onNotifyOk();
                    },
                    cancel: function(argument) {}
                };
            var text = "Are you sure you want to delete the classification";
            notifyObj["text"] = text;
            Utils.notifyConfirm(notifyObj);
        },
        onSelectedSearchClassification: function() {
            var params = {
                searchType: "basic",
                dslChecked: false,
                tag: this.options.value.tag
            };
            this.triggerSearch(params);
        },
        onNotifyOk: function(data) {
            var that = this;
            if (this.tagId) {
                var deleteTagData = this.classificationDefCollection.fullCollection.findWhere({ guid: this.tagId });
                if (deleteTagData) {
                    var tagName = deleteTagData.get("name");
                    deleteTagData.deleteTag({
                        typeName: tagName,
                        success: function() {
                            Utils.notifySuccess({
                                content: "Classification " + tagName + Messages.deleteSuccessMessage
                            });
                            // if deleted tag is prviously searched then remove that tag url from save state of tab.
                            var searchUrl = Globals.saveApplicationState.tabState.searchUrl;
                            var urlObj = Utils.getUrlState.getQueryParams(searchUrl);
                            that.classificationDefCollection.fullCollection.remove(deleteTagData);
                            // to update tag list of search tab fetch typeHeaders.
                            //that.typeHeaders.fetch({ reset: true });
                            that.ui.classificationSearchTree.jstree(true).refresh();
                            delete urlObj.tag;
                            var url = urlObj.type || urlObj.term || urlObj.query ? "#!/search/searchResult" : "#!/search"
                            that.triggerSearch(urlObj, url);
                        }
                    });
                } else {
                    Utils.notifyError({
                        content: Messages.defaultErrorMessage
                    });
                }
            }
        }

    });
    return ClassificationTreeLayoutView;
});