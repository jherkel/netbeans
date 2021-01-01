/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.netbeans.modules.db.explorer.node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.netbeans.api.db.explorer.node.NodeProvider;
import org.netbeans.api.db.explorer.node.NodeProviderFactory;
import org.netbeans.modules.db.explorer.DatabaseConnection;
import org.netbeans.modules.db.metadata.model.api.Action;
import org.netbeans.modules.db.metadata.model.api.Metadata;
import org.netbeans.modules.db.metadata.model.api.MetadataElementHandle;
import org.netbeans.modules.db.metadata.model.api.MetadataModel;
import org.netbeans.modules.db.metadata.model.api.MetadataModelException;
import org.netbeans.modules.db.metadata.model.api.Table;
import org.openide.nodes.Node;
import org.openide.util.Lookup;

/**
 *
 * @author Jakub Herkel
 */
public class PartitionNodeProvider extends NodeProvider {

    // lazy initialization holder class idiom for static fields is used
    // for retrieving the factory
    public static NodeProviderFactory getFactory() {
        return FactoryHolder.FACTORY;
    }

    private static class FactoryHolder {

        static final NodeProviderFactory FACTORY = new NodeProviderFactory() {
            @Override
            public PartitionNodeProvider createInstance(Lookup lookup) {
                PartitionNodeProvider provider = new PartitionNodeProvider(lookup, false);
                return provider;
            }
        };
    }

    private final DatabaseConnection connection;
    private final boolean system;
    private MetadataElementHandle<Table> tableHandle;

    @SuppressWarnings("unchecked")
    private PartitionNodeProvider(Lookup lookup, boolean system) {
        super(lookup, tableComparator);
        this.system = system;
        connection = getLookup().lookup(DatabaseConnection.class);
        tableHandle = getLookup().lookup(MetadataElementHandle.class);
    }

    @Override
    protected void initialize() {

        final List<Node> newList = new ArrayList<>();

        boolean connected = connection.isConnected();
        MetadataModel metaDataModel = connection.getMetadataModel();
        if (connected && metaDataModel != null) {
            try {
                metaDataModel.runReadAction(new Action<Metadata>() {
                    @Override
                    public void run(Metadata metaData) {
                        Table key = tableHandle.resolve(metaData);
                        if (key != null) {
                            Collection<Table> partitions = key.getPartitions();
                            for (Table partition : partitions) {
                                MetadataElementHandle<Table> h = MetadataElementHandle.create(partition);
                                Collection<Node> matches = getNodes(h);
                                if (matches.size() > 0) {
                                    newList.addAll(matches);
                                } else {
                                    NodeDataLookup lookup = new NodeDataLookup();
                                    lookup.add(connection);
                                    lookup.add(h);

                                    newList.add(PartitionNode.create(lookup, PartitionNodeProvider.this));
                                }
                            }
                        }
                    }
                }
                );
            } catch (MetadataModelException e) {
                NodeRegistry.handleMetadataModelException(this.getClass(), connection, e, true);
            }
        }

        setNodes(newList);
    }

    private static final Comparator<Node> tableComparator = new Comparator<Node>() {

        @Override
        public int compare(Node node1, Node node2) {
            return node1.getDisplayName().compareTo(node2.getDisplayName());
        }

    };
}
