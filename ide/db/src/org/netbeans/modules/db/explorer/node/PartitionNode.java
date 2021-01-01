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

import java.awt.datatransfer.Transferable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.netbeans.api.db.explorer.DatabaseMetaDataTransfer;
import org.netbeans.api.db.explorer.node.BaseNode;
import org.netbeans.api.db.explorer.node.ChildNodeFactory;
import org.netbeans.api.db.explorer.node.NodeProvider;
import org.netbeans.modules.db.explorer.DatabaseConnection;
import org.netbeans.modules.db.explorer.DatabaseMetaDataTransferAccessor;
import org.netbeans.modules.db.metadata.model.api.Action;
import org.netbeans.modules.db.metadata.model.api.Metadata;
import org.netbeans.modules.db.metadata.model.api.MetadataElementHandle;
import org.netbeans.modules.db.metadata.model.api.MetadataModel;
import org.netbeans.modules.db.metadata.model.api.MetadataModelException;
import org.netbeans.modules.db.metadata.model.api.Table;
import org.netbeans.modules.db.metadata.model.api.TableType;
import org.openide.nodes.Node;
import org.openide.nodes.PropertySupport;
import org.openide.util.HelpCtx;
import org.openide.util.NbBundle;
import org.openide.util.datatransfer.ExTransferable;

/**
 *
 * @author Jakub Herkel
 */
public class PartitionNode extends BaseNode implements SchemaNameProvider {

    private static final String ICONBASE_PARTITIONED = "org/netbeans/modules/db/resources/partition_partitioned.gif"; // NOI18N
    private static final String ICONBASE = "org/netbeans/modules/db/resources/partition.gif"; // NOI18N
    private static final String FOLDER = "Table"; //NOI18N
    private static final Map<Node, Object> NODES_TO_REFRESH
        = new WeakHashMap<Node, Object>();

    /**
     * Create an instance of TableNode.
     *
     * @param dataLookup the lookup to use when creating node providers
     * @return the TableNode instance
     */
    public static PartitionNode create(NodeDataLookup dataLookup, NodeProvider provider) {
        PartitionNode node = new PartitionNode(dataLookup, provider);
        node.setup();
        return node;
    }

    private String name = ""; // NOI18N
    private Set<TableType> tableTypes = Collections.emptySet();
    private final MetadataElementHandle<Table> tableHandle;
    private final DatabaseConnection connection;

    @SuppressWarnings("unchecked")
    private PartitionNode(NodeDataLookup lookup, NodeProvider provider) {
        super(new ChildNodeFactory(lookup), lookup, FOLDER, provider);
        connection = getLookup().lookup(DatabaseConnection.class);
        tableHandle = getLookup().lookup(MetadataElementHandle.class);
    }

    @Override
    protected void initialize() {
        boolean connected = connection.isConnected();
        MetadataModel metaDataModel = connection.getMetadataModel();
        if (connected && metaDataModel != null) {
            try {
                metaDataModel.runReadAction(new Action<Metadata>() {
                    @Override
                    public void run(Metadata metaData) {
                        Table table = tableHandle.resolve(metaData);
                        if (table == null) {
                            Logger.getLogger(PartitionNode.class.getName()).log(Level.INFO, "Cannot get table name for " + tableHandle);
                            return;
                        }
                        name = table.getName();
                        tableTypes = table.getTableTypes();
                        updateProperties(table);
                    }
                }
                );
            } catch (MetadataModelException e) {
                NodeRegistry.handleMetadataModelException(this.getClass(), connection, e, true);
            }

        }
    }

    private void updateProperties(Table table) {
        PropertySupport.Name ps = new PropertySupport.Name(PartitionNode.this);
        addProperty(ps);

        addProperty(CATALOG, CATALOGDESC, String.class, false, getCatalogName());
        addProperty(SCHEMA, SCHEMADESC, String.class, false, getSchemaName());
    }

    public MetadataElementHandle<Table> getTableHandle() {
        return tableHandle;
    }

    @Override
    public String getCatalogName() {
        return getCatalogName(connection, tableHandle);
    }

    @Override
    public String getSchemaName() {
        return getSchemaName(connection, tableHandle);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDisplayName() {
        return getName();
    }

    @Override
    public String getIconBase() {
        if (tableTypes.contains(TableType.PARTITIONED)) {
            return ICONBASE_PARTITIONED;
        } else {
            return ICONBASE;
        }
    }

    public boolean isSystem() {
        return tableTypes.contains(TableType.SYSTEM);
    }

    @Override
    public String getShortDescription() {
        return NbBundle.getMessage(PartitionNode.class, "ND_Table"); //NOI18N
    }

    @Override
    public HelpCtx getHelpCtx() {
        return new HelpCtx(PartitionNode.class);
    }

    @Override
    public boolean canCopy() {
        return true;
    }

    @Override
    public Transferable clipboardCopy() throws IOException {
        ExTransferable result = ExTransferable.create(super.clipboardCopy());
        result.put(new ExTransferable.Single(DatabaseMetaDataTransfer.TABLE_FLAVOR) {
            @Override
            protected Object getData() {
                return DatabaseMetaDataTransferAccessor.DEFAULT.createTableData(connection.getDatabaseConnection(),
                    connection.findJDBCDriver(), getName());
            }
        });
        return result;
    }

    public static String getSchemaName(DatabaseConnection connection, final MetadataElementHandle<Table> handle) {
        MetadataModel metaDataModel = connection.getMetadataModel();
        final String[] array = new String[1];

        try {
            metaDataModel.runReadAction(
                new Action<Metadata>() {
                @Override
                public void run(Metadata metaData) {
                    Table table = handle.resolve(metaData);
                    if (table != null && table.getParent() != null) {
                        array[0] = table.getParent().getName();
                    }
                }
            }
            );
        } catch (MetadataModelException e) {
            NodeRegistry.handleMetadataModelException(PartitionNode.class, connection, e, true);
        }

        return array[0];
    }

    public static String getCatalogName(DatabaseConnection connection, final MetadataElementHandle<Table> handle) {
        MetadataModel metaDataModel = connection.getMetadataModel();
        final String[] array = new String[1];

        try {
            metaDataModel.runReadAction(
                new Action<Metadata>() {
                @Override
                public void run(Metadata metaData) {
                    Table table = handle.resolve(metaData);
                    if (table != null && table.getParent() != null) {
                        array[0] = table.getParent().getParent().getName();
                    }
                }
            }
            );
        } catch (MetadataModelException e) {
            NodeRegistry.handleMetadataModelException(PartitionNode.class, connection, e, true);
        }

        return array[0];
    }
}
