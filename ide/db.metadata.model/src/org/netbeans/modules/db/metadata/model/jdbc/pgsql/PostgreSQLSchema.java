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
package org.netbeans.modules.db.metadata.model.jdbc.pgsql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.netbeans.modules.db.metadata.model.MetadataUtilities;
import org.netbeans.modules.db.metadata.model.api.MetadataException;
import org.netbeans.modules.db.metadata.model.api.Table;
import org.netbeans.modules.db.metadata.model.api.TableType;
import org.netbeans.modules.db.metadata.model.jdbc.JDBCCatalog;
import org.netbeans.modules.db.metadata.model.jdbc.JDBCSchema;
import org.netbeans.modules.db.metadata.model.jdbc.JDBCTable;

/**
 *
 * @author Andrei Badea
 */
public class PostgreSQLSchema extends JDBCSchema {

    private static final Logger LOGGER = Logger.getLogger(PostgreSQLSchema.class.getName());

    public PostgreSQLSchema(JDBCCatalog catalog, String name, boolean _default, boolean synthetic) {
        super(catalog, name, _default, synthetic);
    }

    @Override
    protected JDBCTable createJDBCTable(String name, Set<TableType> tableTypes) {
        return new PostgreSQLTable(getSchema(),this, name, tableTypes);
    }

    @Override
    public String toString() {
        return "PostgreSQLSchema[name='" + name + "',default=" + _default + ",synthetic=" + synthetic + "]"; // NOI18N
    }

    @Override
    protected void createTables() {
        LOGGER.log(Level.FINE, "Initializing tables in {0}", this);
        Map<String, Table> newTables = new LinkedHashMap<>();
        Set<String> partitionTables = new HashSet<>();
        Set<TableInfoHolder> tableInfoList = new HashSet<>();
        try {
            ResultSet rs = MetadataUtilities.getTables(jdbcCatalog.getJDBCMetadata().getDmd(),
                jdbcCatalog.getName(), name, "%", new String[]{"TABLE", "SYSTEM TABLE", "PARTITIONED TABLE"}); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        String type = MetadataUtilities.trimmed(rs.getString("TABLE_TYPE")); //NOI18N
                        String tableName = MetadataUtilities.trimmed(rs.getString("TABLE_NAME")); // NOI18N
                        Set<TableType> tableTypes = MetadataUtilities.parseTableType(type);
                        if (tableTypes.contains(TableType.PARTITIONED)) {
                            partitionTables.addAll(PostgreSQLUtils.getPartitionsForTable(
                                getJDBCCatalog().getJDBCMetadata().getConnection(),
                                tableName));
                        }
                        tableInfoList.add(new TableInfoHolder(tableName, tableTypes));
                    }
                } finally {
                    rs.close();
                }
                tableInfoList.forEach(t -> {
                    if (!partitionTables.contains(t.getTableName())) {
                        Table table = createJDBCTable(t.getTableName(), t.getTableTypes()).getTable(); //NOI18N
                        newTables.put(t.getTableName(), table);
                        LOGGER.log(Level.FINE, "Created table {0}", table); //NOI18N
                    }
                });
            }

        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        tables = Collections.unmodifiableMap(newTables);
    }

    private static class TableInfoHolder {

        private final String tableName;
        private final Set<TableType> tableTypes;

        public TableInfoHolder(String tableName, Set<TableType> tableTypes) {
            this.tableName = tableName;
            this.tableTypes = tableTypes;
        }

        public String getTableName() {
            return tableName;
        }

        public Set<TableType> getTableTypes() {
            return tableTypes;
        }

    }
}
