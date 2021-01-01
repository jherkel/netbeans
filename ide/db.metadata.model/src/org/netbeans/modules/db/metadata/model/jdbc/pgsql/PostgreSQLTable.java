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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.netbeans.modules.db.metadata.model.MetadataUtilities;
import org.netbeans.modules.db.metadata.model.api.MetadataElement;
import org.netbeans.modules.db.metadata.model.api.MetadataException;
import org.netbeans.modules.db.metadata.model.api.Table;
import org.netbeans.modules.db.metadata.model.api.TableType;
import org.netbeans.modules.db.metadata.model.jdbc.JDBCSchema;
import org.netbeans.modules.db.metadata.model.jdbc.JDBCTable;

/**
 *
 * @author jakub
 */
public class PostgreSQLTable extends JDBCTable {

    private static final Logger LOGGER = Logger.getLogger(PostgreSQLTable.class.getName());

    public PostgreSQLTable(MetadataElement parent,JDBCSchema jdbcSchema, String name, Set<TableType> tableType) {
        super(parent,jdbcSchema, name, tableType);
    }

    @Override
    protected void createPartitions() {
        Map<String, Table> newPartitions = new LinkedHashMap<>();
        if (getTableTypes().contains(TableType.PARTITIONED)) {
            // check only partitioned tables
            try {
                int dbVersion = jdbcSchema.getJDBCCatalog().getJDBCMetadata().getDmd()
                    .getDatabaseMajorVersion();
                if (dbVersion >= 10) {
                    Collection<String> partitionTables = PostgreSQLUtils.getPartitionsForTable(
                        jdbcSchema.getJDBCCatalog().getJDBCMetadata().getConnection(), getName());
                    partitionTables.forEach(tableName -> {
                        Table table = createPartitionTable(tableName);
                        newPartitions.put(tableName, table);
                    });
                }
            } catch (SQLException e) {
                filterSQLException(e);
            }
        }
        partitions = Collections.unmodifiableMap(newPartitions);
    }

    private Table createPartitionTable(String name) {
        try {
            ResultSet rs = MetadataUtilities.getTables(jdbcSchema.getJDBCCatalog().getJDBCMetadata().getDmd(),
                jdbcSchema.getJDBCCatalog().getName(), jdbcSchema.getName(), name, new String[]{"TABLE", "PARTITIONED TABLE"}); // NOI18N
            if (rs != null && rs.next()) {
                try {
                    String type = MetadataUtilities.trimmed(rs.getString("TABLE_TYPE")); //NOI18N
                    String tableName = MetadataUtilities.trimmed(rs.getString("TABLE_NAME")); // NOI18N
                    Set<TableType> tableTypes = MetadataUtilities.parseTableType(type);
                    tableTypes.add(TableType.PARTITION);
                    Table table = new PostgreSQLTable(getTable(),jdbcSchema, tableName, tableTypes).getTable();
                    LOGGER.log(Level.FINE, "Created partition {0}", table); //NOI18N
                    return table;
                } finally {
                    rs.close();
                }
            } else {
                throw new MetadataException("Cannot inspect partition : " + name);

            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
    }

}
