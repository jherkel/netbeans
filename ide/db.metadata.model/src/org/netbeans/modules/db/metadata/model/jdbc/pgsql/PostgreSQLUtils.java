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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author jakub herkel
 */
public final class PostgreSQLUtils {

    private PostgreSQLUtils() {
    }

    public static Collection<String> getPartitionsForTable(Connection connection, String tableName) throws SQLException {
        String psql = "SELECT\n"
            + "    nmsp_parent.nspname AS parent_schema,\n"
            + "    parent.relname      AS parent,\n"
            + "    nmsp_child.nspname  AS child_schema,\n"
            + "    child.relname       AS child\n"
            + "FROM pg_inherits\n"
            + "    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid\n"
            + "    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid\n"
            + "    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace\n"
            + "    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace\n"
            + "WHERE parent.relname='" + tableName + "'";
        List<String> ret = new ArrayList<>();
        try ( Statement s = connection.createStatement()) {
            try ( ResultSet rs = s.executeQuery(psql)) {
                while (rs.next()) {
                    ret.add(rs.getString("child"));
                }
            }
        }
        return ret;
    }

}
