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

package org.netbeans.modules.db.metadata.model.spi;

import java.util.Collection;
import java.util.Set;
import org.netbeans.modules.db.metadata.model.MetadataAccessor;
import org.netbeans.modules.db.metadata.model.api.Column;
import org.netbeans.modules.db.metadata.model.api.ForeignKey;
import org.netbeans.modules.db.metadata.model.api.Index;
import org.netbeans.modules.db.metadata.model.api.MetadataElement;
import org.netbeans.modules.db.metadata.model.api.PrimaryKey;
import org.netbeans.modules.db.metadata.model.api.Table;
import org.netbeans.modules.db.metadata.model.api.TableType;

/**
 *
 * @author Andrei Badea
 */
public abstract class TableImplementation {

    private Table table;

    public final Table getTable() {
        if (table == null) {
            table = MetadataAccessor.getDefault().createTable(this);
        }
        return table;
    }

    public abstract MetadataElement getParent();

    public abstract String getName();

    public abstract Collection<Column> getColumns();

    public abstract Collection<Table> getPartitions();

    public abstract Table getPartition(String name);

    public abstract Column getColumn(String name);

    public abstract PrimaryKey getPrimaryKey();

    public abstract Index getIndex(String name);

    public abstract Collection<Index> getIndexes();

    public abstract Collection<ForeignKey> getForeignKeys();

    public abstract ForeignKey getForeignKeyByInternalName(String internalName);

    public abstract void refresh();

    public abstract Set<TableType> getTableTypes();
    
    @Deprecated
    public abstract boolean isSystem();
    
}
