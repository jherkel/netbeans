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
package org.netbeans.modules.db.metadata.model.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.openide.util.Parameters;

/**
 * Represents the handle of a metadata element.
 *
 * <p>
 * Metadata elements cannot escape the {@link MetadataModel#runReadAction} method. Handles can be
 * used to pass information about metadata elements out of this method. The handle can be
 * {@link #resolve resolved} to the corresponding metadata element in another {@code runReadAction}
 * method.</p>
 *
 * @param <T> the type of the metadata element that this handle was created for.
 *
 * @author Andrei Badea
 */
public class MetadataElementHandle<T extends MetadataElement> {

    // The hierarchy of elements (e.g. [CATALOG,SCHEMA,TABLE,COLUMN] and ["mycatalog","myschema","mytable","mycolumn"])
    //
    // It is the combination of the hierarchy of names and kinds that uniquely identifies this
    // element in the metadata model.
    private final List<HierarchyElement> elements;

    /**
     * Creates a handle for a metadata element.
     *
     * @param <T> the type of the metadata element to create this handle for.
     * @param element a metadata element.
     * @return the handle for the given metadata element.
     */
    public static <T extends MetadataElement> MetadataElementHandle<T> create(T element) {
        Parameters.notNull("element", element);
        List<HierarchyElement> elements = new ArrayList<>();
        MetadataElement current = element;
        while (current != null) {
            elements.add(new HierarchyElement(Kind.of(current),current.getInternalName()));
            current = current.getParent();
        }
        Collections.reverse(elements);
        return new MetadataElementHandle<>(elements);
    }

    // For use in unit tests.
    static <T extends MetadataElement> MetadataElementHandle<T> create(Class<T> clazz, List<HierarchyElement> elements) {
        return new MetadataElementHandle<>(elements);
    }

    private MetadataElementHandle(List<HierarchyElement> elements) {
        this.elements = elements;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MetadataElementHandle<?> other = (MetadataElementHandle<?>) obj;
        if (!Objects.equals(this.elements, other.elements)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 71 * hash + Objects.hashCode(this.elements);
        return hash;
    }


    /**
     * Resolves this handle to the corresponding metadata element, if any.
     *
     * @param metadata the {@link Metadata} instance to resolve this element against.
     * @return the corresponding metadata element or null if it could not be found (for example
     * because it is not present in the given {@code Metadata} instance, or because it has been
     * removed).
     */
    @SuppressWarnings("unchecked")
    public T resolve(Metadata metadata) {
        int idx = elements.size() - 1;
        switch (elements.get(idx).getKind()) {
            case CATALOG:
                return (T) resolveCatalog(metadata,idx);
            case SCHEMA:
                return (T) resolveSchema(metadata,idx);
            case TABLE:
                return (T) resolveTable(metadata,idx);
            case VIEW:
                return (T) resolveView(metadata,idx);
            case PROCEDURE:
                return (T) resolveProcedure(metadata,idx);
            case COLUMN:
                return (T) resolveColumn(metadata,idx);
            case PRIMARY_KEY:
                return (T) resolvePrimaryKey(metadata,idx);
            case PARAMETER:
                return (T) resolveParameter(metadata,idx);
            case FOREIGN_KEY:
                return (T) resolveForeignKey(metadata,idx);
            case INDEX:
                return (T) resolveIndex(metadata,idx);
            case FOREIGN_KEY_COLUMN:
                return (T) resolveForeignKeyColumn(metadata,idx);
            case INDEX_COLUMN:
                return (T) resolveIndexColumn(metadata,idx);
            case RETURN_VALUE:
                return (T) resolveReturnValue(metadata,idx);
            case FUNCTION:
                return (T) resolveFunction(metadata,idx);
            default:
                throw new IllegalStateException("Unhandled kind " + elements.get(idx));
        }
    }

    private Catalog resolveCatalog(Metadata metadata,int idx) {
        return metadata.getCatalog(elements.get(idx).getName());
    }

    private Schema resolveSchema(Metadata metadata,int idx) {
        Catalog catalog = resolveCatalog(metadata,idx - 1);
        if (catalog != null) {
            String name = elements.get(idx).getName();
            if (name != null) {
                return catalog.getSchema(name);
            } else {
                return catalog.getSyntheticSchema();
            }
        }
        return null;
    }

    private Table resolveTable(Metadata metadata,int idx) {
        // A table can be part of a number of different metadata elements.
        // Find out which one and resolve appropriately
        switch (elements.get(idx - 1).getKind()) {
            case SCHEMA:
                Schema schema = resolveSchema(metadata,idx - 1);
                if (schema != null) {
                    return schema.getTable(elements.get(idx).getName());
                }
                return null;
            case TABLE:
                Table table = resolveTable(metadata,idx - 1);
                if (table != null) {
                    return table.getPartition(elements.get(idx).getName());
                }
                return null;
            default:
                throw new IllegalStateException("Unhandled kind " +elements.get(idx - 1).getKind());
        }

    }

    private View resolveView(Metadata metadata,int idx) {
        Schema schema = resolveSchema(metadata,idx - 1);
        if (schema != null) {
            return schema.getView(elements.get(idx).getName());
        }
        return null;
    }

    private Procedure resolveProcedure(Metadata metadata,int idx) {
        Schema schema = resolveSchema(metadata,idx - 1);
        if (schema != null && elements.get(idx - 1).getKind() == Kind.PROCEDURE) {
            return schema.getProcedure(elements.get(idx).getName());
        }
        return null;
    }

    private Function resolveFunction(Metadata metadata,int idx) {
        Schema schema = resolveSchema(metadata,idx - 1);
        if (schema != null && elements.get(idx - 1).getKind() == Kind.FUNCTION) {
            return schema.getFunction(elements.get(idx).getName());
        }
        return null;
    }

    private Value resolveReturnValue(Metadata metadata,int idx) {
        Function proc = resolveFunction(metadata,idx - 1);
        if (proc != null) {
            return proc.getReturnValue();
        }
        Procedure proc2 = resolveProcedure(metadata,idx - 1);
        if (proc2 != null) {
            return proc2.getReturnValue();
        }
        return null;
    }

    private PrimaryKey resolvePrimaryKey(Metadata metadata,int idx) {
        Table table = resolveTable(metadata,idx - 1);
        if (table != null) {
            return table.getPrimaryKey();
        }

        return null;
    }

    private Column resolveColumn(Metadata metadata,int idx) {
        // A column can be part of a number of different metadata elements.
        // Find out which one and resolve appropriately
        switch (elements.get(idx - 1).getKind()) {
            case TABLE:
                Table table = resolveTable(metadata,idx - 1);
                if (table != null) {
                    return table.getColumn(elements.get(idx).getName());
                }
                return null;
            case PROCEDURE:
                Procedure proc = resolveProcedure(metadata,idx - 1);
                if (proc != null) {
                    return proc.getColumn(elements.get(idx).getName());
                }
                return null;
            case VIEW:
                View view = resolveView(metadata,idx - 1);
                if (view != null) {
                    return view.getColumn(elements.get(idx).getName());
                }
                return null;
            default:
                throw new IllegalStateException("Unhandled kind " + elements.get(idx - 1).getKind());
        }
    }

    private Parameter resolveParameter(Metadata metadata,int idx) {
        Procedure proc = resolveProcedure(metadata,idx - 1);
        if (proc != null) {
            return proc.getParameter(elements.get(idx).getName());
        }
        Function proc2 = resolveFunction(metadata,idx - 1);
        if (proc2 != null) {
            return proc2.getParameter(elements.get(idx).getName());
        }
        return null;
    }

    private Index resolveIndex(Metadata metadata,int idx) {
        Table table = resolveTable(metadata,idx - 1);
        if (table != null) {
            return table.getIndex(elements.get(idx).getName());
        }
        return null;
    }

    private ForeignKey resolveForeignKey(Metadata metadata,int idx) {
        Table table = resolveTable(metadata,idx - 1);
        if (table != null) {
            return table.getForeignKeyByInternalName(elements.get(idx).getName());
        }

        return null;
    }

    private ForeignKeyColumn resolveForeignKeyColumn(Metadata metadata,int idx) {
        ForeignKey key = resolveForeignKey(metadata,idx - 1);
        if (key != null) {
            return key.getColumn(elements.get(idx).getName());
        }

        return null;
    }

    private IndexColumn resolveIndexColumn(Metadata metadata,int idx) {
        Index index = resolveIndex(metadata,idx - 1);
        if (index != null) {
            return index.getColumn(elements.get(idx).getName());
        }

        return null;
    }

    // Not private becuase it's used in unit tests
    enum Kind {

        CATALOG(Catalog.class),
        SCHEMA(Schema.class),
        TABLE(Table.class),
        VIEW(View.class),
        PROCEDURE(Procedure.class),
        PARAMETER(Parameter.class),
        COLUMN(Column.class),
        PRIMARY_KEY(PrimaryKey.class),
        FOREIGN_KEY(ForeignKey.class),
        INDEX(Index.class),
        FOREIGN_KEY_COLUMN(ForeignKeyColumn.class),
        INDEX_COLUMN(IndexColumn.class),
        RETURN_VALUE(Value.class),
        FUNCTION(Function.class);

        public static Kind of(MetadataElement element) {
            return of(element.getClass());
        }

        public static Kind of(Class<? extends MetadataElement> clazz) {
            for (Kind kind : Kind.values()) {
                if (kind.clazz.equals(clazz)) {
                    return kind;
                }
            }
            throw new IllegalStateException("Unhandled class " + clazz);
        }

        private final Class<? extends MetadataElement> clazz;

        private Kind(Class<? extends MetadataElement> clazz) {
            this.clazz = clazz;
        }
    }

    static class HierarchyElement {

        private final Kind kind;
        private final String name;

        public HierarchyElement(Kind kind, String name) {
            this.kind = kind;
            this.name = name;
        }

        public Kind getKind() {
            return kind;
        }

        public String getName() {
            return name;
        }

    }
}
