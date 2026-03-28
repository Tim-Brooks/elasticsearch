/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves leaf column mappers from an {@link EIRFSchema} against a {@link MappingLookup}.
 * Reconstructs full dotted field paths by walking parent back-pointers and caches
 * intermediate results to avoid redundant traversals.
 */
public class BatchMappingsResolver {

    public static Resolved resolve(EIRFSchema eirfSchema, MappingLookup mappingLookup) {
        int parentCount = eirfSchema.parentColumnCount();
        int leafCount = eirfSchema.leafColumnCount();

        // Step 1: Build parent full paths (cached array).
        // Process in index order — a parent's parentIndex always refers to an earlier index.
        String[] parentFullPaths = new String[parentCount];
        for (int i = 0; i < parentCount; i++) {
            EIRFSchema.Column col = eirfSchema.getParentColumn(i);
            if (col.parentIndex() == -1) {
                parentFullPaths[i] = col.name();
            } else {
                parentFullPaths[i] = parentFullPaths[col.parentIndex()] + "." + col.name();
            }
        }

        // Step 2: Resolve leaf column mappers and runtime field flags.
        String[] leafFullPaths = new String[leafCount];
        Mapper[] leafMappers = new Mapper[leafCount];
        boolean[] isRuntimeField = new boolean[leafCount];
        for (int i = 0; i < leafCount; i++) {
            EIRFSchema.Column leaf = eirfSchema.getLeafColumn(i);
            if (leaf.parentIndex() == -1) {
                leafFullPaths[i] = leaf.name();
            } else {
                leafFullPaths[i] = parentFullPaths[leaf.parentIndex()] + "." + leaf.name();
            }
            Mapper mapper = mappingLookup.getMapper(leafFullPaths[i]);
            if (mapper == null) {
                mapper = mappingLookup.objectMappers().get(leafFullPaths[i]);
            }
            leafMappers[i] = mapper;
            // Runtime fields appear in field types but not in field mappers.
            if (mapper == null && mappingLookup.getFieldType(leafFullPaths[i]) != null) {
                isRuntimeField[i] = true;
            }
        }

        // Step 3: Detect compound sub-fields among unmapped leaves.
        boolean[] isCompoundSubField = new boolean[leafCount];
        Map<String, CompoundFieldGroup> compoundGroups = new LinkedHashMap<>();
        for (int i = 0; i < leafCount; i++) {
            if (leafMappers[i] != null || isRuntimeField[i]) {
                continue;
            }
            CompoundFieldInfo info = resolveCompoundParent(leafFullPaths[i], mappingLookup);
            if (info != null) {
                isCompoundSubField[i] = true;
                CompoundFieldGroup group = compoundGroups.computeIfAbsent(
                    info.parentPath(),
                    k -> new CompoundFieldGroup(info.parentPath(), info.parentMapper())
                );
                group.addSubField(info.subFieldName(), i);
            }
        }

        // Step 4: Resolve metadata field mappers for preParse/postParse.
        MetadataFieldMapper[] metadataFieldMappers = mappingLookup.getMapping().getSortedMetadataMappers();

        // Step 5: Resolve effective dynamic setting per unmapped leaf.
        ObjectMapper.Dynamic[] leafEffectiveDynamic = new ObjectMapper.Dynamic[leafCount];
        for (int i = 0; i < leafCount; i++) {
            if (leafMappers[i] != null || isRuntimeField[i]) {
                continue;
            }
            leafEffectiveDynamic[i] = resolveEffectiveDynamic(leafFullPaths[i], mappingLookup);
        }

        // Step 6: Resolve copy_to targets per mapped leaf.
        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<String>[] leafCopyToTargets = new List[leafCount];
        for (int i = 0; i < leafCount; i++) {
            if (leafMappers[i] instanceof FieldMapper fm) {
                List<String> targets = fm.copyTo().copyToFields();
                if (targets.isEmpty() == false) {
                    leafCopyToTargets[i] = targets;
                }
            }
        }

        // Step 7: Resolve parent object mappers and disabled status.
        ObjectMapper[] parentObjectMappers = new ObjectMapper[parentCount];
        boolean[] parentDisabled = new boolean[parentCount];
        for (int i = 0; i < parentCount; i++) {
            EIRFSchema.Column col = eirfSchema.getParentColumn(i);
            ObjectMapper obj = mappingLookup.objectMappers().get(parentFullPaths[i]);
            parentObjectMappers[i] = obj;
            if (obj != null && obj.isEnabled() == false) {
                parentDisabled[i] = true;
            } else if (col.parentIndex() != -1) {
                parentDisabled[i] = parentDisabled[col.parentIndex()];
            }
        }

        // Step 8: Propagate disabled status to leaves.
        boolean[] leafParentDisabled = new boolean[leafCount];
        for (int i = 0; i < leafCount; i++) {
            EIRFSchema.Column leaf = eirfSchema.getLeafColumn(i);
            if (leaf.parentIndex() != -1) {
                leafParentDisabled[i] = parentDisabled[leaf.parentIndex()];
            }
        }

        return new Resolved(
            parentFullPaths,
            leafFullPaths,
            leafMappers,
            metadataFieldMappers,
            isCompoundSubField,
            compoundGroups,
            leafEffectiveDynamic,
            isRuntimeField,
            leafCopyToTargets,
            leafParentDisabled,
            parentObjectMappers
        );
    }

    /**
     * Walks up the parent chain for the given field path to find the nearest ObjectMapper
     * with an explicit dynamic setting. Falls back to the root dynamic setting.
     */
    static ObjectMapper.Dynamic resolveEffectiveDynamic(String fieldPath, MappingLookup mappingLookup) {
        int dotPos = fieldPath.length();
        while (true) {
            dotPos = fieldPath.lastIndexOf('.', dotPos - 1);
            if (dotPos < 0) break;
            String parentPath = fieldPath.substring(0, dotPos);
            ObjectMapper obj = mappingLookup.objectMappers().get(parentPath);
            if (obj != null && obj.dynamic() != null) {
                return obj.dynamic();
            }
        }
        return ObjectMapper.Dynamic.getRootDynamic(mappingLookup);
    }

    /**
     * For an unmapped field path, walks backward through parent segments to find a compound field parent.
     * E.g., for "metrics.duration.sum", checks "metrics.duration" then "metrics".
     */
    private static CompoundFieldInfo resolveCompoundParent(String fieldPath, MappingLookup mappingLookup) {
        int dotPos = fieldPath.length();
        while (true) {
            dotPos = fieldPath.lastIndexOf('.', dotPos - 1);
            if (dotPos < 0) return null;
            String parentPath = fieldPath.substring(0, dotPos);
            Mapper parent = mappingLookup.getMapper(parentPath);
            if (parent == null) {
                parent = mappingLookup.objectMappers().get(parentPath);
            }
            if (parent instanceof FieldMapper fm && fm.isCompoundField()) {
                String subFieldName = fieldPath.substring(dotPos + 1);
                return new CompoundFieldInfo(parentPath, fm, subFieldName);
            }
        }
    }

    record CompoundFieldInfo(String parentPath, FieldMapper parentMapper, String subFieldName) {}

    public static final class CompoundFieldGroup {
        private final String parentPath;
        private final FieldMapper parentMapper;
        private final List<String> subFieldNames = new ArrayList<>();
        private final List<Integer> columnIndices = new ArrayList<>();

        CompoundFieldGroup(String parentPath, FieldMapper parentMapper) {
            this.parentPath = parentPath;
            this.parentMapper = parentMapper;
        }

        void addSubField(String subFieldName, int columnIndex) {
            subFieldNames.add(subFieldName);
            columnIndices.add(columnIndex);
        }

        public String parentPath() {
            return parentPath;
        }

        public FieldMapper parentMapper() {
            return parentMapper;
        }

        public List<String> subFieldNames() {
            return subFieldNames;
        }

        public List<Integer> columnIndices() {
            return columnIndices;
        }
    }

    public record Resolved(
        String[] parentFullPaths,
        String[] leafFullPaths,
        Mapper[] leafMappers,
        MetadataFieldMapper[] metadataFieldMappers,
        boolean[] isCompoundSubField,
        Map<String, CompoundFieldGroup> compoundGroups,
        ObjectMapper.Dynamic[] leafEffectiveDynamic,
        boolean[] isRuntimeField,
        List<String>[] leafCopyToTargets,
        boolean[] leafParentDisabled,
        ObjectMapper[] parentObjectMappers
    ) {}
}
