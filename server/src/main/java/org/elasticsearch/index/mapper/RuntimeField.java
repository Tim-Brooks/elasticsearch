/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Definition of a runtime field that can be defined as part of the runtime section of the index mappings
 */
public interface RuntimeField extends ToXContentFragment {

    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", typeName());
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Prints out the parameters that subclasses expose
     */
    void doXContentBody(XContentBuilder builder, Params params) throws IOException;

    /**
     * Exposes the name of the runtime field
     * @return name of the field
     */
    String name();

    /**
     * Exposes the type of the runtime field
     * @return type of the field
     */
    String typeName();

    /**
     * Exposes the {@link MappedFieldType}s backing this runtime field, used to execute queries, run aggs etc.
     * @return the {@link MappedFieldType}s backing this runtime field
     */
    Collection<MappedFieldType> asMappedFieldTypes();

    /**
     *  For runtime fields the {@link RuntimeField.Parser} returns directly the {@link MappedFieldType}.
     *  Internally we still create a {@link RuntimeField.Builder} so we reuse the {@link FieldMapper.Parameter} infrastructure,
     *  but {@link RuntimeField.Builder#init(FieldMapper)} and {@link RuntimeField.Builder#build(ContentPath)} are never called as
     *  {@link RuntimeField.Parser#parse(String, Map, Mapper.TypeParser.ParserContext)} calls
     *  {@link RuntimeField.Builder#parse(String, Mapper.TypeParser.ParserContext, Map)} and returns the corresponding
     *  {@link MappedFieldType}.
     */
    abstract class Builder extends FieldMapper.Builder {
        final FieldMapper.Parameter<Map<String, String>> meta = FieldMapper.Parameter.metaParam();

        protected Builder(String name) {
            super(name);
        }

        public Map<String, String> meta() {
            return meta.getValue();
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            return Collections.singletonList(meta);
        }

        @Override
        public FieldMapper.Builder init(FieldMapper initializer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final FieldMapper build(ContentPath context) {
            throw new UnsupportedOperationException();
        }

        protected abstract RuntimeField createRuntimeField(Mapper.TypeParser.ParserContext parserContext);

        private void validate() {
            ContentPath contentPath = parentPath(name());
            FieldMapper.MultiFields multiFields = multiFieldsBuilder.build(this, contentPath);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException("runtime field [" + name + "] does not support [fields]");
            }
            FieldMapper.CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException("runtime field [" + name + "] does not support [copy_to]");
            }
        }
    }

    /**
     * Parser for a runtime field. Creates the appropriate {@link RuntimeField} for a runtime field,
     * as defined in the runtime section of the index mappings.
     */
    final class Parser {
        private final Function<String, Builder> builderFunction;

        public Parser(Function<String, RuntimeField.Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        RuntimeField parse(String name, Map<String, Object> node, Mapper.TypeParser.ParserContext parserContext)
            throws MapperParsingException {

            RuntimeField.Builder builder = builderFunction.apply(name);
            builder.parse(name, parserContext, node);
            builder.validate();
            return builder.createRuntimeField(parserContext);
        }
    }

    /**
     * Parse runtime fields from the provided map, using the provided parser context.
     * @param node the map that holds the runtime fields configuration
     * @param parserContext the parser context that holds info needed when parsing mappings
     * @param supportsRemoval whether a null value for a runtime field should be properly parsed and
     *                        translated to the removal of such runtime field
     * @return the parsed runtime fields
     */
    static Map<String, RuntimeField> parseRuntimeFields(Map<String, Object> node,
                                                        Mapper.TypeParser.ParserContext parserContext,
                                                        boolean supportsRemoval) {
        Map<String, RuntimeField> runtimeFields = new HashMap<>();
        Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            if (entry.getValue() == null) {
                if (supportsRemoval) {
                    runtimeFields.put(fieldName, null);
                } else {
                    throw new MapperParsingException("Runtime field [" + fieldName + "] was set to null but its removal is not supported " +
                        "in this context");
                }
            } else if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> propNode = new HashMap<>(((Map<String, Object>) entry.getValue()));
                Object typeNode = propNode.get("type");
                String type;
                if (typeNode == null) {
                    throw new MapperParsingException("No type specified for runtime field [" + fieldName + "]");
                } else {
                    type = typeNode.toString();
                }
                Parser typeParser = parserContext.runtimeFieldParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("No handler for type [" + type +
                        "] declared on runtime field [" + fieldName + "]");
                }
                runtimeFields.put(fieldName, typeParser.parse(fieldName, propNode, parserContext));
                propNode.remove("type");
                MappingParser.checkNoRemainingFields(fieldName, propNode);
                iterator.remove();
            } else {
                throw new MapperParsingException("Expected map for runtime field [" + fieldName + "] definition but got a "
                    + entry.getValue().getClass().getName());
            }
        }
        return Collections.unmodifiableMap(runtimeFields);
    }

    /**
     * Collect and return all {@link MappedFieldType} exposed by the provided {@link RuntimeField}s.
     * Note that validation is performed to make sure that there are no name clashes among the collected runtime fields.
     * This is because runtime fields with the same name are not accepted as part of the same section.
     * @param runtimeFields the runtime to extract the mapped field types from
     * @return the collected mapped field types
     */
    static Map<String, MappedFieldType> collectFieldTypes(Collection<RuntimeField> runtimeFields) {
        return runtimeFields.stream()
            .flatMap(runtimeField -> {
                List<String> names = runtimeField.asMappedFieldTypes().stream().map(MappedFieldType::name)
                    .filter(name -> name.equals(runtimeField.name()) == false
                        && (name.startsWith(runtimeField.name() + ".") == false
                        || name.length() > runtimeField.name().length() + 1 == false))
                    .collect(Collectors.toList());
                if (names.isEmpty() == false) {
                    throw new IllegalStateException("Found sub-fields with name not belonging to the parent field they are part of "
                        + names);
                }
                return runtimeField.asMappedFieldTypes().stream();
            })
            .collect(Collectors.toUnmodifiableMap(MappedFieldType::name, mappedFieldType -> mappedFieldType,
                (t, t2) -> {
                    throw new IllegalArgumentException("Found two runtime fields with same name [" + t.name() + "]");
                }));
    }
}
