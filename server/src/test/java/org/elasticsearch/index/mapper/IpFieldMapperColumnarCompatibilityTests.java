/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

public class IpFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"))
        );
    }

    public void testSingleValueIpv6() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("single value ipv6", 1L, doc("d1", 1L, "{\"f\":\"2001:db8::1\"}"))
        );
    }

    public void testAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("absent doc", 1L, doc("d1", 1L, "{}"))
        );
    }

    public void testMixedAbsentPresent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "mixed absent present",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testMultiValueArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("multi-value array", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",\"10.0.0.2\"]}"))
        );
    }

    public void testArrayValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "array values",
                1L,
                doc("d1", 1L, "{\"f\":[\"10.0.0.1\"]}"),
                doc("d2", 2L, "{\"f\":[\"10.0.0.2\",\"10.0.0.3\",\"10.0.0.4\"]}"),
                doc("d3", 3L, "{\"f\":[]}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testExplicitNullNoNullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("explicit null no null_value", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNullValueSubstitution() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("null_value", "0.0.0.0").endObject()),
            columnarSettings(),
            batch("null_value substitution", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{\"f\":\"1.2.3.4\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testArrayContainingNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("array containing null", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",null,\"10.0.0.2\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testMixedIpv4Ipv6() throws IOException {
        // IPv4 and IPv6 addresses in the same batch. An IPv4 address stored as IPv4-mapped IPv6 (e.g.
        // ::ffff:192.168.0.1) should encode identically to the plain IPv4 address 192.168.0.1.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "mixed ipv4 ipv6",
                1L,
                doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"),
                doc("d2", 2L, "{\"f\":\"2001:db8::1\"}"),
                doc("d3", 3L, "{\"f\":\"::ffff:192.168.0.1\"}")
            )
        );
    }

    public void testDuplicateValuesInArray() throws IOException {
        // Array-order path preserves duplicates (unlike SORTED_UNIQUE).
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("duplicate values", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",\"10.0.0.1\",\"10.0.0.2\"]}"))
        );
    }

    public void testNestedArrayFlattening() throws IOException {
        // Nested arrays are flattened, matching the row-path behaviour in DocumentParser.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("nested array flattening", 1L, doc("d1", 1L, "{\"f\":[[\"10.0.0.1\",\"10.0.0.2\"],[\"10.0.0.3\"]]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testLargeMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "large mixed batch",
                1L,
                doc("d1", 1L, "{\"f\":\"1.1.1.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"2.2.2.2\",\"3.3.3.3\"]}"),
                doc("d4", 4L, "{\"f\":\"4.4.4.4\"}"),
                doc("d5", 5L, "{}"),
                doc("d6", 6L, "{\"f\":\"2001:db8::cafe\"}"),
                doc("d7", 7L, "{}")
            )
        );
    }

    public void testSingleValueMultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "single value multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testAbsentAndNullMultiValueFalse() throws IOException {
        // Present value, absent doc ({}), and explicit JSON null without null_value -> absent.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "absent and null multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":null}")
            )
        );
    }

    public void testNullValueSubstitutionMultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip").field("null_value", "0.0.0.0");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "null_value substitution multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":null}"),
                doc("d2", 2L, "{\"f\":\"1.2.3.4\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testSingleElementArrayMultiValueFalse() throws IOException {
        // A single-element array {"f":["1.1.1.1"]} is a legal value for a multi_value=false field.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "single element array multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":[\"1.1.1.1\"]}"),
                doc("d2", 2L, "{\"f\":[]}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testAllPresentDenseMultiValueFalse() throws IOException {
        // Every doc has an ip value; no absent docs. Exercises the dense (validity==null) wrap.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "all present dense multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{\"f\":\"10.0.0.2\"}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testManyMixedPresentAbsentMultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "many mixed present absent multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"1.1.1.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"3.3.3.3\"}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"f\":\"5.5.5.5\"}"),
                doc("d6", 6L, "{\"f\":\"6.6.6.6\"}"),
                doc("d7", 7L, "{}")
            )
        );
    }

    public void testIpv6MultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }), columnarSettings(), batch("ipv6 multi_value=false", 1L, doc("d1", 1L, "{\"f\":\"2001:db8::1\"}"), doc("d2", 2L, "{}")));
    }

    @AwaitsFix(
        bugUrl = "columnar mapColumnBatch does not implement per-field ignore_malformed for ip; malformed values trigger"
            + " a whole-batch row-path fallback rather than a per-doc skip"
    )
    public void testIgnoreMalformed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            batch("ignore_malformed", 1L, doc("d1", 1L, "{\"f\":\"not-an-ip\"}"), doc("d2", 2L, "{\"f\":\"10.0.0.1\"}"))
        );
    }

    // ---- Indexed ip fields (index:true, strict-columnar) — 16-byte BinaryColumn points --------

    public void testIndexedSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed single value", 1L, doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"))
        );
    }

    public void testIndexedMultiValueArray() throws IOException {
        // Two values per doc: the DV blob deduplicates (SORTED_UNIQUE); the points column keeps both.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed multi-value array", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",\"10.0.0.2\"]}"))
        );
    }

    public void testIndexedDuplicatesInArray() throws IOException {
        // Duplicate values: DV deduplicates (binary dv SORTED_UNIQUE), points column preserves them.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed duplicates in array", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",\"10.0.0.1\",\"10.0.0.2\"]}"))
        );
    }

    public void testIndexedAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed absent", 1L, doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"), doc("d2", 2L, "{}"))
        );
    }

    public void testIndexedExplicitNullNoNullValue() throws IOException {
        // Highest-value negative case: DV records a null slot (counts column), points emit nothing.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed explicit null no null_value", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

    public void testIndexedArrayContainingNull() throws IOException {
        // [v, null, v]: DV records 3 slots, points emit only the 2 non-null values.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed array containing null", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",null,\"10.0.0.2\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testIndexedNullValueSubstitution() throws IOException {
        // null_value DOES emit a point (row-path parity: indexValue is called for the null-value address).
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).field("null_value", "0.0.0.0").endObject()),
            columnarSettings(),
            batch(
                "indexed null_value substitution",
                1L,
                doc("d1", 1L, "{\"f\":null}"),
                doc("d2", 2L, "{\"f\":\"1.2.3.4\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testIndexedIpv4Ipv6Mix() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("index", true).endObject()),
            columnarSettings(),
            batch(
                "indexed ipv4/ipv6 mix",
                1L,
                doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"),
                doc("d2", 2L, "{\"f\":\"2001:db8::1\"}"),
                doc("d3", 3L, "{\"f\":\"::ffff:192.168.0.1\"}")
            )
        );
    }

    public void testIndexedAllPresentDenseMultiValueFalse() throws IOException {
        // multi_value=false + index=true: single-valued binary DV column (no counts) + points column.
        // All docs present → dense EscfColumnData → addDenseNDValues in Lucene.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip").field("index", true);
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "indexed all present dense multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{\"f\":\"10.0.0.2\"}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testIndexedSingleValueMultiValueFalse() throws IOException {
        // multi_value=false + index=true: single-valued binary DV column + points column. Sparse
        // (absent docs) → addPackedValue in Lucene.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip").field("index", true);
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "indexed single value multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    // ---- TSDB SORTED_SET path (TIME_SERIES mode, low-cardinality + RANGE skip index) ----------
    //
    // In TSDB every ip field — dimension or not, index:true or not — resolves to
    // IndexType.skippers(): SORTED_SET doc values with a RANGE skip index. The ecs_ip dynamic
    // template on the OTel benchmark produces this configuration. These tests exercise the new
    // mapColumnBatchSortedSet method and verify that the DocValuesSkipIndexType matches the row path.

    // Shared TSDB test constants. Declared at class level to avoid repeated computation.
    private static final BytesRef TSDB_TSID = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final int TSDB_ROUTING_HASH = 42;
    private static final String TSDB_ROUTING = TimeSeriesRoutingHashFieldMapper.encode(TSDB_ROUTING_HASH);
    // epoch millis: 2024-01-15T12:00:00.000Z, 2024-06-01T00:00:00.000Z
    private static final long TSDB_TS_A = 1705320000000L;
    private static final long TSDB_TS_B = 1717200000000L;

    private static Settings tsdbSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .build();
    }

    public void testTsdbSortedSetSingleValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "ip").endObject();
        }),
            tsdbSettings(),
            batch(
                "tsdb sorted_set single value",
                1L,
                doc(idA, TSDB_ROUTING, TSDB_TSID, 1L, "{\"@timestamp\":" + TSDB_TS_A + ",\"f\":\"10.0.0.1\"}")
            )
        );
    }

    public void testTsdbSortedSetAbsentAndNull() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "ip").endObject();
        }),
            tsdbSettings(),
            batch(
                "tsdb sorted_set absent and null",
                1L,
                doc(idA, TSDB_ROUTING, TSDB_TSID, 1L, "{\"@timestamp\":" + TSDB_TS_A + ",\"f\":\"10.0.0.1\"}"),
                doc(idB, TSDB_ROUTING, TSDB_TSID, 2L, "{\"@timestamp\":" + TSDB_TS_B + "}")
            )
        );
    }

    public void testTsdbSortedSetNullValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "ip").field("null_value", "0.0.0.0").endObject();
        }),
            tsdbSettings(),
            batch(
                "tsdb sorted_set null_value substitution",
                1L,
                doc(idA, TSDB_ROUTING, TSDB_TSID, 1L, "{\"@timestamp\":" + TSDB_TS_A + ",\"f\":null}"),
                doc(idB, TSDB_ROUTING, TSDB_TSID, 2L, "{\"@timestamp\":" + TSDB_TS_B + ",\"f\":\"1.2.3.4\"}")
            )
        );
    }

    public void testTsdbSortedSetMultiValueWithDuplicates() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "ip").endObject();
        }),
            tsdbSettings(),
            // Out-of-order and duplicates: Lucene SORTED_SET sorts and dedups at write time.
            batch(
                "tsdb sorted_set multi-value with duplicates",
                1L,
                doc(idA, TSDB_ROUTING, TSDB_TSID, 1L, "{\"@timestamp\":" + TSDB_TS_A + ",\"f\":[\"10.0.0.2\",\"10.0.0.1\",\"10.0.0.2\"]}")
            )
        );
    }

    public void testTsdbSortedSetIpv4Ipv6Mix() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "ip").endObject();
        }),
            tsdbSettings(),
            batch(
                "tsdb sorted_set ipv4/ipv6 mix",
                1L,
                doc(idA, TSDB_ROUTING, TSDB_TSID, 1L, "{\"@timestamp\":" + TSDB_TS_A + ",\"f\":[\"192.168.0.1\",\"2001:db8::1\"]}")
            )
        );
    }

    public void testTsdbSortedSetIndexTrue() throws IOException {
        // index:true in TSDB is still IndexType.skippers() (SORTED_SET + RANGE, no points), because
        // useTimeSeriesDocValuesSkippers is checked before indexed in IpFieldMapper.Builder#indexType().
        // The columnar path must emit SORTED_SET_DV_INDEXED_FIELD_TYPE, not a points column.
        final String idA = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "ip").field("index", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "tsdb sorted_set index=true (still skippers, no points)",
                1L,
                doc(idA, TSDB_ROUTING, TSDB_TSID, 1L, "{\"@timestamp\":" + TSDB_TS_A + ",\"f\":\"10.0.0.1\"}")
            )
        );
    }

    public void testTsdbNoSkipperEmitsPoints() throws IOException {
        // With use_doc_values_skipper=false the skipper feature is suppressed: indexed defaults to true,
        // so indexType() == IndexType.points(true, true): plain SORTED_SET_DV_FIELD_TYPE (no RANGE) + a
        // 16-byte points column. This is the only combination that produces both DV and points for ip.
        final String idA = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(TSDB_ROUTING_HASH, TSDB_TSID, TSDB_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "ip").endObject();
        }),
            Settings.builder().put(tsdbSettings()).put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), false).build(),
            batch(
                "tsdb no skipper emits points",
                1L,
                doc(idA, TSDB_ROUTING, TSDB_TSID, 1L, "{\"@timestamp\":" + TSDB_TS_A + ",\"f\":\"10.0.0.1\"}"),
                doc(idB, TSDB_ROUTING, TSDB_TSID, 2L, "{\"@timestamp\":" + TSDB_TS_B + "}")
            )
        );
    }
}
