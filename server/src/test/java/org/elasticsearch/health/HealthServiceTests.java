/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthServiceTests extends ESTestCase {

    public void testShouldReturnGroupedIndicators() {

        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null);

        var service = new HealthService(
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            )
        );

        assertThat(
            service.getHealth(null, Collections.emptyList()),
            anyOf(
                hasItems(
                    new HealthComponentResult("component1", YELLOW, List.of(indicator2, indicator1)),
                    new HealthComponentResult("component2", GREEN, List.of(indicator3))
                ),
                hasItems(
                    new HealthComponentResult("component1", YELLOW, List.of(indicator1, indicator2)),
                    new HealthComponentResult("component2", GREEN, List.of(indicator3))
                )
            )
        );
    }

    public void testValidateRequestedComponentsAndIndicators() {
        var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null);
        var indicator2 = new HealthIndicatorResult("indicator2", "component1", GREEN, null, null);
        var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null);

        var service = new HealthService(
            List.of(
                createMockHealthIndicatorService(indicator1),
                createMockHealthIndicatorService(indicator2),
                createMockHealthIndicatorService(indicator3)
            )
        );

        // Should not throw
        service.validate("component1", List.of("indicator1", "indicator2"));

        ResourceNotFoundException rnfe = expectThrows(
            ResourceNotFoundException.class,
            () -> service.validate("unknown_component", List.of("indicator1", "indicator2"))
        );
        assertThat(rnfe.getMessage(), equalTo("Health component [unknown_component] not found."));

        ResourceNotFoundException rnfe2 = expectThrows(
            ResourceNotFoundException.class,
            () -> service.validate("component1", List.of("indicator1", "indicator3", "indicator4"))
        );
        assertThat(rnfe2.getMessage(), equalTo("Health indicators [indicator3,indicator4] not found for health component [component1]."));

    }

    private static HealthIndicatorService createMockHealthIndicatorService(HealthIndicatorResult result) {
        var healthIndicatorService = mock(HealthIndicatorService.class);
        when(healthIndicatorService.calculate()).thenReturn(result);
        when(healthIndicatorService.component()).thenReturn(result.component());
        when(healthIndicatorService.name()).thenReturn(result.name());
        return healthIndicatorService;
    }
}
