/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import joptsimple.internal.Strings;

import org.elasticsearch.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * This service collects health indicators from all modules and plugins of elasticsearch
 */
public class HealthService {

    private final List<HealthIndicatorService> healthIndicatorServices;
    private final Map<String, Set<String>> componentToIndicators;

    public HealthService(List<HealthIndicatorService> healthIndicatorServices) {
        this.healthIndicatorServices = healthIndicatorServices;
        HashMap<String, Set<String>> componentToIndicators = new HashMap<>();
        for (HealthIndicatorService service : healthIndicatorServices) {
            componentToIndicators.compute(service.component(), (ignored, value) -> {
                final Set<String> newIndicators;
                newIndicators = Objects.requireNonNullElseGet(value, HashSet::new);
                newIndicators.add(service.name());
                return newIndicators;
            });
        }
        this.componentToIndicators = Collections.unmodifiableMap(componentToIndicators);
    }

    public void validate(String component, List<String> indicators) throws ResourceNotFoundException {
        if (component == null) {
            return;
        }
        Set<String> knownIndicators = componentToIndicators.get(component);
        if (knownIndicators == null) {
            throw new ResourceNotFoundException("Health component [" + component + "] not found.");
        }
        if (knownIndicators.containsAll(indicators) == false) {
            ArrayList<String> unknown = new ArrayList<>(indicators);
            unknown.removeIf(knownIndicators::contains);
            throw new ResourceNotFoundException(
                "Health indicators [" + Strings.join(unknown, ",") + "] not found for health component [" + component + "]."
            );
        }
    }

    public List<HealthComponentResult> getHealth(String component, List<String> indicators) {
        HashSet<String> setOfIndicators = new HashSet<>(indicators);
        return List.copyOf(
            healthIndicatorServices.stream()
                .filter(s -> component == null || s.component().equals(component))
                .filter(s -> setOfIndicators.isEmpty() || setOfIndicators.contains(s.name()))
                .map(HealthIndicatorService::calculate)
                .collect(
                    groupingBy(
                        HealthIndicatorResult::component,
                        TreeMap::new,
                        collectingAndThen(toList(), HealthService::createComponentFromIndicators)
                    )
                )
                .values()
        );
    }

    private static HealthComponentResult createComponentFromIndicators(List<HealthIndicatorResult> indicators) {
        assert indicators.size() > 0 : "Component should not be non empty";
        assert indicators.stream().map(HealthIndicatorResult::component).distinct().count() == 1L
            : "Should not mix indicators from different components";
        return new HealthComponentResult(
            indicators.get(0).component(),
            HealthStatus.merge(indicators.stream().map(HealthIndicatorResult::status)),
            indicators
        );
    }
}
