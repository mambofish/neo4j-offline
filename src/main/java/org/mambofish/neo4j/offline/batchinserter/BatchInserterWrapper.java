/*
 * Copyright 2014+ Mambofish Technology Solutions - All Rights Reserved
 *
 * This file is part of the Satsuma Graph ETL platform, which is confidential and proprietary software.
 *
 * Unauthorized copying of this file, via any means, is strictly prohibited.
 *
 * If you wish to obtain a License for this software, please apply via email to "satsuma@mambofish.org"
 */

package org.mambofish.neo4j.offline.batchinserter;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchInserterWrapper
 */
public class BatchInserterWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchInserterWrapper.class);
    private static final AtomicInteger sessionCount = new AtomicInteger(0);

    private static BatchInserter batchInserter;
    private static final Map<String, Label> createdLabels = new HashMap<>();

    private static Method labelMethod;
    private final Class<?> labelClass;

    private static BatchInserterWrapper INSTANCE;

    public synchronized static BatchInserterWrapper connect(File graphDatabase) throws Exception {
        if (INSTANCE == null) {
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> {
                        if (canShutdown()) {
                            shutdown();
                        }
                    }
            ));
            INSTANCE = new BatchInserterWrapper(graphDatabase);
        }
        sessionCount.getAndIncrement();
        return INSTANCE;
    }

    private BatchInserterWrapper(File graphDatabase) throws Exception {

        loadBatchInserter(graphDatabase);

        try {
            labelMethod = Class.forName("org.neo4j.graphdb.Label").getDeclaredMethod("label", String.class);
        } catch (Exception e1) {
            try {
                labelMethod = Class.forName("org.neo4j.graphdb.DynamicLabel").getDeclaredMethod("label", String.class);
            } catch (Exception e2) {
                throw new RuntimeException("Could not get method for creating labels");
            }
        }
        labelClass = labelMethod.getDeclaringClass();
    }

    public synchronized void createNode(Long id, Map<String, Object> properties, List<String> labels) {
        batchInserter.createNode(id, properties, neo4jLabels(labels));
    }

    public synchronized long createNode(Map<String, Object> properties, List<String> labels) {
        return batchInserter.createNode(properties, neo4jLabels(labels));
    }

    public synchronized long createEdge(long u, long v, String type, Map<String, Object> properties) {
        return batchInserter.createRelationship(u, v, DynamicRelationshipType.withName(type.toUpperCase()), properties);
    }

    public synchronized void updateNode(long node, Map<String, Object> properties, List<String> labels) {
        if (batchInserter.nodeExists(node)) {
            properties.putAll(getNodeProperties(node));
            batchInserter.setNodeProperties(node, properties);
        } else {
            createNode(node, properties, labels);
        }
    }

    public synchronized void updateEdge(long edge, long u, long v, String type, Map<String, Object> properties) {
        try {
            batchInserter.getRelationshipById(edge);
            properties.putAll(getEdgeProperties(edge));
            batchInserter.setRelationshipProperties(edge, properties);
        } catch (Exception e) {
            createEdge(u, v, type, properties);
        }
    }

    public synchronized void createIndex(String label, String property, boolean unique) {
        if (unique) {
            batchInserter.createDeferredConstraint(createdLabels.get(label)).assertPropertyIsUnique(property).create();
        } else {
            batchInserter.createDeferredSchemaIndex(createdLabels.get(label)).on(property).create();
        }
    }

    public synchronized void disconnect() {
        sessionCount.getAndDecrement();
        if (canShutdown()) {
            shutdown();
        }
    }

    private static void shutdown() {
        try {
            batchInserter.shutdown();
            INSTANCE = null;
        } catch (Exception e) {
            LOGGER.warn("Could not shutdown: " + e.getLocalizedMessage());
        }
    }

    private Label getLabel(String name) {
        try {
            return (Label) labelMethod.invoke(labelClass, name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Label[] neo4jLabels(List<String> labels) {

        Label[] neo4jLabels = new Label[labels.size()];

        for (int i = 0; i < labels.size(); i++) {
            String labelName = labels.get(i);
            Label label = createdLabels.get(labelName);
            if (label == null) {
                createdLabels.put(labelName, label = getLabel(labelName));
            }
            neo4jLabels[i] = label;
        }
        return neo4jLabels;
    }


    private Map<String, Object> getNodeProperties(long nodeId) {
        return toPropertiesMap(batchInserter.getNodeProperties(nodeId));
    }

    private Map<String, Object> getEdgeProperties(long edgeId) {
        return toPropertiesMap(batchInserter.getRelationshipProperties(edgeId));
    }

    // neo4j batch inserter api changes between 3.2 and 3.3
    private Map<String, Object> toPropertiesMap(Map<String, Object> map) {

        Map<String, Object> returnedMap = new HashMap<>();

        if (map.size() > 0) {
            Collection<Object> values = map.values();
            Object value = values.iterator().next();
            if (value.getClass().getName().startsWith("org.neo4j.values.storable")) {
                try {
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        String key = entry.getKey();
                        Object storedValue = entry.getValue();
                        Method asObject = storedValue.getClass().getMethod("asObjectCopy");
                        Object realValue = asObject.invoke(storedValue);
                        returnedMap.put(key, realValue);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                returnedMap = map;
            }
        }
        return returnedMap;
    }

    @SuppressWarnings("JavaReflectionMemberAccess")
    private void loadBatchInserter(File graphDatabase) throws Exception {

        Map<String, String> params = MapUtil.stringMap(
                "cache_type", "weak",
                "batch_inserter_batch_size", "10000",
                "dense_node_threshold", "50");

        try {
            Method fileMethod = BatchInserters.class.getMethod("inserter", File.class, Map.class);
            batchInserter = (BatchInserter) fileMethod.invoke(null, graphDatabase, params);

        } catch (NoSuchMethodException | SecurityException me) {
            // 2.x versions don't use a File parameter, but a filename String
            try {
                Method stringMethod = BatchInserters.class.getMethod("inserter", String.class, Map.class);
                batchInserter = (BatchInserter) stringMethod.invoke(null, graphDatabase.getAbsolutePath(), params);
            } catch (Exception e1) {
                e1.printStackTrace();
                throw e1;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static boolean canShutdown() {
        return INSTANCE != null && sessionCount.get() == 0;
    }

}
