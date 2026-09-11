/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import http from "node:http";

const fixtures = [
  { port: 27101, cluster: "cluster-a", pod: "dubbod-a", node: "node-a", instances: ["dubbod-a", "dubbod-b"] },
  { port: 27102, cluster: "cluster-a", pod: "dubbod-b", node: "node-b", instances: ["dubbod-a", "dubbod-b"] },
  { port: 27103, cluster: "cluster-b", pod: "dubbod-c", node: "node-c", instances: ["dubbod-c"] },
];

const readyStatus = {
  xdsServerReady: true,
  cachesSynced: true,
  servicesSynced: true,
  configSynced: true,
  proxylessSynced: true,
  injectorReady: true,
  validationReady: true,
};

const overview = (fixture) => ({
  product: "Dubbo",
  version: "e2e",
  clusterId: fixture.cluster,
  namespace: "dubbo-system",
  podName: fixture.pod,
  mesh: {},
  server: {},
  status: readyStatus,
  counts: {
    services: 1,
    endpointServices: 1,
    xdsConnections: 1,
    registries: 1,
  },
  configKinds: [],
  registries: [{ provider: "Kubernetes", cluster: fixture.cluster, synced: true }],
  services: [{
    name: "echo",
    hostname: "echo.default.svc.cluster.local",
    namespace: "default",
    registry: "Kubernetes",
    ports: "80",
    exposure: "internal",
  }],
  instances: fixture.instances.map((name, index) => ({
    name,
    namespace: "dubbo-system",
    ip: `10.0.${fixture.cluster === "cluster-a" ? 1 : 2}.${index + 1}`,
    isReady: true,
  })),
  dataPlane: [{
    name: "echo-1",
    namespace: "default",
    phase: "Running",
    ready: true,
    sidecarReady: true,
    connected: true,
    nodeId: fixture.node,
  }],
  dataPlanePods: { default: 1 },
  routes: [],
  xdsClients: [{
    nodeId: fixture.node,
    connectedAt: "2026-08-03T12:00:00Z",
    watched: ["CDS", "EDS"],
  }],
  gatewayInstances: [],
  updatedAt: "2026-08-03T12:00:00Z",
});

const metrics = {
  families: [{
    name: "dubbod_inbound_updates",
    type: "COUNTER",
    metrics: [{ labels: { type: "service" }, value: 1 }],
  }],
  updatedAt: "2026-08-03T12:00:00Z",
};

for (const fixture of fixtures) {
  http.createServer((request, response) => {
    response.setHeader("content-type", "application/json");
    if (request.url.startsWith("/api/v1/overview")) {
      response.end(JSON.stringify(overview(fixture)));
      return;
    }
    if (request.url.startsWith("/api/v1/metrics")) {
      response.end(JSON.stringify(metrics));
      return;
    }
    if (request.url.startsWith("/api/v1/logs")) {
      response.end(JSON.stringify({
        kind: "dubbod",
        name: "dubbod",
        namespace: "dubbo-system",
        pods: [{ name: fixture.pod, container: "execute", phase: "Running", ready: true, logs: "e2e ok" }],
      }));
      return;
    }
    response.statusCode = 404;
    response.end(JSON.stringify({ error: "not found" }));
  }).listen(fixture.port, "127.0.0.1");
}

console.log("fake control planes ready on 27101, 27102, 27103");
