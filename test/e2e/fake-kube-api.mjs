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

const pods = ["dubbod-a", "dubbod-b"];
const status = {
  xdsServerReady: true,
  cachesSynced: true,
  servicesSynced: true,
  configSynced: true,
  proxylessSynced: true,
  injectorReady: true,
  validationReady: true,
};

const overview = (pod) => ({
  product: "Dubbo",
  version: "e2e",
  clusterId: "cluster-kube",
  namespace: "dubbo-system",
  podName: pod,
  mesh: {},
  server: {},
  status,
  counts: { services: 1, endpointServices: 1, xdsConnections: 1, registries: 1 },
  configKinds: [],
  registries: [{ provider: "Kubernetes", cluster: "cluster-kube", synced: true }],
  services: [{
    name: "echo",
    hostname: "echo.default.svc.cluster.local",
    namespace: "default",
    registry: "Kubernetes",
    ports: "80",
    exposure: "internal",
  }],
  instances: pods.map((name, index) => ({
    name,
    namespace: "dubbo-system",
    ip: `10.0.0.${index + 1}`,
    isReady: true,
  })),
  dataPlane: [],
  dataPlanePods: {},
  routes: [],
  xdsClients: [{ nodeId: `${pod}-node`, connectedAt: "2026-08-03T12:00:00Z" }],
  gatewayInstances: [],
  updatedAt: "2026-08-03T12:00:00Z",
});

http.createServer((request, response) => {
  response.setHeader("content-type", "application/json");
  if (request.url.startsWith("/apis/discovery.k8s.io/v1/namespaces/dubbo-system/endpointslices")) {
    response.end(JSON.stringify({
      items: [{
        metadata: { name: "dubbod-management-e2e" },
        ports: [{ name: "management", port: 26080 }],
        endpoints: pods.map((pod, index) => ({
          addresses: [`10.0.0.${index + 1}`],
          conditions: { ready: true },
          targetRef: { name: pod },
        })),
      }],
    }));
    return;
  }

  const pod = pods.find((candidate) => request.url.includes(`/pods/${candidate}:26080/proxy/`));
  if (pod && request.url.endsWith("/api/v1/overview")) {
    response.end(JSON.stringify(overview(pod)));
    return;
  }
  if (pod && request.url.endsWith("/api/v1/metrics")) {
    response.end(JSON.stringify({ families: [], updatedAt: "2026-08-03T12:00:00Z" }));
    return;
  }
  if (pod && request.url.includes("/api/v1/logs")) {
    response.end(JSON.stringify({ kind: "dubbod", name: "dubbod", namespace: "dubbo-system", pods: [] }));
    return;
  }
  response.statusCode = 404;
  response.end(JSON.stringify({ error: "not found", path: request.url }));
}).listen(27201, "127.0.0.1", () => {
  console.log("fake Kubernetes API ready on 27201");
});
