// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package console

import (
	"net/http"
	"time"
)

type Endpoint struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	URL    string `json:"url"`
	Source string `json:"source"`
	client *http.Client
}

type ControlPlaneStatus struct {
	Endpoint
	ClusterID string        `json:"clusterId,omitempty"`
	Namespace string        `json:"namespace,omitempty"`
	PodName   string        `json:"podName,omitempty"`
	Version   string        `json:"version,omitempty"`
	Healthy   bool          `json:"healthy"`
	Latency   time.Duration `json:"-"`
	LatencyMS int64         `json:"latencyMs"`
	Error     string        `json:"error,omitempty"`
}

type Overview struct {
	Product          string               `json:"product"`
	Version          string               `json:"version"`
	ClusterID        string               `json:"clusterId"`
	Namespace        string               `json:"namespace"`
	PodName          string               `json:"podName,omitempty"`
	Mesh             OverviewMesh         `json:"mesh"`
	Server           OverviewServer       `json:"server"`
	Status           OverviewStatus       `json:"status"`
	Counts           OverviewCounts       `json:"counts"`
	ConfigKinds      []ConfigKind         `json:"configKinds"`
	Registries       []Registry           `json:"registries"`
	Services         []Service            `json:"services"`
	Instances        []DubbodInstance     `json:"instances"`
	DataPlane        []Workload           `json:"dataPlane"`
	DataPlanePods    map[string]int       `json:"dataPlanePods,omitempty"`
	Routes           []Route              `json:"routes"`
	XDSClients       []XDSClient          `json:"xdsClients"`
	GatewayInstances []DubbodInstance     `json:"gatewayInstances"`
	ControlPlanes    []ControlPlaneStatus `json:"controlPlanes"`
	Clusters         []ClusterSummary     `json:"clusters"`
	Scope            string               `json:"scope"`
	UpdatedAt        time.Time            `json:"updatedAt"`
}

type ClusterSummary struct {
	ID               string `json:"id"`
	ControlPlanes    int    `json:"controlPlanes"`
	HealthyInstances int    `json:"healthyInstances"`
}

type OverviewMesh struct {
	TrustDomain      string `json:"trustDomain,omitempty"`
	RootNamespace    string `json:"rootNamespace,omitempty"`
	DiscoveryAddress string `json:"discoveryAddress,omitempty"`
}

type OverviewServer struct {
	GUIPath           string `json:"guiPath,omitempty"`
	HTTPAddress       string `json:"httpAddress,omitempty"`
	GRPCAddress       string `json:"grpcAddress,omitempty"`
	SecureGRPCAddress string `json:"secureGrpcAddress,omitempty"`
	OverviewPath      string `json:"overviewPath,omitempty"`
	MetricsPath       string `json:"metricsPath,omitempty"`
	VersionPath       string `json:"versionPath,omitempty"`
	ReadyPath         string `json:"readyPath,omitempty"`
}

type OverviewStatus struct {
	XDSServerReady  bool `json:"xdsServerReady"`
	CachesSynced    bool `json:"cachesSynced"`
	ServicesSynced  bool `json:"servicesSynced"`
	ConfigSynced    bool `json:"configSynced"`
	ProxylessSynced bool `json:"proxylessSynced"`
	InjectorReady   bool `json:"injectorReady"`
	ValidationReady bool `json:"validationReady"`
}

type OverviewCounts struct {
	Services               int `json:"services"`
	EndpointServices       int `json:"endpointServices"`
	XDSConnections         int `json:"xdsConnections"`
	Registries             int `json:"registries"`
	PeerAuthentications    int `json:"peerAuthentications"`
	RequestAuthentications int `json:"requestAuthentications"`
	AuthorizationPolicies  int `json:"authorizationPolicies"`
	HTTPRoutes             int `json:"httpRoutes"`
	GatewayClasses         int `json:"gatewayClasses"`
	Gateways               int `json:"gateways"`
}

type ConfigKind struct {
	Kind        string `json:"kind"`
	Count       int    `json:"count"`
	Description string `json:"description"`
	Cluster     string `json:"clusterId,omitempty"`
}

type Registry struct {
	Provider string `json:"provider"`
	Cluster  string `json:"cluster"`
	Synced   bool   `json:"synced"`
}

type Service struct {
	Name            string `json:"name"`
	Hostname        string `json:"hostname"`
	Namespace       string `json:"namespace"`
	Registry        string `json:"registry"`
	Ports           string `json:"ports"`
	Exposure        string `json:"exposure"`
	ServiceAccounts int    `json:"serviceAccounts"`
	DefaultAddress  string `json:"defaultAddress,omitempty"`
	MeshExternal    bool   `json:"meshExternal"`
	MTLSMode        string `json:"mtlsMode,omitempty"`
	MTLSFromPolicy  bool   `json:"mtlsFromPolicy"`
	ControlPlane    string `json:"controlPlane,omitempty"`
	Cluster         string `json:"clusterId,omitempty"`
}

type DubbodInstance struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	IP              string `json:"ip"`
	IsReady         bool   `json:"isReady"`
	GatewayClass    string `json:"gatewayClass,omitempty"`
	GatewayName     string `json:"gatewayName,omitempty"`
	ReadyReplicas   int32  `json:"readyReplicas,omitempty"`
	DesiredReplicas int32  `json:"desiredReplicas,omitempty"`
	ControlPlane    string `json:"controlPlane,omitempty"`
	Cluster         string `json:"clusterId,omitempty"`
}

type Workload struct {
	Name           string     `json:"name"`
	Namespace      string     `json:"namespace"`
	IP             string     `json:"ip,omitempty"`
	Phase          string     `json:"phase"`
	Ready          bool       `json:"ready"`
	SidecarReady   bool       `json:"sidecarReady"`
	ServiceAccount string     `json:"serviceAccount,omitempty"`
	Image          string     `json:"image,omitempty"`
	Inbound        string     `json:"inbound,omitempty"`
	Upstream       string     `json:"upstream,omitempty"`
	XDSAddress     string     `json:"xdsAddress,omitempty"`
	Restarts       int32      `json:"restarts"`
	MTLSModes      []string   `json:"mtlsModes,omitempty"`
	CertExpiresAt  *time.Time `json:"certExpiresAt,omitempty"`
	CertRootActive bool       `json:"certRootActive"`
	ConfigError    string     `json:"configError,omitempty"`
	Connected      bool       `json:"connected"`
	NodeID         string     `json:"nodeId,omitempty"`
	NodeType       string     `json:"nodeType,omitempty"`
	ConnectedAt    *time.Time `json:"connectedAt,omitempty"`
	Watched        []string   `json:"watched,omitempty"`
	ControlPlane   string     `json:"controlPlane,omitempty"`
	Cluster        string     `json:"clusterId,omitempty"`
}

type XDSClient struct {
	NodeID       string    `json:"nodeId"`
	NodeType     string    `json:"nodeType,omitempty"`
	Peer         string    `json:"peer,omitempty"`
	ConnectedAt  time.Time `json:"connectedAt"`
	Watched      []string  `json:"watched,omitempty"`
	ControlPlane string    `json:"controlPlane,omitempty"`
	Cluster      string    `json:"clusterId,omitempty"`
}

type Route struct {
	Name         string      `json:"name"`
	Namespace    string      `json:"namespace"`
	Parents      []string    `json:"parents,omitempty"`
	Hostnames    []string    `json:"hostnames,omitempty"`
	Rules        []RouteRule `json:"rules,omitempty"`
	ControlPlane string      `json:"controlPlane,omitempty"`
	Cluster      string      `json:"clusterId,omitempty"`
}

type RouteRule struct {
	Match    string         `json:"match,omitempty"`
	Backends []RouteBackend `json:"backends,omitempty"`
}

type RouteBackend struct {
	Name   string `json:"name"`
	Port   int32  `json:"port,omitempty"`
	Weight int32  `json:"weight,omitempty"`
}

type MetricsResponse struct {
	Families      []MetricFamily       `json:"families"`
	ControlPlanes []ControlPlaneStatus `json:"controlPlanes,omitempty"`
	Scope         string               `json:"scope,omitempty"`
	UpdatedAt     time.Time            `json:"updatedAt"`
}

type MetricFamily struct {
	Name    string         `json:"name"`
	Help    string         `json:"help,omitempty"`
	Type    string         `json:"type"`
	Metrics []MetricSample `json:"metrics"`
}

type MetricSample struct {
	Labels  map[string]string `json:"labels,omitempty"`
	Value   *float64          `json:"value,omitempty"`
	Count   *uint64           `json:"count,omitempty"`
	Sum     *float64          `json:"sum,omitempty"`
	Buckets []MetricBucket    `json:"buckets,omitempty"`
}

type MetricBucket struct {
	LE    float64 `json:"le"`
	Count uint64  `json:"count"`
}

type LogsResponse struct {
	Kind      string    `json:"kind"`
	Name      string    `json:"name"`
	Namespace string    `json:"namespace"`
	Pods      []PodLog  `json:"pods"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type PodLog struct {
	Name         string `json:"name"`
	Container    string `json:"container"`
	Phase        string `json:"phase"`
	Ready        bool   `json:"ready"`
	Logs         string `json:"logs,omitempty"`
	Error        string `json:"error,omitempty"`
	ControlPlane string `json:"controlPlane,omitempty"`
	Cluster      string `json:"clusterId,omitempty"`
}
