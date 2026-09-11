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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Provider interface {
	Discover(context.Context) ([]Endpoint, error)
}

type StaticProvider []Endpoint

func (provider StaticProvider) Discover(context.Context) ([]Endpoint, error) {
	out := append([]Endpoint(nil), provider...)
	return out, nil
}

type Discovery struct {
	providers []Provider
	mu        sync.RWMutex
	endpoints []Endpoint
	errs      []error
}

func NewDiscovery(providers ...Provider) *Discovery {
	return &Discovery{providers: providers}
}

func (discovery *Discovery) Refresh(ctx context.Context) {
	var endpoints []Endpoint
	var errs []error
	for _, provider := range discovery.providers {
		found, err := provider.Discover(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		endpoints = append(endpoints, found...)
	}

	byID := make(map[string]Endpoint, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.ID == "" || endpoint.URL == "" {
			continue
		}
		byID[endpoint.ID] = endpoint
	}
	endpoints = endpoints[:0]
	for _, endpoint := range byID {
		endpoints = append(endpoints, endpoint)
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].ID < endpoints[j].ID })

	discovery.mu.Lock()
	discovery.endpoints = endpoints
	discovery.errs = errs
	discovery.mu.Unlock()
}

func (discovery *Discovery) Run(ctx context.Context, interval time.Duration) {
	discovery.Refresh(ctx)
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			discovery.Refresh(ctx)
		}
	}
}

func (discovery *Discovery) Endpoints() []Endpoint {
	discovery.mu.RLock()
	defer discovery.mu.RUnlock()
	return append([]Endpoint(nil), discovery.endpoints...)
}

func (discovery *Discovery) Errors() []error {
	discovery.mu.RLock()
	defer discovery.mu.RUnlock()
	return append([]error(nil), discovery.errs...)
}

func ParseEndpoint(raw string) (Endpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Endpoint{}, errors.New("endpoint is empty")
	}
	name := ""
	address := raw
	if index := strings.Index(raw, "="); index > 0 {
		name = strings.TrimSpace(raw[:index])
		address = strings.TrimSpace(raw[index+1:])
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return Endpoint{}, fmt.Errorf("invalid endpoint %q", raw)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Endpoint{}, fmt.Errorf("unsupported endpoint scheme %q", parsed.Scheme)
	}
	if parsed.User != nil {
		return Endpoint{}, errors.New("endpoint URL must not contain credentials")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	if name == "" {
		name = parsed.Host
	}
	return Endpoint{
		ID:     "static/" + slug(name),
		Name:   name,
		URL:    parsed.String(),
		Source: "static",
	}, nil
}

func slug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var out strings.Builder
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.' {
			out.WriteRune(char)
		} else {
			out.WriteByte('-')
		}
	}
	return strings.Trim(out.String(), "-")
}

type KubernetesProvider struct {
	APIServer    string
	Namespace    string
	Service      string
	PortName     string
	Scheme       string
	TokenFile    string
	CAFile       string
	Client       *http.Client
	EndpointPath string
	Context      string
	UsePodProxy  bool
}

func KubeconfigProviders(kubeconfig string, contexts []string, namespace, service, portName string) ([]Provider, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	raw, err := rules.Load()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	if len(contexts) == 0 {
		if raw.CurrentContext == "" {
			return nil, errors.New("kubeconfig has no current context; pass --context")
		}
		contexts = []string{raw.CurrentContext}
	}

	providers := make([]Provider, 0, len(contexts))
	for _, contextName := range contexts {
		if _, exists := raw.Contexts[contextName]; !exists {
			return nil, fmt.Errorf("kubeconfig context %q does not exist", contextName)
		}
		overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
		config, configErr := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
		if configErr != nil {
			return nil, fmt.Errorf("load kubeconfig context %q: %w", contextName, configErr)
		}
		transport, transportErr := rest.TransportFor(config)
		if transportErr != nil {
			return nil, fmt.Errorf("build Kubernetes transport for %q: %w", contextName, transportErr)
		}
		providers = append(providers, &KubernetesProvider{
			APIServer:   strings.TrimRight(config.Host, "/"),
			Namespace:   namespace,
			Service:     service,
			PortName:    portName,
			Scheme:      "http",
			Client:      &http.Client{Transport: transport, Timeout: 10 * time.Second},
			Context:     contextName,
			UsePodProxy: true,
		})
	}
	return providers, nil
}

func InClusterKubernetesProvider(namespace, service, portName, scheme, endpointPath string) (*KubernetesProvider, bool, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	portValue := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || portValue == "" {
		return nil, false, nil
	}
	return NewKubernetesProvider(
		"https://"+net.JoinHostPort(host, portValue),
		namespace,
		service,
		portName,
		scheme,
		endpointPath,
		"/var/run/secrets/kubernetes.io/serviceaccount/token",
		"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
	)
}

func NewKubernetesProvider(apiServer, namespace, service, portName, scheme, endpointPath, tokenFile, caFile string) (*KubernetesProvider, bool, error) {
	if apiServer == "" {
		return nil, false, nil
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caFile != "" {
		ca, err := os.ReadFile(caFile)
		if err != nil {
			return nil, false, fmt.Errorf("read Kubernetes CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, false, errors.New("parse Kubernetes CA")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &KubernetesProvider{
		APIServer:    strings.TrimRight(apiServer, "/"),
		Namespace:    namespace,
		Service:      service,
		PortName:     portName,
		Scheme:       scheme,
		TokenFile:    tokenFile,
		CAFile:       caFile,
		Client:       &http.Client{Transport: transport, Timeout: 5 * time.Second},
		EndpointPath: endpointPath,
	}, true, nil
}

type endpointSliceList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Ports []struct {
			Name *string `json:"name"`
			Port *int32  `json:"port"`
		} `json:"ports"`
		Endpoints []struct {
			Addresses  []string `json:"addresses"`
			Conditions struct {
				Ready *bool `json:"ready"`
			} `json:"conditions"`
			TargetRef *struct {
				Name string `json:"name"`
			} `json:"targetRef"`
		} `json:"endpoints"`
	} `json:"items"`
}

func (provider *KubernetesProvider) Discover(ctx context.Context) ([]Endpoint, error) {
	label := url.QueryEscape("kubernetes.io/service-name=" + provider.Service)
	apiPath := "/apis/discovery.k8s.io/v1/namespaces/" + url.PathEscape(provider.Namespace) +
		"/endpointslices?labelSelector=" + label
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, provider.APIServer+apiPath, nil)
	if err != nil {
		return nil, err
	}
	if provider.TokenFile != "" {
		token, readErr := os.ReadFile(provider.TokenFile)
		if readErr != nil {
			return nil, fmt.Errorf("read Kubernetes token: %w", readErr)
		}
		request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	response, err := provider.Client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("discover Kubernetes EndpointSlices: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return nil, fmt.Errorf("discover Kubernetes EndpointSlices: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	var list endpointSliceList
	if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decode Kubernetes EndpointSlices: %w", err)
	}

	var endpoints []Endpoint
	for _, slice := range list.Items {
		port := int32(0)
		for _, candidate := range slice.Ports {
			if candidate.Port == nil {
				continue
			}
			if provider.PortName == "" || candidate.Name != nil && *candidate.Name == provider.PortName {
				port = *candidate.Port
				break
			}
		}
		if port == 0 {
			continue
		}
		for _, candidate := range slice.Endpoints {
			if candidate.Conditions.Ready != nil && !*candidate.Conditions.Ready {
				continue
			}
			for _, address := range candidate.Addresses {
				name := address
				if candidate.TargetRef != nil && candidate.TargetRef.Name != "" {
					name = candidate.TargetRef.Name
				}
				base := provider.Scheme + "://" + net.JoinHostPort(address, strconv.Itoa(int(port)))
				if provider.UsePodProxy && candidate.TargetRef != nil && candidate.TargetRef.Name != "" {
					base = provider.APIServer + "/api/v1/namespaces/" + url.PathEscape(provider.Namespace) +
						"/pods/" + url.PathEscape(candidate.TargetRef.Name+":"+strconv.Itoa(int(port))) + "/proxy"
				} else if provider.EndpointPath != "" {
					base += "/" + strings.TrimPrefix(path.Clean("/"+provider.EndpointPath), "/")
				}
				source := "kubernetes"
				idPrefix := "kubernetes"
				if provider.Context != "" {
					source = "kube-context/" + provider.Context
					idPrefix += "/" + slug(provider.Context)
				}
				endpoints = append(endpoints, Endpoint{
					ID:     idPrefix + "/" + slug(provider.Namespace) + "/" + slug(provider.Service) + "/" + slug(name),
					Name:   name,
					URL:    strings.TrimRight(base, "/"),
					Source: source,
					client: provider.Client,
				})
			}
		}
	}
	return endpoints, nil
}
