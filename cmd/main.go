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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kdubbo/gui/pkg/console"
	"github.com/kdubbo/gui/web"
)

type endpointFlags []string

func (values *endpointFlags) String() string {
	return strings.Join(*values, ",")
}

func (values *endpointFlags) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	if os.Getenv("DUBBOCTL_GUI_LAUNCH") != "1" {
		fmt.Fprintln(os.Stderr, "dubbod-console must be started with `dubboctl gui`")
		os.Exit(2)
	}

	var endpoints endpointFlags
	var contexts endpointFlags
	listen := flag.String("listen", ":8080", "HTTP listen address")
	basePath := flag.String("base-path", "/", "Console HTTP base path")
	refresh := flag.Duration("discovery-refresh", 15*time.Second, "Control-plane discovery refresh interval")
	timeout := flag.Duration("upstream-timeout", 5*time.Second, "Per-control-plane request timeout")
	kubeAPI := flag.String("kube-api", "", "Kubernetes API URL; in-cluster discovery is automatic when empty")
	kubeconfig := flag.String("kubeconfig", "", "Kubeconfig used for local multi-context discovery")
	discoverKubeconfig := flag.Bool("discover-kubeconfig", false, "Discover from the default kubeconfig")
	kubeNamespace := flag.String("kube-namespace", "dubbo-system", "Namespace containing the management Service")
	kubeService := flag.String("kube-service", "dubbod-management", "Service whose EndpointSlices identify control planes")
	kubePortName := flag.String("kube-port-name", "management", "EndpointSlice port name")
	kubeScheme := flag.String("kube-endpoint-scheme", "http", "Discovered endpoint URL scheme")
	kubeToken := flag.String("kube-token-file", "", "Kubernetes bearer token file")
	kubeCA := flag.String("kube-ca-file", "", "Kubernetes CA file")
	flag.Var(&endpoints, "endpoint", "Static cross-control-plane endpoint as name=http://host:port; repeatable")
	flag.Var(&contexts, "context", "Kubeconfig context to discover; repeatable for cross-control-plane views")
	flag.Parse()

	var providers []console.Provider
	var static console.StaticProvider
	for _, raw := range endpoints {
		endpoint, err := console.ParseEndpoint(raw)
		if err != nil {
			log.Fatalf("invalid --endpoint: %v", err)
		}
		static = append(static, endpoint)
	}
	if len(static) > 0 {
		providers = append(providers, static)
	}

	var err error
	if *discoverKubeconfig || *kubeconfig != "" || len(contexts) > 0 {
		kubeProviders, providerErr := console.KubeconfigProviders(
			*kubeconfig,
			contexts,
			*kubeNamespace,
			*kubeService,
			*kubePortName,
		)
		if providerErr != nil {
			log.Fatalf("configure kubeconfig discovery: %v", providerErr)
		}
		providers = append(providers, kubeProviders...)
	} else if *kubeAPI != "" {
		kubeProvider, enabled, providerErr := console.NewKubernetesProvider(
			*kubeAPI,
			*kubeNamespace,
			*kubeService,
			*kubePortName,
			*kubeScheme,
			"",
			*kubeToken,
			*kubeCA,
		)
		if providerErr != nil {
			log.Fatalf("configure Kubernetes discovery: %v", providerErr)
		}
		if enabled {
			providers = append(providers, kubeProvider)
		}
	} else {
		kubeProvider, enabled, providerErr := console.InClusterKubernetesProvider(
			*kubeNamespace,
			*kubeService,
			*kubePortName,
			*kubeScheme,
			"",
		)
		if providerErr != nil {
			log.Fatalf("configure Kubernetes discovery: %v", providerErr)
		}
		if enabled {
			providers = append(providers, kubeProvider)
		}
	}

	discovery := console.NewDiscovery(providers...)
	client := console.NewClient(*timeout)
	aggregator := &console.Aggregator{Discovery: discovery, Client: client}
	handler, err := console.NewHandler(console.ServerConfig{
		BasePath: *basePath,
		Product:  "dubbod GUI",
		Assets:   web.FS(),
	}, aggregator)
	if err != nil {
		log.Fatalf("initialize console: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go discovery.Run(ctx, *refresh)
	discovery.Refresh(ctx)

	server := &http.Server{
		Addr:              *listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if shutdownErr := server.Shutdown(shutdownContext); shutdownErr != nil {
			log.Printf("shutdown: %v", shutdownErr)
		}
	}()

	log.Printf("dubbod-console listening on %s with %d discovered endpoint(s)", *listen, len(discovery.Endpoints()))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
