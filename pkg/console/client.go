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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type Client struct {
	HTTP       *http.Client
	PrimaryAPI string
	LegacyAPI  string
}

func NewClient(timeout time.Duration) *Client {
	return &Client{
		HTTP:       &http.Client{Timeout: timeout},
		PrimaryAPI: "/api/v1",
		LegacyAPI:  "/gui/api",
	}
}

func (client *Client) Overview(ctx context.Context, endpoint Endpoint) (Overview, time.Duration, error) {
	var overview Overview
	start := time.Now()
	err := client.getWithFallback(ctx, endpoint, "overview", nil, &overview)
	return overview, time.Since(start), err
}

func (client *Client) Metrics(ctx context.Context, endpoint Endpoint) (MetricsResponse, error) {
	var metrics MetricsResponse
	err := client.getWithFallback(ctx, endpoint, "metrics", nil, &metrics)
	return metrics, err
}

func (client *Client) Logs(ctx context.Context, endpoint Endpoint, query url.Values) (LogsResponse, error) {
	var logs LogsResponse
	err := client.getWithFallback(ctx, endpoint, "logs", query, &logs)
	return logs, err
}

func (client *Client) getWithFallback(ctx context.Context, endpoint Endpoint, resource string, query url.Values, target any) error {
	paths := []string{client.PrimaryAPI}
	if client.LegacyAPI != "" && client.LegacyAPI != client.PrimaryAPI {
		paths = append(paths, client.LegacyAPI)
	}
	var lastErr error
	for _, apiPath := range paths {
		if err := client.get(ctx, endpoint, apiPath, resource, query, target); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func (client *Client) get(ctx context.Context, endpoint Endpoint, apiPath, resource string, query url.Values, target any) error {
	base, err := url.Parse(endpoint.URL)
	if err != nil {
		return err
	}
	base.Path = path.Join(base.Path, apiPath, resource)
	base.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return err
	}
	httpClient := client.HTTP
	if endpoint.client != nil {
		httpClient = endpoint.client
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("%s: %w", endpoint.Name, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("%s: HTTP %d: %s", endpoint.Name, response.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("%s: decode response: %w", endpoint.Name, err)
	}
	return nil
}
