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
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestHandlerServesStandaloneAssetsAndAggregatedAPI(t *testing.T) {
	controlPlane := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/overview" {
			http.NotFound(writer, request)
			return
		}
		writeTestOverview(writer, testOverview("cluster-a", "dubbod-a", "node-a"))
	}))
	defer controlPlane.Close()

	discovery := NewDiscovery(StaticProvider{{
		ID: "static/a", Name: "a", URL: controlPlane.URL, Source: "static",
	}})
	discovery.Refresh(context.Background())
	aggregator := &Aggregator{Discovery: discovery, Client: NewClient(time.Second)}
	assets := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte(`<base href="{{ .BasePath }}/"><script id="dubbod-console-config">{{ .ConfigJSON }}</script>`)},
		"app.js":     &fstest.MapFile{Data: []byte(`console.log("ok")`)},
	}
	handler, err := NewHandler(ServerConfig{BasePath: "/console", Product: "dubbod GUI", Assets: fs.FS(assets)}, aggregator)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	indexResponse := httptest.NewRecorder()
	handler.ServeHTTP(indexResponse, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if indexResponse.Code != http.StatusOK || !strings.Contains(indexResponse.Body.String(), `/console/`) {
		t.Fatalf("index = %d %q", indexResponse.Code, indexResponse.Body.String())
	}

	apiResponse := httptest.NewRecorder()
	handler.ServeHTTP(apiResponse, httptest.NewRequest(http.MethodGet, "/console/api/overview?controlPlane=all", nil))
	if apiResponse.Code != http.StatusOK || !strings.Contains(apiResponse.Body.String(), `"controlPlanes"`) {
		t.Fatalf("api = %d %q", apiResponse.Code, apiResponse.Body.String())
	}
}

func TestHandlerRendersRootBasePath(t *testing.T) {
	discovery := NewDiscovery()
	aggregator := &Aggregator{Discovery: discovery, Client: NewClient(time.Second)}
	assets := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte(`<base href="{{ .BasePath }}/">`)},
	}
	handler, err := NewHandler(ServerConfig{BasePath: "/", Product: "dubbod GUI", Assets: fs.FS(assets)}, aggregator)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if body := response.Body.String(); !strings.Contains(body, `<base href="/">`) {
		t.Fatalf("root base path = %q", body)
	}
}
