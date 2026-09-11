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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type ServerConfig struct {
	BasePath string
	Product  string
	Assets   fs.FS
}

type templateData struct {
	BasePath   string
	ConfigJSON template.JS
}

func NewHandler(config ServerConfig, aggregator *Aggregator) (http.Handler, error) {
	basePath := normalizeBasePath(config.BasePath)
	payload, err := json.Marshal(map[string]string{
		"basePath": basePath,
		"product":  config.Product,
	})
	if err != nil {
		return nil, err
	}
	indexTemplate, err := template.ParseFS(config.Assets, "index.html")
	if err != nil {
		return nil, err
	}
	var index bytes.Buffer
	assetBasePath := basePath
	if assetBasePath == "/" {
		assetBasePath = ""
	}
	if err := indexTemplate.Execute(&index, templateData{
		BasePath:   assetBasePath,
		ConfigJSON: template.JS(payload),
	}); err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	apiPath := func(resource string) string { return path.Join(basePath, "api", resource) }

	mux.HandleFunc(apiPath("overview"), func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
		defer cancel()
		overview, fetchErr := aggregator.Overview(ctx, request.URL.Query().Get("controlPlane"))
		writeJSON(writer, overview, fetchErr)
	})
	mux.HandleFunc(apiPath("metrics"), func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
		defer cancel()
		metrics, fetchErr := aggregator.Metrics(ctx, request.URL.Query().Get("controlPlane"))
		writeJSON(writer, metrics, fetchErr)
	})
	mux.HandleFunc(apiPath("logs"), func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		query := cloneQuery(request.URL.Query())
		scope := query.Get("controlPlane")
		query.Del("controlPlane")
		ctx, cancel := context.WithTimeout(request.Context(), 20*time.Second)
		defer cancel()
		logs, fetchErr := aggregator.Logs(ctx, scope, query)
		writeJSON(writer, logs, fetchErr)
	})
	mux.HandleFunc(apiPath("control-planes"), func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
		defer cancel()
		overview, fetchErr := aggregator.Overview(ctx, AllControlPlanes)
		writeJSON(writer, map[string]any{
			"controlPlanes": overview.ControlPlanes,
			"clusters":      overview.Clusters,
			"updatedAt":     overview.UpdatedAt,
		}, fetchErr)
	})

	healthPath := path.Join(basePath, "healthz")
	readyPath := path.Join(basePath, "readyz")
	mux.HandleFunc(healthPath, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc(readyPath, func(writer http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
		defer cancel()
		if _, err := aggregator.Overview(ctx, AllControlPlanes); err != nil {
			http.Error(writer, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = writer.Write([]byte("ready\n"))
	})

	staticPrefix := basePath
	if staticPrefix == "/" {
		staticPrefix = ""
	}
	fileServer := http.FileServer(http.FS(config.Assets))
	mux.Handle(staticPrefix+"/", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestPath := strings.TrimPrefix(request.URL.Path, staticPrefix+"/")
		if requestPath != "" && requestPath != "." {
			if _, openErr := config.Assets.Open(path.Clean(requestPath)); openErr == nil {
				if strings.HasSuffix(requestPath, ".js") || strings.HasSuffix(requestPath, ".css") || strings.HasSuffix(requestPath, ".png") {
					writer.Header().Set("Cache-Control", "public, max-age=3600")
				}
				cloned := request.Clone(request.Context())
				cloned.URL.Path = "/" + requestPath
				fileServer.ServeHTTP(writer, cloned)
				return
			}
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		writer.Header().Set("Cache-Control", "no-cache")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(index.Bytes())
	}))

	if basePath != "/" {
		mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/" {
				http.NotFound(writer, request)
				return
			}
			http.Redirect(writer, request, basePath+"/", http.StatusTemporaryRedirect)
		})
	}
	return mux, nil
}

func normalizeBasePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "/" {
		return "/"
	}
	return "/" + strings.Trim(strings.TrimPrefix(value, "/"), "/")
}

func cloneQuery(source url.Values) url.Values {
	out := make(url.Values, len(source))
	for key, values := range source {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func writeJSON(writer http.ResponseWriter, value any, err error) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err != nil {
		writer.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
		return
	}
	if encodeErr := json.NewEncoder(writer).Encode(value); encodeErr != nil {
		http.Error(writer, fmt.Sprintf("encode response: %v", encodeErr), http.StatusInternalServerError)
	}
}
