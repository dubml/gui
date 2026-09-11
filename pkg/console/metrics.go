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
	"sort"
	"strings"
	"time"
)

// sampleKey identifies one time series: a family plus its label set.
type sampleKey struct {
	family string
	labels string
}

// aggregateMetrics sums matching series across control planes. Counters,
// histogram counts and bucket counts add; the result is fleet-wide totals
// rather than per-replica values.
func aggregateMetrics(responses []MetricsResponse, statuses []ControlPlaneStatus, scope string) MetricsResponse {
	families := map[string]MetricFamily{}
	samples := map[sampleKey]MetricSample{}
	for _, response := range responses {
		for _, family := range response.Families {
			if _, exists := families[family.Name]; !exists {
				families[family.Name] = MetricFamily{Name: family.Name, Help: family.Help, Type: family.Type}
			}
			for _, sample := range family.Metrics {
				key := sampleKey{family: family.Name, labels: labelsKey(sample.Labels)}
				current := samples[key]
				if current.Labels == nil && sample.Labels != nil {
					current.Labels = copyLabels(sample.Labels)
				}
				current.Value = addFloat(current.Value, sample.Value)
				current.Count = addUint(current.Count, sample.Count)
				current.Sum = addFloat(current.Sum, sample.Sum)
				current.Buckets = addBuckets(current.Buckets, sample.Buckets)
				samples[key] = current
			}
		}
	}

	out := MetricsResponse{
		ControlPlanes: statuses,
		Scope:         scope,
		UpdatedAt:     time.Now().UTC(),
	}
	for name, family := range families {
		for key, sample := range samples {
			if key.family == name {
				family.Metrics = append(family.Metrics, sample)
			}
		}
		sort.Slice(family.Metrics, func(i, j int) bool {
			return labelsKey(family.Metrics[i].Labels) < labelsKey(family.Metrics[j].Labels)
		})
		out.Families = append(out.Families, family)
	}
	sort.Slice(out.Families, func(i, j int) bool { return out.Families[i].Name < out.Families[j].Name })
	return out
}

// labelsKey renders a label set as a stable string so equal sets from different
// control planes hash to the same series.
func labelsKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	for _, key := range keys {
		out.WriteString(key)
		out.WriteByte('=')
		out.WriteString(labels[key])
		out.WriteByte(0)
	}
	return out.String()
}

func copyLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		out[key] = value
	}
	return out
}

// addFloat and addUint keep nil meaning "this response carried no such value",
// so a gauge absent everywhere stays absent instead of becoming zero.
func addFloat(left, right *float64) *float64 {
	if left == nil && right == nil {
		return nil
	}
	value := float64(0)
	if left != nil {
		value += *left
	}
	if right != nil {
		value += *right
	}
	return &value
}

func addUint(left, right *uint64) *uint64 {
	if left == nil && right == nil {
		return nil
	}
	value := uint64(0)
	if left != nil {
		value += *left
	}
	if right != nil {
		value += *right
	}
	return &value
}

func addBuckets(left, right []MetricBucket) []MetricBucket {
	if len(left) == 0 {
		return append([]MetricBucket(nil), right...)
	}
	byBound := make(map[float64]uint64, len(left)+len(right))
	for _, bucket := range left {
		byBound[bucket.LE] += bucket.Count
	}
	for _, bucket := range right {
		byBound[bucket.LE] += bucket.Count
	}
	out := make([]MetricBucket, 0, len(byBound))
	for bound, count := range byBound {
		out = append(out, MetricBucket{LE: bound, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LE < out[j].LE })
	return out
}
