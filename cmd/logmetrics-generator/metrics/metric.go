package metrics

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type MetricType int

const (
	None MetricType = iota
	Counter
	Gauge
	Histogram
	Summary
)

var promTypeHandler = map[MetricType]func(metric *Metric) error{
	Counter:   counterHandler,
	Gauge:     gaugeHandler,
	Histogram: histogramHandler,
	Summary:   summaryHandler,
}

// Metric defines a metric object
type Metric struct {
	Type        MetricType
	Name        string
	Description string
	Labels      []string
	Buckets     []float64
	Objectives  map[float64]float64

	vec prometheus.Collector
}

// Set data for Gauge type Metric.
func (m *Metric) Set(labelValues []string, value float64) error {
	if m.Type == None {
		return errors.Errorf("metric '%s' not existed.", m.Name)
	}

	if m.Type != Gauge {
		return errors.Errorf("metric '%s' not Gauge type", m.Name)
	}
	m.vec.(*prometheus.GaugeVec).WithLabelValues(labelValues...).Set(value)
	return nil
}

// Inc increases value for Counter/Gauge type metric, increments
// the counter by 1
func (m *Metric) Inc(labelValues []string) error {
	if m.Type == None {
		return errors.Errorf("metric '%s' not existed.", m.Name)
	}

	if m.Type != Gauge && m.Type != Counter {
		return errors.Errorf("metric '%s' not Gauge or Counter type", m.Name)
	}
	switch m.Type {
	case Counter:
		m.vec.(*prometheus.CounterVec).WithLabelValues(labelValues...).Inc()
	case Gauge:
		m.vec.(*prometheus.GaugeVec).WithLabelValues(labelValues...).Inc()
	}
	return nil
}

// Add adds the given value to the Metric object. Only
// for Counter/Gauge type metric.
func (m *Metric) Add(labelValues []string, value float64) error {
	if m.Type == None {
		return errors.Errorf("metric '%s' not existed.", m.Name)
	}

	if m.Type != Gauge && m.Type != Counter {
		return errors.Errorf("metric '%s' not Gauge or Counter type", m.Name)
	}
	switch m.Type {
	case Counter:
		if value < 0 {
			return errors.Errorf("metric '%s' value cannot be negative", m.Name)
		}
		m.vec.(*prometheus.CounterVec).WithLabelValues(labelValues...).Add(value)
	case Gauge:
		m.vec.(*prometheus.GaugeVec).WithLabelValues(labelValues...).Add(value)
	}
	return nil
}

// Observe is used by Histogram and Summary type metric to
// add observations.
func (m *Metric) Observe(labelValues []string, value float64) error {
	if m.Type == 0 {
		return errors.Errorf("metric '%s' not existed.", m.Name)
	}
	if m.Type != Histogram && m.Type != Summary {
		return errors.Errorf("metric '%s' not Histogram or Summary type", m.Name)
	}
	switch m.Type {
	case Histogram:
		m.vec.(*prometheus.HistogramVec).WithLabelValues(labelValues...).Observe(value)
	case Summary:
		m.vec.(*prometheus.SummaryVec).WithLabelValues(labelValues...).Observe(value)
	}
	return nil
}

func counterHandler(metric *Metric) error {
	metric.vec = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: metric.Name, Help: metric.Description},
		metric.Labels,
	)
	return nil
}

func gaugeHandler(metric *Metric) error {
	metric.vec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: metric.Name, Help: metric.Description},
		metric.Labels,
	)
	return nil
}

func histogramHandler(metric *Metric) error {
	if len(metric.Buckets) == 0 {
		return errors.Errorf("metric '%s' is histogram type, cannot lose bucket param.", metric.Name)
	}
	metric.vec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: metric.Name, Help: metric.Description, Buckets: metric.Buckets},
		metric.Labels,
	)
	return nil
}

func summaryHandler(metric *Metric) error {
	if len(metric.Objectives) == 0 {
		return errors.Errorf("metric '%s' is summary type, cannot lose objectives param.", metric.Name)
	}
	metric.vec = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{Name: metric.Name, Help: metric.Description, Objectives: metric.Objectives},
		metric.Labels,
	)
	return nil
}
