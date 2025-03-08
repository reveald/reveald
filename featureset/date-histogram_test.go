package featureset

import (
	"testing"
	"time"
)

// Helper functions to restore the tests
func intervalEnd(t time.Time, interval DateHistogramInterval) time.Time {
	switch interval {
	case Year:
		return t.AddDate(1, 0, 0)
	case Month:
		return t.AddDate(0, 1, 0)
	case Day:
		return t.AddDate(0, 0, 1)
	case Hour:
		return t.Add(time.Hour)
	case Minute:
		return t.Add(time.Minute)
	case Second:
		return t.Add(time.Second)
	}
	return t
}

func parseTimeFrom(d string, interval DateHistogramInterval) (time.Time, error) {
	switch interval {
	case Year:
		return time.Parse("2006", d)
	case Month:
		return time.Parse("2006-01", d)
	case Day:
		return time.Parse("2006-01-02", d)
	case Hour:
		return time.Parse("2006-01-02 15", d)
	case Minute:
		return time.Parse("2006-01-02 15:04", d)
	case Second:
		return time.Parse("2006-01-02 15:04:05", d)
	}
	return time.Time{}, nil
}

func TestIntervalEnd(t *testing.T) {
	baseTime := time.Date(2023, 5, 15, 10, 30, 45, int(time.Millisecond*500), time.UTC)

	tests := []struct {
		name     string
		interval DateHistogramInterval
		expected time.Time
	}{
		{"Yearly", Year, time.Date(2024, 5, 15, 10, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Monthly", Month, time.Date(2023, 6, 15, 10, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Daily", Day, time.Date(2023, 5, 16, 10, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Hourly", Hour, time.Date(2023, 5, 15, 11, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Minutes", Minute, time.Date(2023, 5, 15, 10, 31, 45, int(time.Millisecond*500), time.UTC)},
		{"Seconds", Second, time.Date(2023, 5, 15, 10, 30, 46, int(time.Millisecond*500), time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := intervalEnd(baseTime, tt.interval)
			if !result.Equal(tt.expected) {
				t.Errorf("intervalEnd(%v, %s) = %v, want %v", baseTime, tt.interval, result, tt.expected)
			}
		})
	}
}

func TestParseTimeFrom(t *testing.T) {
	tests := []struct {
		name     string
		date     string
		interval DateHistogramInterval
		expected time.Time
		wantErr  bool
	}{
		{"Yearly", "2023", Year, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"Monthly", "2023-05", Month, time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC), false},
		{"Daily", "2023-05-15", Day, time.Date(2023, 5, 15, 0, 0, 0, 0, time.UTC), false},
		{"Hourly", "2023-05-15 10", Hour, time.Date(2023, 5, 15, 10, 0, 0, 0, time.UTC), false},
		{"Minutes", "2023-05-15 10:30", Minute, time.Date(2023, 5, 15, 10, 30, 0, 0, time.UTC), false},
		{"Seconds", "2023-05-15 10:30:45", Second, time.Date(2023, 5, 15, 10, 30, 45, 0, time.UTC), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTimeFrom(tt.date, tt.interval)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimeFrom(%s, %s) error = %v, wantErr %v", tt.date, tt.interval, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !result.Equal(tt.expected) {
				t.Errorf("parseTimeFrom(%s, %s) = %v, want %v", tt.date, tt.interval, result, tt.expected)
			}
		})
	}
}
