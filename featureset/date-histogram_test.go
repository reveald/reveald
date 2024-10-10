package featureset

import (
	"testing"
	"time"
)

func TestIntervalEnd(t *testing.T) {
	baseTime := time.Date(2023, 5, 15, 10, 30, 45, int(time.Millisecond*500), time.UTC)

	tests := []struct {
		name     string
		interval string
		expected time.Time
	}{
		{"Yearly", string(DateCalendarIntervalYearly), time.Date(2024, 5, 15, 10, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Monthly", string(DateCalendarIntervalMonthly), time.Date(2023, 6, 15, 10, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Daily Calendar", string(DateCalendarIntervalDaily), time.Date(2023, 5, 16, 10, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Daily Fixed", string(DateFixedIntervalDaily), time.Date(2023, 5, 16, 10, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Hourly", string(DateFixedIntervalHours), time.Date(2023, 5, 15, 11, 30, 45, int(time.Millisecond*500), time.UTC)},
		{"Minutes", string(DateFixedIntervalMinutes), time.Date(2023, 5, 15, 10, 31, 45, int(time.Millisecond*500), time.UTC)},
		{"Seconds", string(DateFixedIntervalSeconds), time.Date(2023, 5, 15, 10, 30, 46, int(time.Millisecond*500), time.UTC)},
		{"Milliseconds", string(DateFixedIntervalMilliseconds), time.Date(2023, 5, 15, 10, 30, 45, int(time.Millisecond*501), time.UTC)},
		{"Invalid Interval", "invalid", baseTime},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntervalEnd(baseTime, tt.interval)
			if !result.Equal(tt.expected) {
				t.Errorf("IntervalEnd(%v, %s) = %v, want %v", baseTime, tt.interval, result, tt.expected)
			}
		})
	}
}

func TestParseTimeFrom(t *testing.T) {
	tests := []struct {
		name     string
		date     string
		interval string
		expected time.Time
		wantErr  bool
	}{
		{"Yearly", "2023", string(DateCalendarIntervalYearly), time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"Monthly", "2023-05", string(DateCalendarIntervalMonthly), time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC), false},
		{"Daily Calendar", "2023-05-15", string(DateCalendarIntervalDaily), time.Date(2023, 5, 15, 0, 0, 0, 0, time.UTC), false},
		{"Daily Fixed", "2023-05-15", string(DateFixedIntervalDaily), time.Date(2023, 5, 15, 0, 0, 0, 0, time.UTC), false},
		{"Hourly", "2023-05-15 10", string(DateFixedIntervalHours), time.Date(2023, 5, 15, 10, 0, 0, 0, time.UTC), false},
		{"Minutes", "2023-05-15 10:30", string(DateFixedIntervalMinutes), time.Date(2023, 5, 15, 10, 30, 0, 0, time.UTC), false},
		{"Seconds", "2023-05-15 10:30:45", string(DateFixedIntervalSeconds), time.Date(2023, 5, 15, 10, 30, 45, 0, time.UTC), false},
		{"Milliseconds", "2023-05-15 10:30:45.500", string(DateFixedIntervalMilliseconds), time.Date(2023, 5, 15, 10, 30, 45, 500000000, time.UTC), false},
		{"Invalid Interval", "2023-05-15", "invalid", time.Time{}, true},
		{"Invalid Date Format", "invalid", string(DateCalendarIntervalYearly), time.Time{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseTimeFrom(tt.date, tt.interval)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimeFrom(%s, %s) error = %v, wantErr %v", tt.date, tt.interval, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !result.Equal(tt.expected) {
				t.Errorf("ParseTimeFrom(%s, %s) = %v, want %v", tt.date, tt.interval, result, tt.expected)
			}
		})
	}
}
