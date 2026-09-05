package pimp

import (
	"testing"
	"time"
)

func TestEtaFrom(t *testing.T) {
	now := time.Date(2026, 9, 4, 6, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		name      string
		elapsed   time.Duration
		completed int
		total     int
		want      time.Duration // from now
	}{
		// a fifth done after 10 minutes implies 40 more
		{"one fifth", 10 * time.Minute, 100, 500, 40 * time.Minute},
		{"half", 30 * time.Minute, 250, 500, 30 * time.Minute},
		{"nearly done", time.Hour, 499, 500, time.Hour / 499},
		{"all done", time.Hour, 500, 500, 0},
		// the old integer-minute arithmetic collapsed to zero inside the
		// first minute, putting the ETA in the past
		{"under a minute", 30 * time.Second, 1000, 5064, 121920 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := etaFrom(now, tt.elapsed, tt.completed, tt.total)
			if want := now.Add(tt.want); got != want {
				t.Errorf("etaFrom() = %v, want %v", got, want)
			}
			if got.Before(now) {
				t.Errorf("etaFrom() = %v is before now (%v)", got, now)
			}
		})
	}
}
