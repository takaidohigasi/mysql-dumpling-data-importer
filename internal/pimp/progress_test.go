package pimp

import (
	"strings"
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

func TestLineScanWriterCountsDistinctFiles(t *testing.T) {
	seen := make(map[string]struct{})
	count := 0
	w := &lineScanWriter{onLine: func(line string) {
		m := workerRecordsRe.FindStringSubmatch(line)
		if m == nil {
			return
		}
		if _, dup := seen[m[1]]; dup {
			return
		}
		seen[m[1]] = struct{}{}
		count++
	}}

	// one file per line, a chunked file repeating, a sub-chunk suffix, noise
	// lines, and a line split across two writes
	lines := "" +
		"Importing from 3 files to table `mercari`.`items` in MySQL Server at 10.64.12.15:3306 using 8 threads\n" +
		"[Worker001]: /dump/mercari.items.000.csv: Records: 100  Deleted: 0  Skipped: 1  Warnings: 0\n" +
		"[Worker002]: /dump/mercari.items.001.csv: Records: 50  Deleted: 0  Skipped: 1  Warnings: 0 - flushed sub-chunk 1\n" +
		"[Worker002]: /dump/mercari.items.001.csv: Records: 50  Deleted: 0  Skipped: 0  Warnings: 0 - loading finished in 2 sub-chunks\n"
	half := len(lines) / 2
	for _, chunk := range []string{lines[:half], lines[half:],
		"[Worker003]: /dump/mercari.items", // split mid-line...
		".002.csv: Records: 7  Deleted: 0  Skipped: 1  Warnings: 0\n",
		"3 files (1.20 GB) were imported in 60.0 sec at 20.00 MB/s\n",
	} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}

	if count != 3 {
		t.Errorf("distinct files counted = %d, want 3 (seen: %v)", count, seen)
	}
	if !strings.Contains(w.String(), "were imported in") {
		t.Errorf("String() should retain the full output, got %q", w.String())
	}
}
