package api

import (
	"io"
	"log"
	"sync"
	"time"
)

// progressInterval is how often progressWriterAt emits a log line.
// Tuned so a multi-GB download leaves a useful trail in the journal
// without flooding it.
const progressInterval = 30 * time.Second

// progressWriterAt wraps an io.WriterAt and periodically logs throughput
// while a download is in flight. The AWS SDK's Downloader writes chunks
// concurrently at varying offsets, so we accumulate `written` under a
// mutex; the offset itself is irrelevant for progress reporting.
type progressWriterAt struct {
	w       io.WriterAt
	total   int64
	label   string
	written int64
	lastLog time.Time
	start   time.Time
	mu      sync.Mutex
}

func newProgressWriterAt(w io.WriterAt, total int64, label string) *progressWriterAt {
	now := time.Now()
	return &progressWriterAt{
		w:       w,
		total:   total,
		label:   label,
		lastLog: now,
		start:   now,
	}
}

func (p *progressWriterAt) WriteAt(b []byte, off int64) (int, error) {
	n, err := p.w.WriteAt(b, off)
	if n <= 0 {
		return n, err
	}
	p.mu.Lock()
	p.written += int64(n)
	if time.Since(p.lastLog) >= progressInterval {
		pct := 0.0
		if p.total > 0 {
			pct = float64(p.written) / float64(p.total) * 100
		}
		mbps := 0.0
		if elapsed := time.Since(p.start).Seconds(); elapsed > 0 {
			mbps = float64(p.written) / (1024 * 1024) / elapsed
		}
		log.Printf("%s: %.1f%% (%.0f / %.0f MB, %.1f MB/s)",
			p.label, pct,
			float64(p.written)/(1024*1024),
			float64(p.total)/(1024*1024),
			mbps)
		p.lastLog = time.Now()
	}
	p.mu.Unlock()
	return n, err
}
