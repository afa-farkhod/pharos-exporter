package internal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
)

type LogTailerConfig struct {
	Path         string
	PollInterval time.Duration
	Output       io.Writer
	FromStart    bool
	Metrics      *LogMetrics
	CheckPropose bool
	CheckEndorse bool
}

type LogTailer struct {
	cfg    LogTailerConfig
	file   *os.File
	reader *bufio.Reader
	inode  uint64
	offset int64
}

type LogMetrics struct {
	proposeTotal    uint64
	lastProposeUnix int64
	endorseTotal    uint64
	lastEndorseUnix int64
	checkPropose    bool
	checkEndorse    bool
}

func NewLogTailer(cfg LogTailerConfig) (*LogTailer, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("log path is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NewLogMetrics()
	}
	cfg.Metrics.checkPropose = cfg.CheckPropose
	cfg.Metrics.checkEndorse = cfg.CheckEndorse
	return &LogTailer{cfg: cfg}, nil
}

func NewLogMetrics() *LogMetrics {
	return &LogMetrics{}
}

func (t *LogTailer) Start(ctx context.Context) error {
	startAtEnd := !t.cfg.FromStart
	for {
		if err := t.openFile(startAtEnd); err != nil {
			if os.IsNotExist(err) {
				if err := sleepWithContext(ctx, t.cfg.PollInterval); err != nil {
					return err
				}
				continue
			}
			return err
		}
		break
	}
	defer t.closeFile()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := t.reader.ReadBytes('\n')
		if len(line) > 0 {
			lineStr := string(line)
			for _, metricLine := range t.cfg.Metrics.UpdateAndFormat(lineStr) {
				if _, werr := fmt.Fprintln(t.cfg.Output, metricLine); werr != nil {
					return werr
				}
			}
			t.offset += int64(len(line))
		}
		if err == nil {
			continue
		}
		if err != io.EOF {
			return err
		}

		rotated, rerr := t.reopenIfRotated()
		if rerr != nil {
			return rerr
		}
		if rotated {
			continue
		}
		if err := sleepWithContext(ctx, t.cfg.PollInterval); err != nil {
			return err
		}
	}
}

func (t *LogTailer) reopenIfRotated() (bool, error) {
	info, err := os.Stat(t.cfg.Path)
	if err != nil {
		return false, err
	}
	inode, err := inodeFromInfo(info)
	if err != nil {
		return false, err
	}
	if inode != t.inode || info.Size() < t.offset {
		t.closeFile()
		if err := t.openFile(false); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (t *LogTailer) openFile(startAtEnd bool) error {
	f, err := os.Open(t.cfg.Path)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	inode, err := inodeFromInfo(info)
	if err != nil {
		f.Close()
		return err
	}
	offset := int64(0)
	if startAtEnd {
		if off, err := f.Seek(0, io.SeekEnd); err == nil {
			offset = off
		}
	}
	t.file = f
	t.reader = bufio.NewReader(f)
	t.inode = inode
	t.offset = offset
	return nil
}

func (t *LogTailer) closeFile() {
	if t.file != nil {
		_ = t.file.Close()
	}
	t.file = nil
	t.reader = nil
}

func inodeFromInfo(info os.FileInfo) (uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("failed to read inode info")
	}
	return stat.Ino, nil
}

func (m *LogMetrics) UpdateAndFormat(line string) []string {
	ts := parseLogTimestamp(line)

	if strings.Contains(line, "Propose, seq:") {
		if !m.checkPropose {
			return nil
		}
		m.proposeTotal++
		m.lastProposeUnix = ts
		return []string{
			fmt.Sprintf("validator_propose_total %d", m.proposeTotal),
			fmt.Sprintf("validator_last_propose_timestamp %d", m.lastProposeUnix),
		}
	}

	if strings.Contains(line, "endorse seq ") {
		if !m.checkEndorse {
			return nil
		}
		m.endorseTotal++
		m.lastEndorseUnix = ts
		return []string{
			fmt.Sprintf("validator_endorse_total %d", m.endorseTotal),
			fmt.Sprintf("validator_last_endorse_timestamp %d", m.lastEndorseUnix),
		}
	}
	return nil
}

func parseLogTimestamp(line string) int64 {
	if len(line) == 0 || line[0] != '[' {
		return time.Now().Unix()
	}
	end := strings.IndexByte(line, ']')
	if end <= 1 {
		return time.Now().Unix()
	}
	ts, err := time.Parse(time.RFC3339Nano, line[1:end])
	if err != nil {
		return time.Now().Unix()
	}
	return ts.Unix()
}
