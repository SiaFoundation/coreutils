package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const logFormat = `{%n  "commit": "%H",%n  "shortCommit": "%h",%n  "timestamp": "%cD",%n  "tag": "%(describe:tags=true)"%n}`

type (
	gitTime time.Time

	gitMeta struct {
		Commit      string  `json:"commit"`
		ShortCommit string  `json:"shortCommit"`
		Timestamp   gitTime `json:"timestamp"`
		Tag         string  `json:"tag"`
	}
)

// UnmarshalJSON implements the json.Unmarshaler interface.
func (t *gitTime) UnmarshalJSON(buf []byte) error {
	timeFormats := []string{
		time.RFC1123Z,
		"Mon, 2 Jan 2006 15:04:05 -0700",
		"2006-01-02 15:04:05 -0700",
		time.UnixDate,
		time.ANSIC,
		time.RFC3339,
		time.RFC1123,
	}

	for _, format := range timeFormats {
		parsed, err := time.Parse(format, strings.Trim(string(buf), `"`))
		if err == nil {
			*t = gitTime(parsed)
			return nil
		}
	}
	return errors.New("failed to parse time")
}

func getGitMeta() (meta gitMeta, _ error) {
	cmd := exec.Command("git", "log", "-1", "--pretty=format:"+logFormat+"")
	buf, err := cmd.Output()
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok && len(err.Stderr) > 0 {
			return gitMeta{}, fmt.Errorf("command failed: %w", errors.New(string(err.Stderr)))
		}
		return gitMeta{}, fmt.Errorf("failed to execute command: %w", err)
	} else if err := json.Unmarshal(buf, &meta); err != nil {
		return gitMeta{}, fmt.Errorf("failed to unmarshal json: %w", err)
	}
	return
}
