package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type commandResult struct {
	Action string
	Result any
	Error  error
}

type outputEnvelope struct {
	Action     string `json:"action"`
	Result     any    `json:"result,omitempty"`
	Error      string `json:"error,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

func printResult(result commandResult, duration time.Duration) error {
	resultErr := ""
	if result.Error != nil {
		resultErr = result.Error.Error()
	}
	return printEnvelope(outputEnvelope{
		Action:     result.Action,
		Result:     result.Result,
		Error:      resultErr,
		DurationMs: duration.Milliseconds(),
	})
}

func printEnvelope(out outputEnvelope) error {
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(data))
	return nil
}
