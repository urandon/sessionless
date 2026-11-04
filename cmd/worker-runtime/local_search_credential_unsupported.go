//go:build !darwin && !linux

package main

import "context"

func readLocalSearchKey(context.Context, string) ([]byte, error) {
	return nil, errWorkerLocalExecution
}
