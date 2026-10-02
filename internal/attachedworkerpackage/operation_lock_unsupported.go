//go:build !darwin && !linux

package attachedworkerpackage

type operationLease struct{}

func acquireOperationLease(string) (*operationLease, error) { return nil, ErrInvalid }
func (*operationLease) Close() error                        { return nil }
