package attachedworkerpackage

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Every successful stage retains its exact unit and receipt bytes in a
// content-addressed private archive. Immutable links are never overwritten;
// a partial write can leave extra archive data, but never a false active
// receipt or a missing rollback target for a completed update.
func archiveBundle(unitPath string, unit []byte, receipt ReceiptV1) error {
	if receipt.UnitSHA256 != digest(unit) || receipt.Version != VersionV1 {
		return ErrConflict
	}
	if err := writeImmutable(archiveUnitPath(unitPath, receipt.UnitSHA256), unit); err != nil {
		return err
	}
	return writeImmutable(archiveReceiptPath(unitPath, digest(encodeReceipt(receipt))), encodeReceipt(receipt))
}

func loadArchivedBundle(unitPath, unitSHA, receiptSHA string) ([]byte, ReceiptV1, error) {
	if !validDigest(unitSHA) || !validDigest(receiptSHA) {
		return nil, ReceiptV1{}, ErrInvalid
	}
	unit, err := readRegular(archiveUnitPath(unitPath, unitSHA))
	if err != nil || digest(unit) != unitSHA {
		return nil, ReceiptV1{}, ErrConflict
	}
	encoded, err := readRegular(archiveReceiptPath(unitPath, receiptSHA))
	if err != nil || digest(encoded) != receiptSHA {
		return nil, ReceiptV1{}, ErrConflict
	}
	var receipt ReceiptV1
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(new(any)) != io.EOF ||
		receipt.Version != VersionV1 || receipt.UnitSHA256 != unitSHA ||
		!bytes.Equal(encoded, encodeReceipt(receipt)) {
		return nil, ReceiptV1{}, ErrConflict
	}
	return unit, receipt, nil
}

func archiveUnitPath(unitPath, hash string) string {
	return unitPath + ".archive-unit-" + hash
}

func archiveReceiptPath(unitPath, hash string) string {
	return unitPath + ".archive-receipt-" + hash
}

func encodeReceipt(receipt ReceiptV1) []byte {
	encoded, _ := json.Marshal(receipt)
	return append(encoded, '\n')
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if char >= '0' && char <= '9' || char >= 'a' && char <= 'f' {
			continue
		}
		return false
	}
	return true
}

func writeImmutable(path string, payload []byte) error {
	if existing, err := readRegular(path); err == nil {
		if bytes.Equal(existing, payload) {
			return syncArchive(path)
		}
		return ErrConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrConflict
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".sessionless-archive-")
	if err != nil {
		return ErrIO
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return ErrIO
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return ErrIO
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return ErrIO
	}
	if err := file.Close(); err != nil {
		return ErrIO
	}
	if err := os.Link(file.Name(), path); err != nil {
		if errors.Is(err, os.ErrExist) {
			if existing, readErr := readRegular(path); readErr == nil && bytes.Equal(existing, payload) {
				return syncArchive(path)
			}
			return ErrConflict
		}
		return ErrIO
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return ErrAmbiguous
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return ErrAmbiguous
	}
	return nil
}

func syncArchive(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return ErrAmbiguous
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return ErrAmbiguous
	}
	if err := file.Close(); err != nil {
		return ErrAmbiguous
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return ErrAmbiguous
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return ErrAmbiguous
	}
	return nil
}
