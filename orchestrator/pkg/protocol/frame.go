package protocol

import (
	"encoding/binary"
	"errors"
)

const (
	HeaderSize  = 3 // 1 byte severity + 2 byte device_id length
	MaxIDLength = 65535
)

var ErrDeviceIDTooLong = errors.New("device_id melebihi batas 65535 byte")

// TelemetryFrame merepresentasikan paket sebelum di-encode ke wire format.
type TelemetryFrame struct {
	Severity uint8
	DeviceID []byte
	Payload  []byte
}

// EncodeInto menulis TelemetryFrame ke buffer yang sudah ada (zero-allocation).
func (f *TelemetryFrame) EncodeInto(buf []byte) (int, error) {
	idLen := len(f.DeviceID)
	if idLen > MaxIDLength {
		return 0, ErrDeviceIDTooLong
	}

	totalLen := HeaderSize + idLen + len(f.Payload)
	if len(buf) < totalLen {
		return 0, errors.New("buffer tujuan terlalu kecil")
	}

	buf[0] = f.Severity
	binary.BigEndian.PutUint16(buf[1:3], uint16(idLen))
	copy(buf[3:3+idLen], f.DeviceID)
	copy(buf[3+idLen:totalLen], f.Payload)

	return totalLen, nil
}
