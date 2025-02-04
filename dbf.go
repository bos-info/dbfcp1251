// Package dbf provides parsing DBF files with cp1251 codepage(Cyrillic) for FoxBASE+/Dbase III plus, no memo
package dbf

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"golang.org/x/text/encoding/charmap"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
)

// A Reader serves content from a DBF file.
//
// A dbf.Reader should have some metadata, and a Read() method that returns
// table rows, one at a time
type Reader struct {
	rs           io.ReadSeeker
	year         int
	month        int
	day          int
	length       int // number of records
	fields       []Field
	headerLength uint16 // in bytes
	recordLength uint16 // length of each record, in bytes
	mu           sync.Mutex
}

type header struct {
	// documented at: https://en.wikipedia.org/wiki/.dbf
	Version      byte
	Year         uint8 // stored as offset from (decimal) 1900
	Month        uint8
	Day          uint8
	NumberRecord uint32
	HeaderLength uint16 // in bytes
	RecordLength uint16 // length of each record, in bytes
}

// NewReader returns a new Reader reading from r.
func NewReader(r io.ReadSeeker) (*Reader, error) {
	var h header
	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil {
		return nil, err
	} else if h.Version != 0x03 {
		return nil, fmt.Errorf("unexepected file Version: %d\n", h.Version)
	}

	var fields []Field
	if _, err := r.Seek(0x20, 0); err != nil {
		return nil, err
	}
	var offset uint16
	for offset = 0x20; offset < h.HeaderLength-1; offset += 32 {
		f := Field{}
		errBin := binary.Read(r, binary.LittleEndian, &f)
		if errBin != nil {
			fmt.Println("binary.Read failed:", errBin)
		}
		if err = f.validate(); err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	br := bufio.NewReader(r)
	if eoh, err := br.ReadByte(); err != nil {
		return nil, err
	} else if eoh != 0x0D {
		return nil, fmt.Errorf("Header was supposed to be %d bytes long, but found byte %#x at that offset instead of expected byte 0x0D\n", h.HeaderLength, eoh)
	}

	return &Reader{rs: r, year: 1900 + int(h.Year),
		month: int(h.Month), day: int(h.Day), length: int(h.NumberRecord), fields: fields,
		headerLength: h.HeaderLength, recordLength: h.RecordLength}, nil
}

// ModDate return year, month and day of modification file
func (r *Reader) ModDate() (int, int, int) {
	return r.year, r.month, r.day
}

// FieldName return name of ordinal number of the column
func (r *Reader) FieldName(i int) (name string) {
	return strings.TrimRight(string(r.fields[i].Name[:]), "\x00")
}

// FieldNames return names of all columns
func (r *Reader) FieldNames() (names []string) {
	for i := range r.fields {
		names = append(names, r.FieldName(i))
	}
	return
}

func (f *Field) validate() error {
	switch f.Type {
	case 'C', 'N', 'F':
		return nil
	}
	return fmt.Errorf("sorry, dbf library doesn't recognize field type '%c'", f.Type)
}

type Field struct {
	Name          [11]byte // 0x0 terminated
	Type          byte
	Offset        uint32
	Len           uint8
	DecimalPlaces uint8 // ?
	// Flags         uint8
	// AutoIncrNext  uint32
	// AutoIncrStep  uint8
	_ [14]byte
}

type Record map[string]interface{}

// Read implements the Reader interface only for C,N,F types of record in a file
func (r *Reader) Read(i uint16) (rec Record, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	offset := int64(r.headerLength) + int64(r.recordLength)*int64(i)
	if _, err = r.rs.Seek(offset, 0); err != nil {
		log.Println("seek error")
	}

	var deleted byte
	if err = binary.Read(r.rs, binary.LittleEndian, &deleted); err != nil {
		return nil, err
	} else if deleted == '*' {
		return nil, fmt.Errorf("record %d is deleted", i)
	} else if deleted != ' ' {
		return nil, fmt.Errorf("record %d contained an unexpected value in the deleted flag: %v", i, deleted)
	}

	rec = make(Record)
	for i, f := range r.fields {
		buf := make([]byte, f.Len)
		if err = binary.Read(r.rs, binary.LittleEndian, &buf); err != nil {
			return nil, err
		}

		fieldVal := strings.TrimSpace(string(buf))
		fieldName := r.FieldName(i)

		switch f.Type {
		case 'F':
			if len(fieldVal) == 0 {
				rec[fieldName] = float64(0)
			} else {
				rec[fieldName], err = strconv.ParseFloat(fieldVal, 64)
			}
		case 'N':
			if len(fieldVal) == 0 {
				rec[fieldName] = 0
			} else if f.DecimalPlaces > 0 {
				rec[fieldName], err = strconv.ParseFloat(fieldVal, 64)
			} else {
				rec[fieldName], err = strconv.Atoi(fieldVal)
			}
		case 'C':
			if len(fieldVal) == 0 {
				rec[fieldName] = ""
			} else {
				decoder := charmap.Windows1251.NewDecoder()
				rec[fieldName], err = decoder.String(fieldVal)
			}
		default:
			rec[fieldName] = fieldVal
		}
		if err != nil {
			return nil, err
		}
	}
	return rec, nil
}

// Length return a number of records in file
func (r *Reader) Length() int {
	return r.length
}
