package dbf

// reference implementation:
//     http://dbf.berlios.de/
// test data: http://abs.gov.au/AUSSTATS/abs@.nsf/DetailsPage/2923.0.30.0012006?OpenDocument
// a dbf.Reader should have some metadata, and a Read() method that returns
// table rows, one at a time

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

type Reader struct {
	rs           io.ReadSeeker
	year         int
	month        int
	day          int
	length       int // number of records
	fields       []Field
	headerLength uint16 // in bytes
	recordLength uint16 // length of each record, in bytes
	sync.Mutex
}

type header struct {
	// documented at: http://www.dbase.com/knowledgebase/int/db7_file_fmt.htm
	version      byte
	year         uint8 // stored as offset from (decimal) 1900
	month        uint8
	day          uint8
	numberRecord uint32
	headerLength uint16 // in bytes
	recordLength uint16 // length of each record, in bytes
}

func NewReader(r io.ReadSeeker) (*Reader, error) {
	var h header
	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil {
		return nil, err
	} else if h.version != 0x03 {
		return nil, fmt.Errorf("unexepected file version: %day\n", h.version)
	}

	var fields []Field
	if _, err := r.Seek(0x20, 0); err != nil {
		return nil, err
	}
	var offset uint16
	for offset = 0x20; offset < h.headerLength-1; offset += 32 {
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
		return nil, fmt.Errorf("Header was supposed to be %d bytes long, but found byte %#x at that offset instead of expected byte 0x0D\n", h.headerLength, eoh)
	}

	return &Reader{r, 1900 + int(h.year),
		int(h.month), int(h.day), int(h.numberRecord), fields,
		h.headerLength, h.recordLength, *new(sync.Mutex)}, nil
}

func (r *Reader) ModDate() (int, int, int) {
	return r.year, r.month, r.day
}

func (r *Reader) FieldName(i int) (name string) {
	return strings.TrimRight(string(r.fields[i].Name[:]), "\x00")
}

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

func (r *Reader) Read(i uint16) (rec Record, err error) {
	r.Lock()
	defer r.Unlock()
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

func (r *Reader) Length() int {
	return r.length
}
