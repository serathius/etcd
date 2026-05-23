// Copyright 2022 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_readRaw(t *testing.T) {
	path := t.TempDir()
	mustCreateWALLog(t, path)
	var out bytes.Buffer
	readRaw(nil, walDir(path), &out)
	assertReadRawOutput(t, `CRC: 0
Metadata: 
Snapshot: index:0  term:0
Entry: Term:1  Index:1  Type:EntryConfChange  Data:"\x08\x01\x10\x00\x18\x02\"\x00"
Entry: Term:2  Index:2  Type:EntryConfChange  Data:"\x08\x02\x10\x01\x18\x02\"\x00"
Entry: Term:2  Index:3  Type:EntryConfChange  Data:"\x08\x03\x10\x02\x18\x02\"\x00"
Entry: Term:2  Index:4  Type:EntryConfChange  Data:"\x08\x04\x10\x03\x18\x03\"\x00"
Entry: Term:4  Index:5  Type:EntryNormal  Data:"\x08\x05\x1a\x15\n\x011\x12\x02hi\x18\x06 \x01(\x01X\xa0\x9c\x01h\xa0\x9c\x01"
Entry: Term:5  Index:6  Type:EntryNormal  Data:"\x08\x06\"\x10\n\x04foo1\x12\x04bar1\x18\x010\x01"
Entry: Term:6  Index:7  Type:EntryNormal  Data:"\x08\x07*\x08\n\x010\x12\x019\x18\x01"
Entry: Term:7  Index:8  Type:EntryNormal  Data:"\x08\x082\x14\x12\x08\x1a\x06\n\x01a\x12\x01b\x1a\x08\x1a\x06\n\x01a\x12\x01b"
Entry: Term:8  Index:9  Type:EntryNormal  Data:"\x08\t:\x02\x10\x01"
Entry: Term:9  Index:10  Type:EntryNormal  Data:"\x08\nB\x04\x08\x01\x10\x01"
Entry: Term:10  Index:11  Type:EntryNormal  Data:"\x08\x0bJ\x02\x08\x02"
Entry: Term:11  Index:12  Type:EntryNormal  Data:"\x08\x0cR\x06\x08\x03\x10\x04\x18\x05"
Entry: Term:12  Index:13  Type:EntryNormal  Data:"\x08\r\xc2>\x00"
Entry: Term:13  Index:14  Type:EntryNormal  Data:"\x08\x0e\x9a?\x00"
Entry: Term:14  Index:15  Type:EntryNormal  Data:"\x08\x0f\xa2?\x19\n\x06myname\x12\x08password\x1a\x05token"
Entry: Term:15  Index:16  Type:EntryNormal  Data:"\x08\x10\xe2D\x10\n\x05name1\x12\x05pass1\x1a\x00"
Entry: Term:16  Index:17  Type:EntryNormal  Data:"\x08\x11\xeaD\x07\n\x05name1"
Entry: Term:17  Index:18  Type:EntryNormal  Data:"\x08\x12\xf2D\x07\n\x05name1"
Entry: Term:18  Index:19  Type:EntryNormal  Data:"\x08\x13\xfaD\x0e\n\x05name1\x12\x05pass2"
Entry: Term:19  Index:20  Type:EntryNormal  Data:"\x08\x14\x82E\x0e\n\x05user1\x12\x05role1"
Entry: Term:20  Index:21  Type:EntryNormal  Data:"\x08\x15\x8aE\x0e\n\x05user2\x12\x05role2"
Entry: Term:21  Index:22  Type:EntryNormal  Data:"\x08\x16\x92E\x00"
Entry: Term:22  Index:23  Type:EntryNormal  Data:"\x08\x17\x9aE\x00"
Entry: Term:23  Index:24  Type:EntryNormal  Data:"\x08\x18\x82K\x07\n\x05role2"
Entry: Term:24  Index:25  Type:EntryNormal  Data:"\x08\x19\x8aK\x07\n\x05role1"
Entry: Term:25  Index:26  Type:EntryNormal  Data:"\x08\x1a\x92K\x07\n\x05role3"
Entry: Term:26  Index:27  Type:EntryNormal  Data:"\x08\x1b\x9aK\x1b\n\x05role3\x12\x12\x08\x01\x12\x04Keys\x1a\x08RangeEnd"
Entry: Term:27  Index:28  Type:EntryNormal  Data:"\x08\x1c\xa2K\x16\n\x05role3\x12\x03key\x1a\x08rangeend"
Entry: Term:27  Index:29  Type:EntryNormal  Data:"?"
EOF: All entries were processed.
`, out.String())
}

func assertReadRawOutput(t *testing.T, expected, actual string) {
	t.Helper()

	// google.golang.org/protobuf intentionally makes String output unstable across
	// binaries by varying whitespace, so accept the alternate snapshot line.
	// See https://github.com/protocolbuffers/protobuf-go/blob/v1.36.11/internal/encoding/text/encode.go#L229-L232.
	normalize := func(s string) string {
		for strings.Contains(s, "  ") {
			s = strings.ReplaceAll(s, "  ", " ")
		}
		return s
	}

	assert.Equal(t, normalize(expected), normalize(actual))
}
