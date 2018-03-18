// Code generated by protoc-gen-go. DO NOT EDIT.
// source: entry.proto

/*
Package doom is a generated protocol buffer package.

It is generated from these files:
	entry.proto

It has these top-level messages:
	Entry
	Entries
*/
package doom

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Entry struct {
	Key    string `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Offset int64  `protobuf:"varint,2,opt,name=offset" json:"offset,omitempty"`
	Length int64  `protobuf:"varint,3,opt,name=length" json:"length,omitempty"`
	Data   []byte `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *Entry) Reset()                    { *m = Entry{} }
func (m *Entry) String() string            { return proto.CompactTextString(m) }
func (*Entry) ProtoMessage()               {}
func (*Entry) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Entry) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Entry) GetOffset() int64 {
	if m != nil {
		return m.Offset
	}
	return 0
}

func (m *Entry) GetLength() int64 {
	if m != nil {
		return m.Length
	}
	return 0
}

func (m *Entry) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type Entries struct {
	Entries []*Entry `protobuf:"bytes,1,rep,name=entries" json:"entries,omitempty"`
}

func (m *Entries) Reset()                    { *m = Entries{} }
func (m *Entries) String() string            { return proto.CompactTextString(m) }
func (*Entries) ProtoMessage()               {}
func (*Entries) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Entries) GetEntries() []*Entry {
	if m != nil {
		return m.Entries
	}
	return nil
}

func init() {
	proto.RegisterType((*Entry)(nil), "doom.Entry")
	proto.RegisterType((*Entries)(nil), "doom.Entries")
}

func init() { proto.RegisterFile("entry.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 153 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4e, 0xcd, 0x2b, 0x29,
	0xaa, 0xd4, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x49, 0xc9, 0xcf, 0xcf, 0x55, 0x8a, 0xe5,
	0x62, 0x75, 0x05, 0x09, 0x0a, 0x09, 0x70, 0x31, 0x67, 0xa7, 0x56, 0x4a, 0x30, 0x2a, 0x30, 0x6a,
	0x70, 0x06, 0x81, 0x98, 0x42, 0x62, 0x5c, 0x6c, 0xf9, 0x69, 0x69, 0xc5, 0xa9, 0x25, 0x12, 0x4c,
	0x0a, 0x8c, 0x1a, 0xcc, 0x41, 0x50, 0x1e, 0x48, 0x3c, 0x27, 0x35, 0x2f, 0xbd, 0x24, 0x43, 0x82,
	0x19, 0x22, 0x0e, 0xe1, 0x09, 0x09, 0x71, 0xb1, 0xa4, 0x24, 0x96, 0x24, 0x4a, 0xb0, 0x28, 0x30,
	0x6a, 0xf0, 0x04, 0x81, 0xd9, 0x4a, 0x06, 0x5c, 0xec, 0x20, 0xe3, 0x33, 0x53, 0x8b, 0x85, 0x54,
	0xb9, 0xd8, 0x53, 0x21, 0x4c, 0x09, 0x46, 0x05, 0x66, 0x0d, 0x6e, 0x23, 0x6e, 0x3d, 0x90, 0x0b,
	0xf4, 0xc0, 0xd6, 0x07, 0xc1, 0xe4, 0x92, 0xd8, 0xc0, 0xae, 0x33, 0x06, 0x04, 0x00, 0x00, 0xff,
	0xff, 0x1a, 0xaf, 0xa5, 0xbd, 0xac, 0x00, 0x00, 0x00,
}