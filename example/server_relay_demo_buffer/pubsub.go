package main

import (
	"bytes"
	"sync"

	log "github.com/sirupsen/logrus"
	flvtag "github.com/yutopp/go-flv/tag"
)

type Pubsub struct {
	srv  *RelayService
	name string

	pub  *Pub
	subs []*Sub

	rb *RingBuffer
	m  sync.Mutex
}

func NewPubsub(srv *RelayService, name string) *Pubsub {
	rb := NewRingBuffer()
	rb.Run()

	return &Pubsub{
		srv:  srv,
		name: name,

		subs: make([]*Sub, 0),
		rb:   rb,
	}
}

func (pb *Pubsub) Deregister() error {
	pb.m.Lock()
	defer pb.m.Unlock()

	for _, sub := range pb.subs {
		_ = sub.Close()
	}

	return pb.srv.RemovePubsub(pb.name)
}

func (pb *Pubsub) Pub() *Pub {
	pub := &Pub{
		pb: pb,
	}

	pb.pub = pub

	return pub
}

func (pb *Pubsub) Sub() *Sub {
	pb.m.Lock()
	defer pb.m.Unlock()

	sub := &Sub{
		pb: pb,
	}

	// TODO: Implement more efficient resource management
	pb.subs = append(pb.subs, sub)

	return sub
}

type Pub struct {
	pb *Pubsub
}

// TODO: Should check codec types and so on.
// In this example, checks only sequence headers and assume that AAC and AVC.
func (p *Pub) Publish(flv *flvtag.FlvTag) error {
	switch flv.Data.(type) {
	case *flvtag.AudioData:
		//for _, sub := range p.pb.subs {
		//	_ = sub.onEvent()
		//}
		newFlv := cloneView(flv)
		_, err := p.pb.rb.Write(newFlv)
		if err != nil {
			log.Printf("failed to write to ring buffer, err: %v", err)
		}

	case *flvtag.ScriptData:
		//for _, sub := range p.pb.subs {
		//	_ = sub.onEvent()
		//}

		newFlv := cloneView(flv)
		_, err := p.pb.rb.Write(newFlv)
		if err != nil {
			log.Printf("failed to write to ring buffer, err: %v", err)
		}

	case *flvtag.VideoData:
		//d := flv.Data.(*flvtag.VideoData)

		newFlv := cloneView(flv)

		//if d.AVCPacketType == flvtag.AVCPacketTypeSequenceHeader {
		//	log.Printf("got avc seq header")
		//	p.pb.avcSeqHeader = newFlv
		//}
		//
		//if p.pb.lastKeyFrame == nil && d.FrameType == flvtag.FrameTypeKeyFrame {
		//	log.Printf("got key frame")
		//	p.pb.lastKeyFrame = newFlv
		//}

		_, err := p.pb.rb.Write(newFlv)
		if err != nil {
			log.Printf("failed to write to ring buffer, err: %v", err)
		}

	default:
		panic("unexpected")
	}

	return nil
}

func (p *Pub) Close() error {
	return p.pb.Deregister()
}

type Sub struct {
	initialized bool
	closed      bool

	pb                *Pubsub
	eventCallback     func(*flvtag.FlvTag) error
	firstTimestamp    uint32
	lastTimestamp     uint32
	curTimestamp      uint32
	lastTimestampDiff uint32
}

func (s *Sub) Run() {
	if !s.initialized {

		//if s.pb.dataFrame != nil {
		//	if err := s.onEvent(cloneView(s.pb.dataFrame)); err != nil {
		//		log.Printf("failed to write , err: %v", err)
		//	}
		//	s.pb.dataFrame = nil
		//}
		//
		//if s.pb.avcSeqHeader != nil {
		//	if err := s.onEvent(cloneView(s.pb.avcSeqHeader)); err != nil {
		//		log.Printf("failed to write seq header, err: %v", err)
		//	}
		//}
		//if s.pb.lastKeyFrame != nil {
		//	if err := s.onEvent(cloneView(s.pb.lastKeyFrame)); err != nil {
		//		log.Printf("failed to write last key frame, err: %v", err)
		//	}
		//}
		s.initialized = true
	}

	err := s.pb.rb.SetWriter(s.onEvent)
	if err != nil {
		log.Printf("failed to set writer, err: %v", err)
	}
}

func (s *Sub) onEvent(flv *flvtag.FlvTag) error {
	if s.closed {
		return nil
	}

	// We need to re-timestamp the data so that the subscribers can achieve a
	// smooth transition across reconnections. Since the first timestamp is
	// always 0 for FLV, we can use this to determine wraparound scenarios
	if flv.Timestamp != 0 && s.firstTimestamp == 0 {
		// This is the start of the stream, so initialise
		s.firstTimestamp = flv.Timestamp
		s.curTimestamp = 0
		s.lastTimestampDiff = 1
	} else {
		// Tag timestamps should be monotonically increasing, so we use a running timestamp
		// In case of wraparound, use the previous diff for the tag
		// TODO: Check for any AV sync issues
		if flv.Timestamp > s.lastTimestamp {
			s.lastTimestampDiff = flv.Timestamp - s.lastTimestamp
		}
		s.curTimestamp += s.lastTimestampDiff
	}
	s.lastTimestamp = flv.Timestamp
	flv.Timestamp = s.curTimestamp

	return s.eventCallback(flv)
}

func (s *Sub) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	return nil
}

func cloneView(flv *flvtag.FlvTag) *flvtag.FlvTag {
	// Need to clone the view because Binary data will be consumed
	v := *flv

	switch flv.Data.(type) {
	case *flvtag.AudioData:
		dCloned := *v.Data.(*flvtag.AudioData)
		v.Data = &dCloned

		dCloned.Data = bytes.NewBuffer(dCloned.Data.(*bytes.Buffer).Bytes())

	case *flvtag.VideoData:
		dCloned := *v.Data.(*flvtag.VideoData)
		v.Data = &dCloned

		dCloned.Data = bytes.NewBuffer(dCloned.Data.(*bytes.Buffer).Bytes())

	case *flvtag.ScriptData:
		dCloned := *v.Data.(*flvtag.ScriptData)
		v.Data = &dCloned

	default:
		panic("unreachable")
	}

	return &v
}
