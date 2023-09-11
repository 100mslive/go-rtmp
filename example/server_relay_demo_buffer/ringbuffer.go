package main

import (
	"errors"
	"log"

	flvtag "github.com/yutopp/go-flv/tag"
)

type RingBuffer struct {
	in  chan *flvtag.FlvTag
	out chan *flvtag.FlvTag

	writer      func(d *flvtag.FlvTag) error
	writerAdded chan struct{}

	avcSeqHeader *flvtag.FlvTag
	lastKeyFrame *flvtag.FlvTag
	dataFrame    *flvtag.FlvTag
}

func NewRingBuffer() *RingBuffer {
	in := make(chan *flvtag.FlvTag)
	out := make(chan *flvtag.FlvTag, 1000)
	return &RingBuffer{
		in:          in,
		out:         out,
		writerAdded: make(chan struct{}),
	}
}

func (rb *RingBuffer) Run() {
	go func() {
		for ele := range rb.in {
			select {
			case rb.out <- ele:
				//log.Printf("added to out chan")
			default:
				flv := <-rb.out

				if flv.TagType == flvtag.TagTypeScriptData {
					log.Printf("saving script data")
					rb.dataFrame = flv
				}
				if flv.TagType == flvtag.TagTypeVideo && flv.Data.(*flvtag.VideoData).AVCPacketType == flvtag.AVCPacketTypeSequenceHeader {
					log.Printf("saving seq header")
					rb.avcSeqHeader = flv
				}

				if flv.TagType == flvtag.TagTypeVideo && flv.Data.(*flvtag.VideoData).FrameType == flvtag.FrameTypeKeyFrame {
					log.Printf("saving key frame")
					rb.lastKeyFrame = flv
				}

				rb.out <- ele
				log.Printf("overflow! added to out chan")
			}
		}
		log.Printf("exiting routine 1")
	}()

	go func() {
		for {
			<-rb.writerAdded

			if rb.dataFrame != nil {
				log.Printf("sending script data")
				rb.writer(rb.dataFrame)
				rb.dataFrame = nil
			}

			if rb.avcSeqHeader != nil {
				log.Printf("sending seq header")
				rb.writer(rb.avcSeqHeader)
				rb.avcSeqHeader = nil
			}

			if rb.lastKeyFrame != nil {
				log.Printf("sending key frame")
				rb.writer(rb.lastKeyFrame)
				rb.lastKeyFrame = nil
			}

			log.Printf("writer added")
			if rb.writer != nil {
				for flv := range rb.out {

					//log.Printf("pop from out chan")
					err := rb.writer(flv)
					if err != nil {
						log.Printf("writing to writer failed error: %v", err)
					}
				}
			}
		}
		log.Printf("exiting routine 2")
	}()
	log.Printf("exiting ring buffer run")

}

func (rb *RingBuffer) Close() {
	close(rb.in)
	close(rb.out)
	return
}

func (rb *RingBuffer) Write(p *flvtag.FlvTag) (int, error) {
	rb.in <- p
	log.Printf("adde to in chan")
	return 0, nil
}

func (rb *RingBuffer) SetWriter(w func(t *flvtag.FlvTag) error) error {
	//pb.lock.Lock()
	//defer pb.lock.Unlock()

	if w == nil {
		return errors.New("empty writer")
	}

	rb.writer = w
	rb.writerAdded <- struct{}{}

	log.Printf("writer added set writer")
	return nil
}
