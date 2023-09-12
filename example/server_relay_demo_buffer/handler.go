package main

import (
	"bytes"
	"io"
	"log"

	"github.com/pkg/errors"
	flvtag "github.com/yutopp/go-flv/tag"
	"github.com/yutopp/go-rtmp"
	rtmpmsg "github.com/yutopp/go-rtmp/message"
)

var _ rtmp.Handler = (*Handler)(nil)

// Handler An RTMP connection handler
type Handler struct {
	rtmp.DefaultHandler
	relayService *RelayService

	//
	conn *rtmp.Conn

	//
	pub *Pub
	sub *Sub
}

func (h *Handler) OnServe(conn *rtmp.Conn) {
	h.conn = conn
}

func (h *Handler) OnConnect(timestamp uint32, cmd *rtmpmsg.NetConnectionConnect) error {
	log.Printf("OnConnect: %#v", cmd)

	// TODO: check app name to distinguish stream names per apps
	// cmd.Command.App

	return nil
}

func (h *Handler) OnCreateStream(timestamp uint32, cmd *rtmpmsg.NetConnectionCreateStream) error {
	log.Printf("OnCreateStream: %#v", cmd)
	return nil
}

func (h *Handler) OnPublish(_ *rtmp.StreamContext, timestamp uint32, cmd *rtmpmsg.NetStreamPublish) error {
	log.Printf("OnPublish: %#v", cmd)

	if h.sub != nil {
		return errors.New("Cannot publish to this stream")
	}

	// (example) Reject a connection when PublishingName is empty
	if cmd.PublishingName == "" {
		return errors.New("PublishingName is empty")
	}

	// Reuse pubsub if there is already one
	// Ideally, we want to reuse pubsub if there is a reconnection from the publisher
	// within the reconnection window
	pubsub, err := h.relayService.GetPubsub(cmd.PublishingName)
	if err != nil {
		pubsub, err = h.relayService.NewPubsub(cmd.PublishingName)
		if err != nil {
			return errors.Wrap(err, "Failed to create pubsub")
		} else {
			log.Printf("OnPublish: Created a new PubSub: %v", pubsub)
		}
	} else {
		log.Printf("OnPublish: Reusing PubSub %v", pubsub)
	}

	pub := pubsub.Pub()

	h.pub = pub

	return nil
}

func (h *Handler) OnPlay(ctx *rtmp.StreamContext, timestamp uint32, cmd *rtmpmsg.NetStreamPlay) error {
	log.Printf("OnPlay: %#v", cmd)
	if h.sub != nil {
		return errors.New("Cannot play on this stream")
	}

	pubsub, err := h.relayService.GetPubsub(cmd.StreamName)
	if err != nil {
		return errors.Wrap(err, "Failed to get pubsub")
	}

	sub := pubsub.Sub()
	sub.eventCallback = onEventCallback(h.conn, ctx.StreamID)
	sub.Run()

	h.sub = sub

	return nil
}

func (h *Handler) OnSetDataFrame(timestamp uint32, data *rtmpmsg.NetStreamSetDataFrame) error {
	log.Printf("OnSetDataFrame: %#v", data)
	r := bytes.NewReader(data.Payload)

	var script flvtag.ScriptData
	if err := flvtag.DecodeScriptData(r, &script); err != nil {
		log.Printf("Failed to decode script data: Err = %+v", err)
		return nil // ignore
	}

	log.Printf("SetDataFrame: Script = %#v", script)

	// TODO: Since subscriber is not currently attempting direct playback,
	// we can safely ignore DataFrames for now. We'll need to add validations
	// for codec parameters across reconnections
	// _ = h.pub.Publish(&flvtag.FlvTag{
	// 	TagType:   flvtag.TagTypeScriptData,
	// 	Timestamp: timestamp,
	// 	Data:      &script,
	// })

	return nil
}

func (h *Handler) OnAudio(timestamp uint32, payload io.Reader) error {
	var audio flvtag.AudioData
	if err := flvtag.DecodeAudioData(payload, &audio); err != nil {
		return err
	}

	flvBody := new(bytes.Buffer)
	if _, err := io.Copy(flvBody, audio.Data); err != nil {
		return err
	}
	audio.Data = flvBody

	_ = h.pub.Publish(&flvtag.FlvTag{
		TagType:   flvtag.TagTypeAudio,
		Timestamp: timestamp,
		Data:      &audio,
	})

	return nil
}

func (h *Handler) OnVideo(timestamp uint32, payload io.Reader) error {
	var video flvtag.VideoData
	if err := flvtag.DecodeVideoData(payload, &video); err != nil {
		return err
	}

	// Need deep copy because payload will be recycled
	flvBody := new(bytes.Buffer)
	if _, err := io.Copy(flvBody, video.Data); err != nil {
		return err
	}
	video.Data = flvBody

	_ = h.pub.Publish(&flvtag.FlvTag{
		TagType:   flvtag.TagTypeVideo,
		Timestamp: timestamp,
		Data:      &video,
	})

	return nil
}

func (h *Handler) OnClose() {
	log.Printf("OnClose")

	// TODO: Cleanup things that actually need to be closed
	// For reconnection to work seamlessly, we need context of previous sessions
	// So, don't close the pubsub (we are anyway only registering/deregistering here)
	// if h.pub != nil {
	// 	_ = h.pub.Close()
	// }

	// if h.sub != nil {
	// 	_ = h.sub.Close()
	// }
}
