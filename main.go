package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
)

const msgTemplate = `
{
    "session_id": "%s",
    "timestamp": %d,
    "playback_id": "%s",
    "ip":"73.152.182.50",
    "protocol":"video/mp4",
    "page_url": "https://www.fishtank.live/",
    "source_url": "https://vod-cdn.lp-playback.studio/raw/jxf4iblf6wlsyor6526t4tcmtmqa/catalyst-vod-com/hls/362f9l7ekeoze518/1080p0.mp4?tkn=8b140ec6b404a",
    "player": "video-@livepeer/react@3.1.9",
    "timestamp_ts": "2023-08-27 10:11:02.957000 UTC",
    "user_id": "%s",
    "d_storage_url": "",
    "source":"stream/asset/recording",
    "creator_id": "%s",
    "deviceType": "%s",
    "device_model": "iPhone 12",
    "device_brand": "Apple",
    "browser": "%s",
    "os": "iOS",
    "cpu": "amd64",
    "playback_geo_hash": "eyc",
    "playback_continent_name": "%s",
    "playback_country_code": "US",
    "playback_country_name": "%s",
    "playback_subdivision_name": "Calirfornia",
    "playback_timezone": "America/Los_Angeles",
    "data": {
        "errors": %d, 
        "playtime_ms": 4500,
        "ttff_ms": 300,
        "preload_time_ms": 1000,
        "autoplay_status": "auto",
        "buffer_ms": 50,
        "event": {
            "type": "heartbeat",
            "timestamp":%d,
            "payload": "heartbeat message"
        }
    }
}
`

var (
	deviceTypes = []string{
		"Macintosh",
		"Android",
		"iPhone",
	}
	browsers = []string{
		"Chrome",
		"Safari",
		"Edge",
		"Explorer",
		"Firefox",
	}
	countries = []string{
		"Poland",
		"Germany",
		"Ukraine",
		"France",
		"Spain",
		"Czechia",
	}
)

func main() {
	var (
		playbackID    = "abcdefgh-1"
		sessionNumber = 1
		kafkaTopic    = "playbackLogs4"
	)
	flag.StringVar(&playbackID, "playback-id", playbackID, "playbackID")
	flag.IntVar(&sessionNumber, "session-number", sessionNumber, "number of concurrent sessions")
	flag.StringVar(&kafkaTopic, "kafka-topic", kafkaTopic, "kafka topic")
	flag.Parse()

	var producerClient = CreateKafkaClient(kafkaTopic, sessionNumber)

	fmt.Println("Starting sending playback logs...")

	done := make(chan bool)
	kafkaMsgs := make(chan string, 1000)

	for i := 0; i < sessionNumber; i++ {
		sessionID := fmt.Sprintf("%s-%d", playbackID, i)
		go func() {
			// Start each goroutine with a random delay
			time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			for {
				kafkaMsgs <- msg(playbackID, sessionID)
				time.Sleep(5 * time.Second)
			}
		}()
	}

	go func() {
		for {
			for msg := range kafkaMsgs {
				producerClient.Send(msg)
			}
		}
	}()

	<-done
}

func msg(playbackID, sessionID string) string {
	errors := 0
	deviceType := deviceTypes[rand.Intn(len(deviceTypes))]
	browser := browsers[rand.Intn(len(browsers))]
	continent := "Europe"
	country := countries[rand.Intn(len(countries))]
	userId := "user12345"
	creatorId := "creator12345"
	timestamp := time.Now().UnixMilli()
	return fmt.Sprintf(msgTemplate,
		sessionID,
		timestamp,
		playbackID,
		userId,
		creatorId,
		deviceType,
		browser,
		continent,
		country,
		errors,
		timestamp,
	)
}
