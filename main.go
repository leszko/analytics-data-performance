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
    "server_timestamp": %d,
    "playback_id": "%s",
    "viewership_hash":"4ee7ed32-ae83-49fc-874e-6b291e7c94bd",
    "protocol":"video/mp4",
    "page_url": "https://www.fishtank.live/",
    "source_url": "https://vod-cdn.lp-playback.studio/raw/jxf4iblf6wlsyor6526t4tcmtmqa/catalyst-vod-com/hls/362f9l7ekeoze518/1080p0.mp4?tkn=8b140ec6b404a",
    "player": "video-@livepeer/react@3.1.9",
    "timestamp_ts": "2023-08-27 10:11:02.957000 UTC",
    "user_id": "%s",
    "d_storage_url": "",
    "source":"asset",
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
		"event_type": "heartbeat",
		"event_timestamp": %d,
        "autoplay_status": "autoplay",
        "stalled_count": 0,
        "waiting_count": 0,
        "time_errored_ms": 0,
        "time_stalled_ms": 0,
        "time_playing_ms": 5000,
        "time_waiting_ms": 0,
        "mount_to_play_ms": 86,
        "mount_to_first_frame_ms": 134,
        "play_to_first_frame_ms": 48,
        "duration_ms": 3600000,
        "offset_ms": 1688,
        "player_height_px": 123,
        "player_width_px": 124,
        "video_height_px": 12345,
        "video_width_px": 124,
        "window_height_px": 532,
        "window_width_px": 234
    }
}
`

var (
	deviceTypes = []string{
		"Macintosh",
		"Android",
		"iPhone",
		"iPad",
		"Windows",
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
		"Sweden",
		"Norway",
		"Finland",
		"Denmark",
		"Switzerland",
		"United Kingdom",
		"Ireland",
		"Netherlands",
		"Belgium",
		"Austria",
		"Portugal",
		"Greece",
	}
	playbackIds = []string{
		"abcdefgh-1",
		"abcdefgh-2",
		"abcdefgh-3",
		"abcdefgh-4",
		"abcdefgh-5",
		"abcdefgh-6",
		"abcdefgh-7",
		"abcdefgh-8",
		"abcdefgh-9",
		"abcdefgh-10",
		"abcdefgh-11",
		"abcdefgh-12",
		"abcdefgh-13",
		"abcdefgh-14",
		"abcdefgh-15",
		"abcdefgh-16",
		"abcdefgh-17",
		"abcdefgh-18",
		"abcdefgh-19",

		"ijklmnop-1",
		"ijklmnop-2",
		"ijklmnop-3",
		"ijklmnop-4",
		"ijklmnop-5",
		"ijklmnop-6",
		"ijklmnop-7",
		"ijklmnop-8",
		"ijklmnop-9",
		"ijklmnop-10",
		"ijklmnop-11",
		"ijklmnop-12",
		"ijklmnop-13",
		"ijklmnop-14",
		"ijklmnop-15",
		"ijklmnop-16",
		"ijklmnop-17",
		"ijklmnop-18",
		"ijklmnop-19",
		"ijklmnop-20",
		"ijklmnop-21",
		"ijklmnop-22",
		"ijklmnop-23",
		"ijklmnop-24",

		"qrstuvwx-1",
		"qrstuvwx-2",
		"qrstuvwx-3",
		"qrstuvwx-4",
		"qrstuvwx-5",
		"qrstuvwx-6",
		"qrstuvwx-7",
		"qrstuvwx-8",
		"qrstuvwx-9",
		"qrstuvwx-10",
		"qrstuvwx-11",
		"qrstuvwx-12",
		"qrstuvwx-13",
		"qrstuvwx-14",
		"qrstuvwx-15",
		"qrstuvwx-16",
		"qrstuvwx-17",
		"qrstuvwx-18",
	}
)

func main() {
	var (
		playbackID    = "abcdefgh-1"
		sessionNumber = 10
		kafkaTopic    = "viewership_events"
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
		playbackID := playbackIds[rand.Intn(len(playbackIds))]
		sessionID := fmt.Sprintf("%s-%d-%d", playbackID, i, rand.Intn(1000))
		deviceType := deviceTypes[rand.Intn(len(deviceTypes))]
		browser := browsers[rand.Intn(len(browsers))]
		country := countries[rand.Intn(len(countries))]
		creatorId := "creator12345"

		go func() {
			// Start each goroutine with a random delay
			time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			for {
				kafkaMsgs <- msg(
					playbackID,
					sessionID,
					deviceType,
					browser,
					country,
					creatorId,
				)
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

func msg(
	playbackID string,
	sessionID string,
	deviceType string,
	browser string,
	country string,
	creatorId string,
) string {
	continent := "Europe"
	userId := "user12345"
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
		timestamp,
	)
}
