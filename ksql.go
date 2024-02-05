package main

import (
	"context"
	"fmt"
	"github.com/thmeitz/ksqldb-go"
	"github.com/thmeitz/ksqldb-go/net"
	"os"
	"time"
)

var query = `
SELECT
	DEVICE_TYPE,
	BROWSER,
	CONTINENT,
	COUNTRY,
    COUNT_DISTINCT(SESSION_ID) as VIEW_COUNT,
	  AVG(
		  CASE
		    WHEN ERRORS > 0 THEN 1
		    ELSE 0
		  END
	  ) AS ERROR_RATE,
	  AVG((WAITTIME_MS + STALLTIME_MS) / (PLAYTIME_MS + WAITTIME_MS + STALLTIME_MS)) as REBUFFER_RATE
  FROM  PLAYBACK_LOGS_STREAM
  WINDOW TUMBLING(SIZE 5 SECONDS)
WHERE PLAYBACK_ID = 'abcdefgh-1'
GROUP BY
	DEVICE_TYPE,
	BROWSER,
	CONTINENT,
	COUNTRY
EMIT CHANGES;
`

func createKsqlClient() (ksqldb.KsqldbClient, error) {
	url, _ := os.LookupEnv("KSQL_URL")
	username, _ := os.LookupEnv("KSQL_API_KEY")
	password, _ := os.LookupEnv("KSQL_API_SECRET")

	options := net.Options{
		Credentials: net.Credentials{Username: username, Password: password},
		BaseUrl:     url,
		AllowHTTP:   true,
	}
	return ksqldb.NewClientWithOptions(options)
}

func queryKSQL() {
	kcl, err := createKsqlClient()
	if err != nil {
		fmt.Println(err)
	}
	defer kcl.Close()

	fmt.Println(query)
	// you can disable parsing with `kcl.EnableParseSQL(false)`

	rowChannel := make(chan ksqldb.Row)
	headerChannel := make(chan ksqldb.Header, 1)

	// This Go routine will handle rows as and when they
	// are sent to the channel
	go func() {
		//var dataTs float64
		//var id string
		//var name string
		//var dogSize string
		//var age string
		for row := range rowChannel {
			if row != nil {
				//// Should do some type assertions here
				//dataTs = row[0].(float64)
				//id = row[1].(string)
				//name = row[2].(string)
				//dogSize = row[3].(string)
				//age = row[4].(string)
				//
				//// Handle the timestamp
				//t := int64(dataTs)
				//ts := time.Unix(t/1000, 0).Format(time.RFC822)
				//
				//fmt.Printf("🐾 New dog at %v: '%v' is %v and %v (id %v)\n", ts, name, dogSize, age, id)
				fmt.Println(row)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	e := kcl.Push(ctx, ksqldb.QueryOptions{Sql: query}, rowChannel, headerChannel)
	if e != nil {
		fmt.Println(e)
	}
}
