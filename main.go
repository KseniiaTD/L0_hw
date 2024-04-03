package main

import (
	"html/template"
	"l0/cache"
	"l0/database"
	"l0/service"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	stan "github.com/nats-io/stan.go"
)

var tpl = template.Must(template.ParseFiles("index.html"))

func indexHandler(w http.ResponseWriter, r *http.Request) {
	tpl.Execute(w, nil)
}

func main() {
	clusterID := "test-cluster"
	clientID := "myClient"
	//User := "ruser"
	//Password := "x-files"
	//URL := fmt.Sprintf("nats://%s:%s@127.0.0.1:4222", User, Password)
	URL := stan.DefaultNatsURL
	subject := "json_parse_order"
	qgroup := "group_1"

	db := database.Database{}
	err := db.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	cache := cache.NewCache(5*time.Minute, 30*time.Minute)
	service := service.New(db, cache)

	err = service.RestoreCache()
	if err != nil {
		log.Fatal(err)
	}

	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(URL),

		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		}))

	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}

	defer sc.Close()

	msg_handler := func(msg *stan.Msg) {
		if msg != nil && msg.Data != nil {
			log.Println("message received")
			service.AddNewOrder(msg.Data)
			msg.Ack()
		}
	}

	sub, err := sc.QueueSubscribe(subject, qgroup, msg_handler, stan.SetManualAckMode())

	if err != nil {
		sc.Close()
		log.Fatal(err)
	}

	defer sub.Unsubscribe()

	http.HandleFunc("/data", service.GetByIdHandler)
	http.HandleFunc("/", indexHandler)
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}
