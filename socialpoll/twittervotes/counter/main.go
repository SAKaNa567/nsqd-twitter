package main

import (
	"fmt"
	"flag"
	"os"
	"log"
	"gopkg.in/mgo.v2"
	"sync"
	"github.com/bitly/go-nsq"
	"time"
	"gopkg.in/mgo.v2/bson"
	"os/signal"
	"syscall"
)

var fatalErr error

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
	fatalErr = e
}

const updateDuration = 1 * time.Second

func main() {
	defer func() {
		if fatalErr !=  nil {
			os.Exit(1)
		}
	}()


	//データベース接続などのリソースをクリーンアップする方法について、
	//考えるのは、リソースの取得に成功した直後が最適です。
	log.Println("データベースに接続します...")
	db, err := mgo.Dial("localhost")
	if err != nil{
		fatal(err)
		return
	}
	defer func() {
		log.Println("データベース接続を閉じます。...")
		db.Close()
	}()
	pollData := db.DB("ballots").C("polls")


	//mapとLockはGoでよく使われる組み合わせで、複数のGoroutine
	//が１つのマップにアクセスする際に、同時に読み書きを行ってマップを破壊
	// してしまうのを防ぐためのもの。
	var countsLock sync.Mutex
	var counts map[string]int

	log.Println("NSQに接続します...")
	q, err := nsq.NewConsumer("votes","counter",nsq.NewConfig())
	if err != nil {
		fatal(err)
		return
	}

	//nsqからメッセージを受け取った際の処理.
	q.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error{
		countsLock.Lock()
		defer countsLock.Unlock()
		if counts == nil {
			counts = make(map[string]int)
		}
		vote := string(m.Body)
		counts[vote]++
		return nil
	}))

	//　NSQのサービスに接続するコードを作成する.
	if err := q.ConnectToNSQLookupd("localhost:4161"); err != nil {
		fatal(err)
		return
	}


	//データベースへとプッシュするコード
	log.Println("NSQ上での投票を待機します...")
	var updater *time.Timer
	updater = time.AfterFunc(updateDuration, func() {
		countsLock.Lock()
		defer countsLock.Unlock()
		if len(counts) == 0 {
			log.Println("新しい投票はありません。データベースの更新をスキップします")
		} else {
			log.Println("データベース更新します...")
			log.Println(counts)
			ok := true
			for option, count := range counts {
				sel := bson.M{"options":bson.M{"$in":[]string{option}}}
				up := bson.M{"$inc": bson.M{"results." + option :count}}
				if _, err := pollData.UpdateAll(sel,up); err != nil {
					log.Println("更新に失敗しました:",err)
					ok = false
					continue
				}
				counts[option] = 0
			}
			if ok {
				log.Println("データベースの更新が完了しました。")
				counts = nil//投票数をリセットする.
			}
		}
		updater.Reset(updateDuration)
	})

	//Ctrl + Cへの応答...
		termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		select {
		case <-termChan:
			updater.Stop()
			 q.Stop()
		case <-q.StopChan:
			//完了しました。
			return
		}
	}
}
