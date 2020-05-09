package main

import (
	"os"
	"fmt"
	"time"
	"math"
	"strings"
	"context"
	"github.com/gocolly/colly/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
) 

// MONGO CONNECTIONS
var mongo_pword   			= osCheck("MONGO_PWORD")
var mongo_uname   			= osCheck("MONGO_UNAME")
var mongo_host    			= osCheck("MONGO_HOST")
var mongoURI      			= fmt.Sprintf("mongodb+srv://%s:%s@%s", mongo_uname, mongo_pword, mongo_host)
var clientOptions 			= options.Client().ApplyURI(mongoURI)
var mongoClient, mongoErr 	= mongo.Connect(context.TODO(), clientOptions)
var mongoCol 				= mongoClient.Database("test").Collection("stocks")
var opts 					= options.Replace().SetUpsert(true)

//DPT NUMBER (https://gobyexample.com/time)
var pts_per_stock 	= 13.0
var loc, err 		= time.LoadLocation("America/New_York")
var n 				= time.Now().In(loc)
var nyse_open 		= time.Date(n.Year(), n.Month(), n.Day(), 9, 30, 0, 0, n.Location())
var nyse_close 		= time.Date(n.Year(), n.Month(), n.Day(), 14, 0, 0, 0, n.Location())
var nyse_timedelta 	= nyse_close.Sub(nyse_open)
var nyse_sec 		= nyse_timedelta.Seconds()
var now_timedelta 	= n.Sub(nyse_open)
var now_sec 		= now_timedelta.Seconds()
var dpt  			= math.Round(now_sec / nyse_sec * pts_per_stock)

func osCheck(os_var string) (string) {
	val, ok := os.LookupEnv(os_var)
    if !ok {
        fmt.Println("error: unable to find MONGO_PW in the environment")
        os.Exit(1)
    }
	return val
}

//https://stackoverflow.com/questions/55306617/how-to-add-values-to-an-bson-d-object
func map2bson (m map[string]string) (bson.D) {
	var mongoDoc bson.D
    for k,v := range m { mongoDoc = append(mongoDoc, bson.E{k, v}) }
	return append(mongoDoc, bson.E{"dpt", dpt})
}


func bson2mongo(ticker string, mongoDoc bson.D) {
	filter := bson.M{"ticker":ticker,"dpt":dpt}
	mongoCol.ReplaceOne(context.TODO(), filter, mongoDoc, opts)
}

func array2map(array []string) (map[string]string) {
	if len(array) > 0 && len(array) % 2 == 0 {
		var Map = make(map[string]string)
		for i := 0; i < len(array); i +=2 { Map[array[i]] = array[i+1] }
		return Map
	} else {
		return nil
	}
}

// https://edmundmartin.com/writing-a-web-crawler-with-golang-and-colly/
func main() {

	if dpt >= 0 && dpt <= 13 {

		var start = time.Now()

		// DROP DB IF FIRST DPT
		if dpt == 0 { mongoCol.Drop(context.TODO()) }

		// Instantiate default collector
		c := colly.NewCollector(
			colly.AllowedDomains("finviz.com"),
			colly.Async(true),
		)

		c.Limit(&colly.LimitRule{
			DomainGlob: "*", 
			Parallelism: 20,  //15
		})

		c.AllowURLRevisit = false

		c.OnHTML("a[href]", func(e *colly.HTMLElement) {
			link := e.Attr("href")
			if strings.Contains(link, "screener.ashx?v=521&r=") {
				if e.Text == "next" {
					c.Visit(e.Request.AbsoluteURL(link))
				}
			} else if strings.Contains(link, "quote.ashx?t=") {
				c.Visit("https://finviz.com/quote.ashx?t="+e.Text)
			}
		})

		c.OnHTML("tbody", func(e *colly.HTMLElement) {

			var data []string;
			var ticker string

			e.ForEach(".fullview-ticker", func(_ int, e *colly.HTMLElement) { 
				ticker = e.Text
				data = append(data, "ticker")
				data = append(data, ticker)
			})
			
			e.ForEach("tr .table-dark-row", func(_ int, row *colly.HTMLElement) { 
				row.ForEach("td", func(_ int, el *colly.HTMLElement) {
					data = append(data, el.Text)
				})
			})
			
			if len(ticker) > 0 && len(data) > 10 {
					m := array2map(data)
					b := map2bson(m)
					bson2mongo(ticker, b)
					data = data[:0]
			}
		
		})

		// Before making a request print "Visiting ..."
		c.OnRequest(func(r *colly.Request) {
			fmt.Println("Visiting: ", r.URL.String())
		})
		
		c.Visit("https://finviz.com/screener.ashx?v=521")

		c.Wait()
		
		fmt.Println(time.Since(start))

	}
	
}
