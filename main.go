package main

import (
	"os"
	"fmt"
	"time"
	"strings"
	"context"
	
	"github.com/gocolly/colly/v2"
	
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
) 

var mongo_pword   = osCheck("MONGO_PWORD")
var mongo_uname   = osCheck("MONGO_UNAME")
var mongo_host    = osCheck("MONGO_HOST")
var mongoURI      = fmt.Sprintf("mongodb+srv://%s:%s@%s", mongo_uname, mongo_pword, mongo_host)
var clientOptions = options.Client().ApplyURI(mongoURI)
var mongoClient, mongoErr = mongo.Connect(context.TODO(), clientOptions)
var mongoCol = mongoClient.Database("test").Collection("finviz")


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
	return mongoDoc
}


func bson2mongo(mongoDoc bson.D) {
	mongoCol.InsertOne(context.TODO(), mongoDoc)
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
   	
	start := time.Now()

	mongoCol.Drop(context.TODO())

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

	//extensions.RandomUserAgent(c)

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
			data = append(data, "Ticker")
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
				bson2mongo(b)
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