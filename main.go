/*
Copyright © 2023 Rychart Redwerkz <redwerkz@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/queue"
	"github.com/kyokomi/emoji/v2"
	"github.com/mozillazg/go-unidecode"

  "go.uber.org/zap"

  "golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	JAN int = int(time.January)
	DEC int = int(time.December)
	VEN int = 0
  GEN int = 1

  ORG int = 2004
)

type Date string

func (d *Date) UnmarshalJSON(bytes []byte) error {
    dd, err := time.Parse(`"2006-01-02T15:04:05.000+0000"`, string(bytes))
    if err != nil{
        return err
    }
    *d = Date(dd.Format("01/02/2006"))

    return nil
}

type Event struct {
  Date  string  `json:"date"`
  Venue string  `json:"venue"`
  Genre string  `json:"genre"`
	Title string  `json:"title"`
	Text  string  `json:"text"`
	Link  string  `json:"link"`
}

func daysIn(m time.Month, year int) int {
	return time.Date(year, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func decode(s string) string {
  return repair(unidecode.Unidecode(s))
}

func escape(s string) string {
  return url.QueryEscape(s)
}

func remSpace(s string) string {
	rr := make([]rune, 0, len(s))
	for _, r := range s {
		if !unicode.IsSpace(r) {
			rr = append(rr, r)
		}
	}
	return string(rr)
}

func repair(s string) string {
  res := strings.ReplaceAll(s, "m\u0026B", "m'n'B")
  res = strings.ReplaceAll(res, "\"", "")
  res = strings.ReplaceAll(res, "kc", "küc")
  res = strings.ReplaceAll(res, "ebud", "ebäud")
  res = strings.ReplaceAll(res, "wlb", "wölb")
  res = strings.ReplaceAll(res, "Mrz", "März")
  res = strings.ReplaceAll(res, "show_event.pl?sts=det\u0026", "https://morgengrau.net/cgi-bin/morgengrau/show_event.pl?sts=det&")
  return res
}

func split(s string, sep string) []string {
	return strings.Split(decode(s), sep)
}

func title(s string) string {
  cass := cases.Title(language.English)
  return cass.String(trim(decode(repair(s))))
}

func trim(s string) string {
  return strings.TrimSpace(repair(s))
}

func scrape() (string, int, *zap.Logger) {

  logger, _ := zap.NewProduction()
  defer logger.Sync()

	u, err := url.Parse("https://www.morgengrau.net/cgi-bin/morgengrau/event_suche_action.pl?datumundzeit=event_such_form.pl&query=date&datesearch=1")
	if err != nil { logger.Fatal(err.Error()) }

	qry := u.Query()
  q, _ := queue.New(16, &queue.InMemoryQueueStorage{MaxSize: 10000})

  cur := time.Now()
  for y := ORG; y <= cur.Year(); y++ {
    qry.Set("year", strconv.Itoa(y))

    for m := JAN; m <= DEC; m++ {
      qry.Set("month", strconv.Itoa(m))

      days := daysIn(time.Month(m), y)
      for d := 1; d <= days; d++ {
        qry.Set("day", strconv.Itoa(d))
        u.RawQuery = qry.Encode()
        q.AddURL(u.String())
	    }
    }
  }

	c := colly.NewCollector(
    colly.Async(true),
	)
	c.SetRequestTimeout(120 * time.Second)
  c.Limit(&colly.LimitRule{
    DomainGlob: "*",
    Parallelism: 16,
    // Delay: 1 * time.Second,
    // RandomDelay: 1 * time.Millisecond,
  })

  fileName := "data/events.json"
  file, err := os.Create(fileName)
	if err != nil { logger.Fatal(err.Error()) }

  cnt := 0
  c.OnRequest(func(r *colly.Request) {
    logger.Info("visiting",
      zap.String("url", r.URL.String()),
    )
    cnt++
	})

	c.OnResponse(func(r *colly.Response) {
    logger.Info("response from",
      zap.String("url", r.Request.URL.RawQuery),
    )
	})

	c.OnError(func(_ *colly.Response, err error) { logger.Fatal(err.Error()) })

  events := make([]Event, 0)

  c.OnHTML("body", func(e *colly.HTMLElement) {
    res := trim(e.ChildText("font"))
    item := Event{}
    if !(strings.Contains(res, "nichts gefunden")) {
      item.Date = strings.TrimSuffix(strings.TrimPrefix(decode(res), "Events am "), ":")
    }

    e.ForEach("a.event_title", func(i int, s *colly.HTMLElement) {
      if !(strings.Contains(res, "nichts gefunden")) {
        item.Title = title(s.Text)
      }
    })

    e.ForEach("span.event_dates", func(i int, s *colly.HTMLElement) {
      about := split(s.Text, "|")
      item.Venue = trim(about[VEN])
      item.Genre = trim(about[GEN])
    })

    // e.ChildText("td.event_text", func(i int, s *colly.HTMLElement) {
    item.Text = remSpace(e.ChildText("td.event_text"))
    // })

    e.ForEach("a[href].event", func(i int, s *colly.HTMLElement) {
      item.Link = repair(s.Attr("href"))
    })

    if !(strings.Contains(res, "nichts gefunden")) {
      events = append(events, item)
    }
  })

	c.OnScraped(func(r *colly.Response) {

		js, err := json.MarshalIndent(events, "", "  ")
    e := json.NewEncoder(os.Stdout)
    e.SetEscapeHTML(false)
    e.Encode(js)

		if err != nil {	logger.Fatal(err.Error()) }
		  // emoji.Println("[INFO] Writing data to file ...")
		if err := os.WriteFile(fileName, js, 0664); err != nil {
			emoji.Println("[ERROR] Writing data to file :cross_mark:")
    }
	})

  q.Run(c)

  c.Wait()
  defer file.Close()

  return c.String(), cnt, logger
}

func main() {
  start := time.Now()

  c, cnt, logger := scrape()

  t := time.Since(start)

  logger.Info("stats", zap.String("time", t.String()))
  logger.Info("amount", zap.Int("amount", cnt))
  logger.Info("Scrapping complete")
  logger.Info(c)
}
