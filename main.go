// Copyright 2015 Tamás Gulácsi
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/blevesearch/bleve"
	"gopkg.in/inconshreveable/log15.v2"
)

var Log = log15.New()

func main() {
	Log.SetHandler(log15.StderrHandler)

	flagAddr := flag.String("http", ":9997", "host:port to listen on")
	flagJavaBin := flag.String("java", "/usr/bin/java", "absolute path of the java binary")
	flagTikaJar := flag.String("tika-jar", "/usr/local/share/java/tika-server.jar", "absolute path of the Tika jar")
	flagTikaPort := flag.Int("tika-port", 9998, "Tika port")
	flagIndex := flag.String("index", "/data/index.bleve", "absolute path of the Bleve index file")
	flag.Parse()

	conf := config{java: *flagJavaBin, jar: *flagTikaJar, tikaPort: *flagTikaPort,
		httpClient: http.DefaultClient,
	}
	_, err := os.Stat(*flagIndex)
	if err == nil { //exist
		conf.index, err = bleve.Open(*flagIndex)
	} else {
		conf.index, err = bleve.New(*flagIndex, bleve.NewIndexMapping())
	}
	if err != nil {
		Log.Crit("Open bleve index", "path", *flagIndex, "error", err)
		os.Exit(2)
	}

	Log.Info("Trying Tika server")
	if err := conf.ensureTikaServer(); err != nil {
		Log.Crit("Start Tika server", "error", err)
		os.Exit(1)
	}

	Log.Info("Tika server started successfully.")
	http.HandleFunc("/search", conf.searchHandler)
	http.HandleFunc("/add", conf.addHandler)
	http.HandleFunc("/", conf.rootHandler)

	Log.Info("Listening on " + *flagAddr)
	Log.Info("Running", "error", http.ListenAndServe(*flagAddr, nil))
}

type config struct {
	index      bleve.Index
	java, jar  string
	tikaPort   int
	httpClient *http.Client

	tikaMu sync.Mutex
	tika   *os.Process
	tikaCh chan error
}

func (c config) rootHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "PUT":
		c.putHandler(w, r)
	case "GET":
		c.getHandler(w, r)
	default:
		http.Error(w, "invalid method; allowed: GET,PUT", http.StatusMethodNotAllowed)
	}
}

func (c config) putHandler(w http.ResponseWriter, r *http.Request) {
	if err := c.ensureTikaServer(); err != nil {
		http.Error(w, fmt.Sprintf("Cannot start Tika server: %v", err), http.StatusInternalServerError)
		return
	}
}
func (c config) getHandler(w http.ResponseWriter, r *http.Request) {
}

func (c config) searchHandler(w http.ResponseWriter, r *http.Request) {
}
func (c config) addHandler(w http.ResponseWriter, r *http.Request) {
	if err := c.ensureTikaServer(); err != nil {
		http.Error(w, fmt.Sprintf("Start Tika server: %v", err), http.StatusInternalServerError)
		return
	}

	ct := r.Header.Get("Content-Type")
	defer r.Body.Close()
	var id string
	bdy := io.ReadCloser(r.Body)
	if ct == "multipart/form-data" || ct == "application/x-www-form-encoded" {
		if err := r.ParseForm(); err != nil {
			http.Error(w, fmt.Sprintf("cannot parse request: %v", err), http.StatusBadRequest)
			return
		}
		id = r.Form.Get("id")
		if r.MultipartForm == nil || r.MultipartForm.File == nil {
			http.Error(w, "no file given!", http.StatusBadRequest)
			return
		}
		for k := range r.MultipartForm.File {
			var err error
			bdy, _, err = r.FormFile(k)
			if err != nil {
				http.Error(w, fmt.Sprintf("FormFile[%q]: %v", k, err), http.StatusBadRequest)
				return
			}
			defer bdy.Close()
			break
		}
	} else {
		id = r.URL.Query().Get("id")
	}
	if id == "" {
		http.Error(w, "id is required!", http.StatusBadRequest)
		return
	}

	meta, text, err := c.analyze(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("analyze: %v", err), http.StatusInternalServerError)
		return
	}
	Log.Debug("analyze", "meta", meta, "text", text)
	if err = c.store(id, meta, text); err != nil {
		http.Error(w, fmt.Sprintf("store: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(&document{ID: id, metadata: meta})
}

type document struct {
	ID string `json:"id"`
	metadata
	text string
}

func (c config) store(ID string, meta metadata, text string) error {
	return c.index.Index(ID, document{ID: ID, metadata: meta, text: text})
}

func (c config) analyze(r io.Reader) (metadata, string, error) {
	var (
		meta metadata
		text string
		buf  bytes.Buffer
	)

	baseUrl := "http://localhost:" + strconv.Itoa(c.tikaPort)
	// buffer data in memory
	r2 := io.TeeReader(r, &buf)
	// meta
	req, err := http.NewRequest("PUT", baseUrl+"/meta", r2)
	if err != nil {
		return meta, text, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return meta, text, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return meta, text, err
	}
	meta, err = readMeta(resp.Body)

	// buffer remaining data
	if _, err = io.Copy(ioutil.Discard, r2); err != nil {
		return meta, text, err
	}

	// get text
	if req, err = http.NewRequest("PUT", baseUrl+"/tika", bytes.NewReader(buf.Bytes())); err != nil {
		return meta, text, err
	}
	if resp, err = c.httpClient.Do(req); err != nil {
		return meta, text, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return meta, text, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return meta, text, err
	}
	text = string(b)
	return meta, text, err
}

type metadata struct {
	Author      string            `json:"author"`
	ContentType string            `json:"content-type"`
	Title       string            `json:"title"`
	Data        map[string]string `json:"data"`
	Created     time.Time         `json:"created"`
}

/*
"cp:revision","2"
"meta:last-author","altbac"
"Last-Author","altbac"
"meta:save-date","2013-05-03T07:46:00Z"
"Author","altbac"
"dcterms:created","2013-05-03T07:46:00Z"
"date","2013-05-03T07:46:00Z"
"extended-properties:Template","Normal"
"creator","altbac"
"Edit-Time","600000000"
"Creation-Date","2013-05-03T07:46:00Z"
"title","A BAGOLY TANODA NYÁRI TÁBORA"
"meta:author","altbac"
"dc:title","A BAGOLY TANODA NYÁRI TÁBORA"
"Last-Save-Date","2013-05-03T07:46:00Z"
"Revision-Number","2"
"Last-Printed","1601-01-01T00:00:00Z"
"meta:print-date","1601-01-01T00:00:00Z"
"meta:creation-date","2013-05-03T07:46:00Z"
"dcterms:modified","2013-05-03T07:46:00Z"
"Template","Normal"
"dc:creator","altbac"
"Last-Modified","2013-05-03T07:46:00Z"
"X-Parsed-By","org.apache.tika.parser.ParserDecorator$1","org.apache.tika.parser.microsoft.OfficeParser"
"modified","2013-05-03T07:46:00Z"
"Content-Type","application/msword"
*/
func readMeta(r io.Reader) (metadata, error) {
	var meta metadata
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := bytes.TrimLeft(bytes.TrimRight(scanner.Bytes(), "\"\n"), `"`)
		i := bytes.Index(line, []byte(`","`))
		if i < 0 {
			Log.Warn("no field separator", "line", scanner.Text())
			continue
		}
		key := string(line[:i])
		value := string(bytes.Replace(line[i+3:], []byte{'"'}, []byte{}, -1))
		Log.Debug("scan", "key", key, "value", value)

		switch key {
		case "Content-Type":
			meta.ContentType = value
		case "Author":
			meta.Author = value
		case "Creation-Date":
			var err error
			if meta.Created, err = time.Parse(time.RFC3339, value); err != nil {
				Log.Warn("parse Creation-Date", "text", value, "error", err)
			}
		case "title":
			meta.Title = value
		default:
			if meta.Data == nil {
				meta.Data = make(map[string]string, 32)
			}
			meta.Data[key] = value
		}
	}
	return meta, scanner.Err()
}

// ensureTikaServer checks whether the Tika server runs, and starts it if not.
// Writes the PID to config.tikaPID
func (c *config) ensureTikaServer() error {
	c.tikaMu.Lock()
	defer c.tikaMu.Unlock()
	if c.tikaCh != nil {
		select {
		case err := <-c.tikaCh:
			Log.Error("Tika stopped", "error", err)
		default:
			return nil
		}
		close(c.tikaCh)
	}
	c.tikaCh = make(chan error, 1)
	cmd := exec.Command(c.java, "-jar", c.jar, "-h", "localhost", "-p", strconv.Itoa(c.tikaPort))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	Log.Info("Starting Tika", "args", cmd.Args)
	if err := cmd.Start(); err != nil {
		return err
	}
	c.tika = cmd.Process
	go func() {
		err := cmd.Wait()
		Log.Info("Tika ended", "error", err)
		c.tikaCh <- err
	}()
	select {
	case err := <-c.tikaCh:
		return err
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

func (c *config) killTikaServer() error {
	c.tikaMu.Lock()
	defer c.tikaMu.Unlock()
	if c.tika == nil {
		return nil
	}
	tika := c.tika
	c.tika = nil
	exited := make(chan struct{}, 1)
	go func() {
		_, _ = tika.Wait()
		exited <- struct{}{}
	}()
	if err := tika.Signal(syscall.SIGTERM); err != nil {
		Log.Warn("TERMinating Tika", "pid", tika.Pid, "error", err)
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		if err := tika.Kill(); err != nil {
			Log.Warn("KILLing Tika", "pid", tika.Pid, "error", err)
		}
	}
	return nil
}
