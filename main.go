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
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

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

	conf := config{index: *flagIndex, java: *flagJavaBin, jar: *flagTikaJar, tikaPort: *flagTikaPort}
	http.HandleFunc("/", conf.rootHandler)

	Log.Info("Trying Tika server")
	if err := conf.ensureTikaServer(); err != nil {
		Log.Crit("Start Tika server", "error", err)
		os.Exit(1)
	}
	Log.Info("Tika server started successfully.")
	_ = conf.killTikaServer()

	Log.Info("Listening on " + *flagAddr)
	Log.Info("Running", "error", http.ListenAndServe(*flagAddr, nil))
}

type config struct {
	index     string
	java, jar string
	tikaPort  int

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
