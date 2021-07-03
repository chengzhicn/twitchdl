package main

import (
	"bufio"
	"crypto/md5"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func newRequest(method, url string, body io.Reader) (req *http.Request, err error) {
	if req, err = http.NewRequest(method, url, body); err == nil {
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0")
		req.Header.Set("Accept", "application/vnd.twitchtv.v5+json")
		req.Header.Set("Referer", "https://www.twitch.tv/videos/"+flag.Args()[0])
		req.Header.Set("client-id", "jzkbprff40iqj646a697cyrvl0zt2m6")
	}
	return
}

func headChunk(chunk string) (info map[string]interface{}, err error) {
	if headReq, err := newRequest("HEAD", chunk, nil); err != nil {
		return nil, fmt.Errorf("failed to create HEAD request %s: %s\n", chunk, err.Error())
	} else if headRsp, err := http.DefaultClient.Do(headReq); err != nil {
		return nil, fmt.Errorf("failed to do HEAD request %s: %s\n", chunk, err.Error())
	} else if headRsp.StatusCode == 200 && headRsp.ContentLength > 0 {
		info = make(map[string]interface{})
		info["url"] = chunk
		info["size"] = headRsp.ContentLength
		headRsp.Body.Close()
	} else if headRsp.StatusCode != 403 {
		return nil, fmt.Errorf("HEAD request %s got unexpected status: %d\n", chunk, headRsp.StatusCode)
	} else {
		headRsp.Body.Close()
	}
	return info, nil
}

func headChunkWithRetry(chunk string, retry int, timeout time.Duration) (info map[string]interface{}) {
	var err error
	for i := 0; i <= retry; i++ {
		if info, err = headChunk(chunk); err == nil {
			return
		}
		time.Sleep(timeout)
	}
	log.Fatal(err)
	return
}

func fetchPlaylist(m3u string) (playlist map[string]int) {
	playlist = make(map[string]int)
	if m3uReq, err := newRequest("GET", m3u, nil); err != nil {
		log.Fatalf("failed to create playlist request %s: %s\n", m3u, err.Error())
	} else if m3uRsp, err := http.DefaultClient.Do(m3uReq); err != nil {
		log.Fatalf("failed to do playlist request %s: %s\n", m3u, err.Error())
	} else if m3uRsp.StatusCode == 200 && m3uRsp.ContentLength != 0 {
		m3uScanner := bufio.NewScanner(m3uRsp.Body)
		for index := 0; m3uScanner.Scan(); {
			if line := m3uScanner.Text(); len(line) != 0 && string(line[0]) != "#" {
				playlist[line] = index
				index++
			}
		}
		m3uRsp.Body.Close()
		if err = m3uScanner.Err(); err != nil {
			log.Fatalf("failed to parse playlist %s: %s\n", m3u, err.Error())
		} else if len(playlist) == 0 {
			log.Fatalf("playlist %s is empty\n", m3u)
		}
	} else if m3uRsp.StatusCode != 403 {
		log.Fatalf("playlist request %s got unexpected status: %d\n", m3u, m3uRsp.StatusCode)
	} else {
		m3uRsp.Body.Close()
	}
	return
}

func getInfo(output, quality, playlist string, concurrent, startChunk, endChunk int) (result []map[string]interface{}) {
	var videoInfo map[string]interface{}
	var base string
	if output == "" {
		output = path.Base(flag.Args()[0]) + ".ts"
	}
	if infoReq, err := newRequest("GET", "https://api.twitch.tv/kraken/videos/v"+path.Base(flag.Args()[0]), nil); err != nil {
		log.Fatalf("failed to create video info request: %s\n", err.Error())
	} else if infoRsp, err := (&http.Client{}).Do(infoReq); err != nil {
		log.Fatalf("failed to do video info request: %s\n", err.Error())
	} else if infoRsp.StatusCode != 200 {
		log.Fatalf("video info request got unexpected status: %d\n", infoRsp.StatusCode)
	} else if err = json.NewDecoder(infoRsp.Body).Decode(&videoInfo); err != nil {
		log.Fatalf("failed to decode video info request: %s\n", err.Error())
	} else if infoUrl, err := url.Parse(videoInfo["seek_previews_url"].(string)); err != nil {
		log.Fatalf("failed to parse url %s: %s\n", videoInfo["seek_previews_url"].(string), err.Error())
	} else {
		infoRsp.Body.Close()
		infoUrl.Path = path.Dir(path.Dir(infoUrl.Path))
		base = infoUrl.String() + "/" + quality + "/"
		if playlist == "" {
			if playlist = "index-dvr.m3u8"; videoInfo["broadcast_type"].(string) == "highlight" {
				playlist = "highlight-" + flag.Args()[0] + ".m3u8"
			}
		}
		log.Printf("base url: %s, playlist: %s\n", base, playlist)
	}
	chunks := fetchPlaylist(base + playlist)
	jobs := make(chan string, len(chunks))
	done := make(chan map[string]interface{}, len(chunks))
	reNewMuted := regexp.MustCompile(`^(\d+)\.`)
	reOldMuted := regexp.MustCompile(`^([^.]*)(-\d+)\.`)
	for i := 0; i < concurrent; i++ {
		go func(jobs chan string) {
			for chunk := range jobs {
				chunkInfo := headChunkWithRetry(base+chunk, 2, time.Second)
				if chunkInfo == nil {
					chunkInfo = headChunkWithRetry(base+reNewMuted.ReplaceAllString(reOldMuted.ReplaceAllString(chunk, "$1-muted$2."), "$1-muted."), 2, time.Second)
				}
				if chunkInfo != nil {
					chunkInfo["chunk"] = chunk
				}
				done <- chunkInfo
			}
		}(jobs)
	}
	sent, got, skipped := 0, 0, 0
	if len(chunks) != 0 {
		for chunk, index := range chunks {
			if index >= startChunk && index <= endChunk {
				jobs <- chunk
				sent++
			}
		}
		close(jobs)
		jobs = nil
	}
	for t := time.NewTimer(0); sent != got || jobs != nil; {
		select {
		case jobs <- strconv.Itoa(sent+startChunk) + ".ts":
			chunks[strconv.Itoa(sent+startChunk)+".ts"] = sent + startChunk
			sent++
		case item := <-done:
			got++
			if item != nil && chunks[item["chunk"].(string)] <= endChunk {
				result = append(result, item)
			} else if cap(done) != 0 {
				skipped++
			} else if jobs != nil {
				close(jobs)
				jobs = nil
			}
			if t.C == nil {
				t = time.NewTimer(time.Second)
			}
		case <-t.C:
			log.Printf("chunks checked: %d(%d skipped)/%d\n", got, skipped, sent)
			t.C = nil
		}
	}
	log.Printf("checked %d chunks, skipped %d muted chunks\n", sent, skipped)
	sort.Slice(result, func(i, j int) bool {
		return chunks[result[i]["chunk"].(string)] < chunks[result[j]["chunk"].(string)]
	})
	for i, offset := 0, int64(0); i < len(result); i++ {
		result[i]["output"] = output
		result[i]["offset"] = offset
		offset += result[i]["size"].(int64)
	}
	return
}

func downloadChunk(job map[string]interface{}, orgSize int64, done chan bool) error {
	buf := bufio.NewWriterSize(nil, 1024*1024)
	output, err := os.OpenFile(job["output"].(string), os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file %s: %s\n", job["output"], err.Error())
	} else if _, err = output.Seek(job["offset"].(int64), 0); err != nil {
		return fmt.Errorf("failed to seek output file %s: %s\n", job["output"], err.Error())
	}
	downloadReq, err := newRequest("GET", job["url"].(string), nil)
	if err != nil {
		return fmt.Errorf("failed to create download request %s: %s\n", job["url"], err.Error())
	}
	if orgSize >= job["offset"].(int64)+job["size"].(int64) {
		h := md5.New()
		if _, err := io.CopyN(h, output, job["size"].(int64)); err != nil {
			return fmt.Errorf("failed to read output file %s: %s\n", job["output"], err.Error())
		} else if _, err = output.Seek(job["offset"].(int64), 0); err != nil {
			return fmt.Errorf("failed to seek output file %s: %s\n", job["output"], err.Error())
		}
		downloadReq.Header.Set("If-None-Match", fmt.Sprintf(`"%x"`, h.Sum(nil)))
	}
	buf.Reset(output)
	if downloadRsp, err := http.DefaultClient.Do(downloadReq); err != nil {
		return fmt.Errorf("failed to do download request %s: %s\n", job["url"], err.Error())
	} else if (downloadRsp.StatusCode != 200 || downloadRsp.ContentLength != job["size"].(int64)) && (downloadRsp.StatusCode != 304 || downloadRsp.ContentLength != 0) {
		return fmt.Errorf("download request %s got unexpected response: %d %d\n", job["url"], downloadRsp.StatusCode, downloadRsp.ContentLength)
	} else if _, err = io.Copy(buf, downloadRsp.Body); err != nil {
		return fmt.Errorf("failed to download chunk %s: %s\n", job["url"], err.Error())
	} else if err = buf.Flush(); err != nil {
		return fmt.Errorf("failed to flush chunk %s: %s\n", job["url"], err.Error())
	} else {
		downloadRsp.Body.Close()
		output.Close()
		done <- downloadRsp.ContentLength == 0
		return nil
	}
}

func downloadChunkWithRetry(job map[string]interface{}, orgSize int64, done chan bool, retry int, timeout time.Duration) {
	var err error
	for i := 0; i <= retry; i++ {
		if err = downloadChunk(job, orgSize, done); err == nil {
			return
		}
		time.Sleep(timeout)
	}
	log.Fatal(err)
}

func downloadChunks(chunks []map[string]interface{}, concurrent int, orgSize int64) {
	jobs := make(chan map[string]interface{}, len(chunks))
	done := make(chan bool, len(chunks))
	for i := range chunks {
		jobs <- chunks[i]
	}
	close(jobs)
	for i := 0; i < concurrent; i++ {
		go func() {
			for job := range jobs {
				downloadChunkWithRetry(job, orgSize, done, 5, 5*time.Second)
			}
		}()
	}
	got, skipped := 0, 0
	for t := time.NewTimer(0); got != len(chunks); {
		select {
		case cached := <-done:
			got++
			if cached {
				skipped++
			}
			if t.C == nil {
				t = time.NewTimer(time.Second)
			}
		case <-t.C:
			log.Printf("chunks downloaded: %d(%d skipped)/%d\n", got, skipped, len(chunks))
			t.C = nil
		}
	}
	log.Printf("%d chunks downloaded, skipped %d already downloaded chunks\n", got, skipped)
}

func main() {
	var outputFile string
	var quality string
	var playlist string
	var bindAddr string
	var concurrent int
	var startChunk int
	var endChunk int
	var insecure bool
	flag.StringVar(&outputFile, "o", "", "output file")
	flag.StringVar(&playlist, "p", "", "alternative playlist")
	flag.StringVar(&quality, "q", "chunked", "video quality")
	flag.StringVar(&bindAddr, "b", "", "bind address")
	flag.IntVar(&concurrent, "c", 8, "concurrent downloads")
	flag.IntVar(&startChunk, "s", 0, "start chunk index")
	flag.IntVar(&endChunk, "e", 999999999, "end chunk index")
	flag.BoolVar(&insecure, "i", false, "force using http instead of https")
	flag.Parse()

	log.Printf("disabling HTTP/2 because it's crawling under packet loss...\n")
	http.DefaultClient.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			LocalAddr: &net.TCPAddr{
				IP: net.ParseIP(bindAddr), // empty bindAddr will return nil
			},
		}).DialContext,
		IdleConnTimeout: 3 * time.Second,
		TLSNextProto:    make(map[string]func(string, *tls.Conn) http.RoundTripper),
	}
	log.Printf("fetching chunk info...\n")
	chunksInfo := getInfo(outputFile, quality, playlist, concurrent, startChunk, endChunk)
	if insecure {
		for i := range chunksInfo {
			chunksInfo[i]["url"] = strings.Replace(chunksInfo[i]["url"].(string), "https://", "http://", 1)
		}
	}
	log.Printf("finished fetching chunk info, total %d chunks to download\n", len(chunksInfo))
	output := chunksInfo[0]["output"].(string)
	size := chunksInfo[len(chunksInfo)-1]["offset"].(int64) + chunksInfo[len(chunksInfo)-1]["size"].(int64)
	if f, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		log.Fatalf("failed to open output file %s: %s\n", output, err.Error())
	} else if fi, err := f.Stat(); err != nil {
		log.Fatalf("failed to stat output file %s: %s\n", output, err.Error())
	} else if err := f.Truncate(size); err != nil {
		log.Fatalf("failed to truncate output file %s: %s\n", output, err.Error())
	} else {
		f.Close()
		log.Printf("downloading to file %s, total size: %d\n", output, size)
		downloadChunks(chunksInfo, concurrent, fi.Size())
		log.Printf("download finished\n")
	}
}
