package util

import (
	"bytes"
	"fmt"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"net/http"
)

var (
	backslashN  = []byte{'\\', 'n'}
	backslashR  = []byte{'\\', 'r'}
	backslashT  = []byte{'\\', 't'}
	backslashDQ = []byte{'\\', '"'}
	backslashBS = []byte{'\\', '\\'}
)

func Escape(key []byte) string {
	r, err := escape(key)
	if err != nil {
		log.Error("Escape string failed. ", fmt.Sprintf("err=%v", err))
		return ""
	}

	return r
}

func escape(key []byte) (string, error) {
	buf := bytes.Buffer{}

	for _, c := range key {
		var err error
		switch c {
		case '\n':
			_, err = buf.Write(backslashN)
		case '\r':
			_, err = buf.Write(backslashR)
		case '\t':
			_, err = buf.Write(backslashT)
		case '"':
			_, err = buf.Write(backslashDQ)
		case '\\':
			_, err = buf.Write(backslashBS)
		default:
			if c >= 0x20 && c < 0x7f {
				err = buf.WriteByte(c)
			} else {
				_, err = buf.WriteString(fmt.Sprintf("\\%03o", c))
			}
		}

		if err != nil {
			return "", err
		}
	}

	return string(buf.Bytes()), nil
}

func HttpGet(tidbServer string, tidbPort int, path string) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d/%s", tidbServer, tidbPort, path)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil && err == nil {
			err = errClose
		}
	}()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		// Print response body directly if status is not ok.
		fmt.Println(string(body))
		return nil, err
	}

	fmt.Println(string(body))
	return body, nil
}
