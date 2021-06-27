package utils

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
)

func SerializeGoObjectGOB(obj interface{}) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(obj); err != nil {
		return nil
	}
	return buf.Bytes()
}

func DeserializeGoObjectGOB(buf []byte, obj interface{}) error {
	if buf == nil {
		return errors.New("Can't desrialize null object")
	}
	if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(obj); err != nil {
		return err
	}

	return nil
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}