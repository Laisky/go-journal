package journal

import "crypto/rand"

func randomTestString(size int) string {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range data {
		data[i] = alphabet[int(data[i])%len(alphabet)]
	}
	return string(data)
}
