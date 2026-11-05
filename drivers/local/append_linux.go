package local

import "os"

func appendFile(file *os.File, p []byte) (int, error) { return file.Write(p) }
