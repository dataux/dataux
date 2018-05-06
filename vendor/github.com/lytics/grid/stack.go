package grid

import "strings"
import "bytes"

func niceStack(stack []byte) string {
	lines := strings.Split(string(stack), "\n")
	var buf bytes.Buffer
	for i, line := range lines {
		if strings.Contains(line, ":") {
			idx := strings.Index(line, "+")
			if idx > 0 {
				line = line[:idx-1]
				line = strings.Replace(line, "\t", "", -1)
				line = strings.Replace(line, " ", "", -1)
				buf.WriteString(line)
				if i < len(lines)-2 {
					buf.WriteString(" <-- ")
				}
			}
		}
	}
	return buf.String()
}
