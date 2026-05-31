package granite

import (
	"fmt"
)

var bunits = [...]string{"", "Ki", "Mi", "Gi", "Ti"}

func shortenb(bytes int64) string {
	i := 0
	for ; bytes > 1024 && i < 4; i++ {
		bytes /= 1024
	}
	return fmt.Sprintf("%d%sB", bytes, bunits[i])
}
