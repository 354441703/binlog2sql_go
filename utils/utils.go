package utils

func Contains(list []string, key string) bool {
	for _, l := range list {
		if l == key {
			return true
		}
	}
	return false
}
