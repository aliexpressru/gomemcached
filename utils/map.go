package utils

func PickByKeys(in map[string][]byte, keys []string) map[string][]byte {
	r := make(map[string][]byte, len(keys))
	for i := range keys {
		if v, ok := in[keys[i]]; ok {
			r[keys[i]] = v
		}
	}
	return r
}
