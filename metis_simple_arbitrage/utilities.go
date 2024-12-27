package metis_simple_arbitrage

import "github.com/ethereum/go-ethereum/common"

func addressInSlice(a common.Address, list []string) bool {
	for _, b := range list {
		if common.HexToAddress(b) == a {
			return true
		}
	}
	return false
}

func getTokenIndexesInPair(addr1 common.Address, addr2 common.Address) (metisIndex int, tokenIndex int) {
	if addr1 == common.HexToAddress(METIS_TOKEN_ADDRESS) || addr1 == common.HexToAddress(WMETIS_TOKEN_ADDRESS) {
		return 0, 1
	} else if addr2 == common.HexToAddress(METIS_TOKEN_ADDRESS) || addr2 == common.HexToAddress(WMETIS_TOKEN_ADDRESS) {
		return 1, 0
	} else {
		return -1, -1
	}
}
