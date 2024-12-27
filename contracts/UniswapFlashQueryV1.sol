//SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "../../common/interfaces/IBaseV1Pair.sol";
import "../../common/interfaces/IUniswapV2Pair.sol";
import "../../common/dexes/UniswapV2Factory.sol";

// In order to quickly load up data from Uniswap-like market, this contract allows easy iteration with a single eth_call
contract FlashUniswapQueryV1 {
	function getReservesByPairs(IUniswapV2Pair[] calldata _pairs) external view returns (uint256[3][] memory) {
		uint256[3][] memory result = new uint256[3][](_pairs.length);
		for (uint256 i = 0; i < _pairs.length; i++) {
			(result[i][0], result[i][1], result[i][2]) = _pairs[i].getReserves();
		}
		return result;
	}

	function getPairsByIndexRange(
		UniswapV2Factory _uniswapFactory,
		uint256 _start,
		uint256 _stop
	) external view returns (address[3][] memory) {
		uint256 _allPairsLength = _uniswapFactory.allPairsLength();
		if (_stop > _allPairsLength) {
			_stop = _allPairsLength;
		}
		require(_stop >= _start, "start cannot be higher than stop");
		uint256 _qty = _stop - _start;
		address[3][] memory result = new address[3][](_qty);
		for (uint256 i = 0; i < _qty; i++) {
			IUniswapV2Pair _uniswapPair = IUniswapV2Pair(_uniswapFactory.allPairs(_start + i));
			result[i][0] = _uniswapPair.token0();
			result[i][1] = _uniswapPair.token1();
			result[i][2] = address(_uniswapPair);
		}
		return result;
	}

	function filterVolatileHermesPairs(IBaseV1Pair[] calldata _pairs) external view returns (bool[] memory) {
		bool[] memory result = new bool[](_pairs.length);
		for (uint256 i = 0; i < _pairs.length; i++) {
			(, , , , result[i], , ) = _pairs[i].metadata();
		}
		return result;
	}
}
