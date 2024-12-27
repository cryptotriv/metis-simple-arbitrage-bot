//SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "../../common/interfaces/IERC20.sol";
import "../../common/interfaces/IUniswapV2PairV1.sol";
import "../../common/utils/Withdrawable.sol";

contract TokenProvidenceV1 is Withdrawable {
	address public immutable NATIVE_TOKEN;

	uint256 constant MAX_UINT = 2 ** 256 - 1 - 100;

	constructor(address owner_, address nativeToken_) Withdrawable(owner_) {
		NATIVE_TOKEN = nativeToken_;
	}

	receive() external payable {}

	function getAmountOut(
		uint256 amountIn,
		uint256 reserveIn,
		uint256 reserveOut,
		uint256 fee
	) internal pure returns (uint256 amountOut) {
		require(amountIn > 0, "TokenProvidence: INSUFFICIENT_INPUT_AMOUNT");
		require(reserveIn > 0 && reserveOut > 0, "TokenProvidence: INSUFFICIENT_LIQUIDITY");
		uint256 normalisedFee = 10000 - fee;
		uint256 amountInWithFee = amountIn * normalisedFee;
		uint256 numerator = amountInWithFee * reserveOut;
		uint256 denominator = reserveIn * 10000 + amountInWithFee;
		amountOut = numerator / denominator;
	}

	function getAmountIn(
		uint256 amountOut,
		uint256 reserveIn,
		uint256 reserveOut,
		uint256 fee
	) internal pure returns (uint256 amountIn) {
		require(amountOut > 0, "TokenProvidence: INSUFFICIENT_OUTPUT_AMOUNT");
		require(reserveIn > 0 && reserveOut > 0, "TokenProvidence: INSUFFICIENT_LIQUIDITY");
		uint256 normalisedFee = 10000 - fee;
		uint256 numerator = reserveIn * amountOut * 10000;
		uint256 denominator = (reserveOut - amountOut) * normalisedFee;
		amountIn = (numerator / denominator) + 1;
	}

	// This function should only be CALLED off-chain
	function healthCheck(address marketAddress, address token, uint256 fee) external payable {
		// Buy token by estimating how many tokens you will get.
		// After buying, compare it with the tokens you have. Can help in catching:
		// 1. Internal Fee Scams
		// 2. Low profit margins in sandwitch bots
		// 3. Potential rugs (high internal fee is often a rug)
		uint256 amountInNative = msg.value;

		// Get the market, token and reserves
		IUniswapV2PairV1 market = IUniswapV2PairV1(marketAddress);
		IERC20 native = IERC20(NATIVE_TOKEN);
		IERC20 t = IERC20(token);

		{
			(uint256 reserve0, uint256 reserve1, ) = market.getReserves();
			address token0 = market.token0();

			uint256 reserveIn = token0 == NATIVE_TOKEN ? reserve0 : reserve1;
			uint256 reserveOut = token0 == NATIVE_TOKEN ? reserve1 : reserve0;

			// Transfer Native token
			native.transfer(marketAddress, amountInNative);

			// Figure out how much we should get
			uint256 amountOut = getAmountOut(amountInNative, reserveIn, reserveOut, fee);

			// Figure out which token is our token
			uint256 amount0Out = token0 == NATIVE_TOKEN ? 0 : amountOut;
			uint256 amount1Out = token0 == NATIVE_TOKEN ? amountOut : 0;

			// Do the swap
			market.swap(amount0Out, amount1Out, address(this), "");

			// Check how many tokens we received
			uint256 balance = t.balanceOf(address(this));
			require(balance >= amountOut, "TokenProvidence: TOKEN HAS INTERNAL FEE");
		}

		// Sell token. Keep track of native before and after.
		// Can catch the following:
		// 1. Honeypots
		// 2. Internal Fee Scams
		// 3. Buy diversions
		{
			uint256 balance = t.balanceOf(address(this));
			(uint256 reserve0, uint256 reserve1, ) = market.getReserves();
			address token0 = market.token0();

			uint256 reserveIn = token0 == NATIVE_TOKEN ? reserve0 : reserve1;
			uint256 reserveOut = token0 == NATIVE_TOKEN ? reserve1 : reserve0;
			uint256 amountBack = getAmountOut(balance, reserveOut, reserveIn, fee);

			// Send back the tokens
			t.transfer(marketAddress, balance);

			// Get back metis
			uint256 amount0Out = token0 == NATIVE_TOKEN ? amountBack : 0;
			uint256 amount1Out = token0 == NATIVE_TOKEN ? 0 : amountBack;

			market.swap(amount0Out, amount1Out, address(this), "");

			// Check balance
			balance = native.balanceOf(address(this));
			require(balance >= amountBack, "TokenProvidence: TOKEN HAS INTERNAL FEE");
		}
	}
}
