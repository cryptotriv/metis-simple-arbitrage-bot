//SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "../../common/interfaces/IERC20.sol";
import "../../common/interfaces/IFlashSwapCalleeV1.sol";
import "../../common/utils/Withdrawable.sol";
import "../../common/interfaces/IUniswapV2PairV1.sol";
import "../../common/interfaces/IWETH.sol";

struct Arb {
	address buyFromPair;
	uint8 buyFromFee;
	uint112 nativeInAmount;
	uint112 tokenAmount;
	uint112 nativeOutAmount;
	uint112 profit;
	address sellToPair;
	uint8 sellToFee;
	// uint112[2] buyReserve;
	// uint112[2] sellReserve;
	bool buyFromIsWMetis;
	bool sellToIsWMetis;
}

struct Vars {
	bool opportunityPresent;
	uint256 amountOutInter;
	uint256 amountOutProfit;
	uint8 denom;
}

contract FlashSwapExecutorV1 is Withdrawable {
	address public immutable NATIVE_TOKEN;
	IWETH public immutable WMETIS;

	constructor(address owner_, address nativeToken_, IWETH wmetis_) Withdrawable(owner_) {
		NATIVE_TOKEN = nativeToken_;
		WMETIS = wmetis_;
	}

	receive() external payable {}

	// By right, this should only be called by authorized addresses
	function executeNativeArb(Arb[] calldata arbs, uint112 minProfit) external {
		bool gotOpportunity = false;

		for (uint256 i = 0; i < arbs.length; i++) {
			Vars memory myVar = Vars(true, 0, 0, 1);

			// Check presence of opportunity
			{
				(uint256 buyReserve0, uint256 buyReserve1, ) = IUniswapV2PairV1(arbs[i].buyFromPair).getReserves();
				(uint256 sellReserve0, uint256 sellReserve1, ) = IUniswapV2PairV1(arbs[i].sellToPair).getReserves();

				for (uint8 k = 1; k <= 2; k++) {
					if (
						IUniswapV2PairV1(arbs[i].buyFromPair).token0() == NATIVE_TOKEN ||
						IUniswapV2PairV1(arbs[i].buyFromPair).token0() == address(WMETIS)
					) {
						myVar.amountOutInter = getAmountOut(
							arbs[i].nativeInAmount / k,
							buyReserve0,
							buyReserve1,
							arbs[i].buyFromFee
						);
					} else {
						myVar.amountOutInter = getAmountOut(
							arbs[i].nativeInAmount / k,
							buyReserve1,
							buyReserve0,
							arbs[i].buyFromFee
						);
					}

					if (
						IUniswapV2PairV1(arbs[i].sellToPair).token0() == NATIVE_TOKEN ||
						IUniswapV2PairV1(arbs[i].sellToPair).token0() == address(WMETIS)
					) {
						myVar.amountOutProfit = getAmountOut(
							myVar.amountOutInter,
							sellReserve1,
							sellReserve0,
							arbs[i].sellToFee
						);
					} else {
						myVar.amountOutProfit = getAmountOut(
							myVar.amountOutInter,
							sellReserve0,
							sellReserve1,
							arbs[i].sellToFee
						);
					}

					myVar.opportunityPresent = myVar.amountOutProfit > (arbs[i].nativeInAmount / k + minProfit);

					if (myVar.opportunityPresent) {
						myVar.denom = k;
						break;
					}
				}
			}

			// require(buyReserve0 == buyReserve[0], "BR0");
			// require(buyReserve1 == buyReserve[1], "BR1");
			// require(sellReserve0 == sellReserve[0], "SR0");
			// require(sellReserve1 == sellReserve[1], "SR1");

			if (myVar.opportunityPresent) {
				// Pack calldata
				bytes memory data = abi.encode(
					arbs[i].buyFromPair,
					arbs[i].nativeInAmount / myVar.denom,
					myVar.amountOutInter,
					myVar.amountOutProfit,
					arbs[i].sellToPair,
					arbs[i].buyFromIsWMetis,
					arbs[i].sellToIsWMetis
				);

				// Identify the tokens
				address token0 = IUniswapV2PairV1(arbs[i].sellToPair).token0();

				// Set the amounts
				// We only work with NATIVE_TOKEN pairs
				uint256 amount0Out = token0 == NATIVE_TOKEN || token0 == address(WMETIS) ? myVar.amountOutProfit : 0;
				uint256 amount1Out = token0 == NATIVE_TOKEN || token0 == address(WMETIS) ? 0 : myVar.amountOutProfit;

				// Call swap with calldata
				IUniswapV2PairV1(arbs[i].sellToPair).swap(amount0Out, amount1Out, address(this), data);

				// We got opportunity!
				gotOpportunity = gotOpportunity || true;
			}
		}

		// Transfer profits
		if (gotOpportunity) {
			IERC20 nativeToken = IERC20(NATIVE_TOKEN);
			uint256 balance = nativeToken.balanceOf(address(this));
			// require(balance > 0, "NO PROFIT");
			bool success = nativeToken.transfer(msg.sender, balance);
			require(success, "PROFIT TRANSFER FAILED");
		}
	}

	function getAmountOut(
		uint256 amountIn,
		uint256 reserveIn,
		uint256 reserveOut,
		uint256 feePerTenThousands
	) internal pure returns (uint256 amountOut) {
		uint256 fee = 10000 - feePerTenThousands;
		uint256 amountInWithFee = amountIn * fee;
		uint256 numerator = amountInWithFee * reserveOut;
		uint256 denominator = (reserveIn * 10000) + amountInWithFee;
		amountOut = numerator / denominator;
	}

	function _baseHook(address, uint256, uint256, bytes calldata data) internal {
		// Decode parameters
		(
			address buyFromPair,
			uint112 nativeInAmount,
			uint112 tokenAmount,
			uint112 nativeOutAmount,
			address sellToPair,
			bool buyFromIsWMetis,
			bool sellToIsWMetis
		) = abi.decode(data, (address, uint112, uint112, uint112, address, bool, bool));

		// Check that our caller is the buyFromPair
		// require(msg.sender == sellToPair, "CALLER NOT PAIR");

		// Check that we got correct amount
		{
			// IUniswapV2PairV1 sellPair = IUniswapV2PairV1(sellToPair);
			// address token0In = sellPair.token0();
			// uint256 amountNativeReceived = token0In == NATIVE_TOKEN ? amount0 : amount1;

			// require(amountNativeReceived >= nativeOutAmount, "RECEIVED NOT ENOUGH");

			// Transfer the native to buyFromPair
			if (sellToIsWMetis) {
				WMETIS.withdraw(nativeOutAmount);
			}

			if (buyFromIsWMetis) {
				WMETIS.deposit{value: nativeInAmount}();
				WMETIS.transfer(buyFromPair, nativeInAmount);
			} else {
				IERC20(NATIVE_TOKEN).transfer(buyFromPair, nativeInAmount);
			}
		}

		// Execute swap on buyFromPair with empty data to send tokens to sellToPair
		IUniswapV2PairV1 buyPair = IUniswapV2PairV1(buyFromPair);
		address token0Out = buyPair.token0();
		uint256 amount0Out = token0Out == NATIVE_TOKEN || token0Out == address(WMETIS) ? 0 : tokenAmount;
		uint256 amount1Out = token0Out == NATIVE_TOKEN || token0Out == address(WMETIS) ? tokenAmount : 0;

		buyPair.swap(amount0Out, amount1Out, sellToPair, "");
	}

	function uniswapV2Call(address sender, uint256 amount0, uint256 amount1, bytes calldata data) external {
		_baseHook(sender, amount0, amount1, data);
	}

	function hook(address sender, uint256 amount0, uint256 amount1, bytes calldata data) external {
		_baseHook(sender, amount0, amount1, data);
	}

	function netswapCall(address sender, uint256 amount0, uint256 amount1, bytes calldata data) external {
		_baseHook(sender, amount0, amount1, data);
	}

	function miniMeCall(address sender, uint256 amount0, uint256 amount1, bytes calldata data) external {
		_baseHook(sender, amount0, amount1, data);
	}
}
