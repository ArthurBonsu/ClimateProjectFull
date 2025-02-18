// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

contract RewardToken is ERC20, Ownable {
    constructor() ERC20("PasschainReward", "PSCR") {
        // Mint 1 million tokens to the contract deployer
        _mint(msg.sender, 1_000_000 * 10**18);
    }

    // Optional: Add a function to mint more tokens if needed
    function mint(address to, uint256 amount) public onlyOwner {
        _mint(to, amount);
    }
}