// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;
import "../interfaces/IUncertaintyAnalytics.sol";

contract RequestManager {
    IUncertaintyAnalytics private immutable analytics;
    
    constructor(address _analytics) {
        require(_analytics != address(0), "Analytics address cannot be zero");
        require(_analytics.code.length > 0, "Analytics must be a contract");
        
        // Initialize the contract
        analytics = IUncertaintyAnalytics(_analytics);
    }
    
    function submitRequest() external payable returns (uint256) {
        return analytics.submitRequest{value: msg.value}();
    }
}