// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;
import "../interfaces/IUncertaintyAnalytics.sol";

contract ResponseManager {
    IUncertaintyAnalytics private immutable analytics;
    
    constructor(address _analytics) {
        require(_analytics != address(0), "Analytics address cannot be zero");
        require(_analytics.code.length > 0, "Analytics must be a contract");
        analytics = IUncertaintyAnalytics(_analytics);
    }
    
    function submitResponse(uint256 _requestId) external {
        analytics.submitResponse(_requestId);
    }
}