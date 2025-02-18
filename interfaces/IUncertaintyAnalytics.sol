// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

interface IUncertaintyAnalytics {
    function submitRequest() external payable returns (uint256);
    function submitResponse(uint256 _requestId) external;
    function updateDisruptionLevel(uint256 _level) external;
    function updateEscalationLevel(uint256 _level) external;
    function calculateUnavailabilityCost(uint256 _requestId) external;
    function updateDataHoldingCost(uint256 _cost) external;
}