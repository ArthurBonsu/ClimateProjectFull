// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract UncertaintyAnalytics {
    // State variables
    address public owner;
    bool private locked;
    
    struct Request {
        uint256 id;
        address requester;
        uint256 timestamp;
        uint256 confirmationTime;
        uint256 executionTime;
        uint256 cost;
        bool isValid;
        bool isProcessed;
        Status status;
    }
    
    struct Response {
        uint256 requestId;
        address responder;
        uint256 timestamp;
        uint256 processingTime;
        uint256 cost;
        bool isValid;
        Status status;
    }
    
    enum Status { Pending, Processing, Completed, Failed }
    
    uint256 public requestCount;
    uint256 public responseCount;
    uint256 public totalTransactionCost;
    uint256 public failedTransactionCount;
    uint256 public dataHoldingCost;
    uint256 public unavailabilityCost;
    uint256 public disruptionLevel;
    uint256 public escalationLevel;
    
    mapping(uint256 => Request) public requests;
    mapping(uint256 => Response) public responses;
    mapping(address => uint256) public requesterStats;
    mapping(address => uint256) public responderStats;
    
    uint256 public constant MAX_PROCESSING_TIME = 1 days;
    uint256 public constant BASE_COST = 0.001 ether;
    
    event RequestSubmitted(uint256 indexed requestId, address requester);
    event ResponseReceived(uint256 indexed requestId, address responder);
    event TransactionFailed(uint256 indexed requestId, string reason);
    event CostRecorded(uint256 requestId, uint256 cost, string costType);
    
    modifier onlyOwner() {
        require(msg.sender == owner, "Not owner");
        _;
    }
    
    modifier noReentrant() {
        require(!locked, "No reentrancy");
        locked = true;
        _;
        locked = false;
    }
    
    constructor() {
        owner = msg.sender;
        locked = false;
        requestCount = 0;
        responseCount = 0;
        totalTransactionCost = 0;
        failedTransactionCount = 0;
        dataHoldingCost = 0;
        unavailabilityCost = 0;
        disruptionLevel = 0;
        escalationLevel = 0;
    }
    
    function submitRequest() external payable noReentrant returns (uint256) {
        require(msg.value >= BASE_COST, "Insufficient payment");
        
        unchecked {
            requestCount++;
        }
        
        requests[requestCount] = Request({
            id: requestCount,
            requester: msg.sender,
            timestamp: block.timestamp,
            confirmationTime: 0,
            executionTime: 0,
            cost: msg.value,
            isValid: true,
            isProcessed: false,
            status: Status.Pending
        });
        
        totalTransactionCost += msg.value;
        requesterStats[msg.sender]++;
        
        emit RequestSubmitted(requestCount, msg.sender);
        return requestCount;
    }
    
    function submitResponse(uint256 _requestId) external noReentrant {
        require(_requestId <= requestCount, "Invalid request ID");
        require(requests[_requestId].isValid, "Request not valid");
        
        unchecked {
            responseCount++;
        }
        
        responses[_requestId] = Response({
            requestId: _requestId,
            responder: msg.sender,
            timestamp: block.timestamp,
            processingTime: block.timestamp - requests[_requestId].timestamp,
            cost: 0,
            isValid: true,
            status: Status.Completed
        });
        
        responderStats[msg.sender]++;
        
        emit ResponseReceived(_requestId, msg.sender);
    }
    
    function calculateUnavailabilityCost(uint256 _requestId) external {
        Request storage request = requests[_requestId];
        require(request.isValid, "Invalid request");
        
        uint256 processingTime = block.timestamp - request.timestamp;
        if (processingTime > MAX_PROCESSING_TIME) {
            uint256 penalty = (processingTime - MAX_PROCESSING_TIME) * BASE_COST / 86400;
            unavailabilityCost += penalty;
        }
    }
    
    function recordFailedTransaction(uint256 _requestId, string calldata _reason) external onlyOwner {
        Request storage request = requests[_requestId];
        request.status = Status.Failed;
        failedTransactionCount++;
        
        emit TransactionFailed(_requestId, _reason);
    }
    
    function updateDataHoldingCost(uint256 _cost) external onlyOwner {
        dataHoldingCost += _cost;
        emit CostRecorded(0, _cost, "DataHolding");
    }
    
    function getMetrics() external view returns (
        uint256 avgProcessingTime,
        uint256 successRate,
        uint256 totalCost,
        uint256 disruptionCount
    ) {
        if (responseCount > 0) {
            uint256 totalTime = 0;
            for (uint256 i = 1; i <= responseCount; i++) {
                totalTime += responses[i].processingTime;
            }
            avgProcessingTime = totalTime / responseCount;
        }
        
        if (requestCount > 0) {
            successRate = ((requestCount - failedTransactionCount) * 100) / requestCount;
        }
        
        totalCost = totalTransactionCost;
        disruptionCount = failedTransactionCount;
    }
    
    receive() external payable {}

    // Add these functions in UncertaintyAnalytics.sol
function updateDisruptionLevel(uint256 _level) external onlyOwner {
    disruptionLevel = _level;
}

function updateEscalationLevel(uint256 _level) external onlyOwner {
    escalationLevel = _level;
}
    
    function withdraw() external onlyOwner {
        (bool success, ) = payable(owner).call{value: address(this).balance}("");
        require(success, "Withdrawal failed");
    }
}