// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";

// NodeTypes library remains unchanged
library NodeTypes {
    enum NodeRole {
        DESIGNATOR,
        VALIDATOR,
        EXECUTOR
    }

    struct Node {
        address nodeAddress;
        NodeRole role;
        uint256 capacity;
        uint256 performance;
        bool isActive;
        bytes32 clusterId;
    }

    struct Cluster {
        bytes32 id;
        address leader;
        uint256 formationTime;
        uint256 nodeCount;
        uint256 processedTxCount;
        bytes32 sessionKey;
        bool isActive;
    }
}

contract ClusterManager is AccessControl {
    using NodeTypes for NodeTypes.Node;
    using NodeTypes for NodeTypes.Cluster;

    bytes32 public constant CLUSTER_ADMIN = keccak256("CLUSTER_ADMIN");
    
    mapping(address => NodeTypes.Node) public nodes;
    mapping(bytes32 => NodeTypes.Cluster) public clusters;
    mapping(bytes32 => mapping(address => bool)) public clusterMembers;
    
    uint256 public constant MIN_CLUSTER_SIZE = 3;
    uint256 public constant MAX_CLUSTER_SIZE = 10;
    uint256 public constant VALIDATION_THRESHOLD = 70;
    
    event ClusterFormed(bytes32 indexed clusterId, address leader);
    event NodeAssigned(address indexed node, bytes32 indexed clusterId);
    event SessionKeyGenerated(bytes32 indexed clusterId, bytes32 sessionKey);
    
    constructor() {
        _setupRole(DEFAULT_ADMIN_ROLE, msg.sender);
    }
    
    function generateSessionKey(
        bytes32 clusterId
    ) internal returns (bytes32) {
        NodeTypes.Cluster storage cluster = clusters[clusterId];
        require(cluster.isActive, "Cluster not active");
        
        bytes32 randomValue = keccak256(abi.encodePacked(
            block.timestamp,
            block.number,
            blockhash(block.number - 1), // Use blockhash as a source of randomness
            clusterId,
            msg.sender
        ));
        
        cluster.sessionKey = randomValue;
        emit SessionKeyGenerated(clusterId, randomValue);
        
        return randomValue;
    } // Added closing brace here
}

contract BeeRoutingAlgorithm is ClusterManager {
    struct Route {
        bytes32[] clusterPath;
        uint256 pathScore;
        bool isOptimal;
    }
    
    mapping(bytes32 => Route) public routes;
    
    // Routing parameters
    uint256 private constant CAPACITY_WEIGHT = 40;
    uint256 private constant PERFORMANCE_WEIGHT = 60;
    uint256 private constant SCORE_THRESHOLD = 75;
    
    function findOptimalRoute(
        bytes32 txId,
        uint256 txSize,
        NodeTypes.NodeRole[] memory requiredRoles
    ) public returns (bytes32[] memory) {
        Route storage route = routes[txId];
        
        bytes32[] memory eligibleClusters = findEligibleClusters(
            txSize,
            requiredRoles
        );
        
        (bytes32[] memory optimalPath, uint256 pathScore) = calculateOptimalPath(
            eligibleClusters,
            txSize
        );
        
        route.clusterPath = optimalPath;
        route.pathScore = pathScore;
        route.isOptimal = true;
        
        return optimalPath;
    }
    
   function findEligibleClusters(
    uint256 txSize,
    NodeTypes.NodeRole[] memory requiredRoles
) internal view returns (bytes32[] memory) {
    uint256 clusterCount = 0;
    bytes32[] memory tempClusters = new bytes32[](10); // Temporary fixed size array
    
    // Iterate through clusters using a counter approach
    for (uint256 i = 0; i < 10; i++) { // Assuming max 10 clusters, adjust as needed
        bytes32 clusterId = bytes32(i); // Convert index to bytes32
        
        if (clusters[clusterId].id == 0) break; // Stop if cluster doesn't exist
        
        NodeTypes.Cluster storage cluster = clusters[clusterId];
        if (!cluster.isActive) continue;
        
        // Check if cluster has required roles
        bool hasAllRoles = true;
        for (uint j = 0; j < requiredRoles.length; j++) {
            bool roleFound = false;
            
            // Iterate through possible node addresses 
            // (you'll need to track these separately)
            address[] memory clusterNodeAddresses = getClusterNodeAddresses(clusterId);
            
            for (uint k = 0; k < clusterNodeAddresses.length; k++) {
                NodeTypes.Node storage node = nodes[clusterNodeAddresses[k]];
                if (node.role == requiredRoles[j] && node.isActive) {
                    roleFound = true;
                    break;
                }
            }
            
            if (!roleFound) {
                hasAllRoles = false;
                break;
            }
        }
        
        // Check capacity
        if (hasAllRoles && cluster.nodeCount >= txSize) {
            tempClusters[clusterCount] = clusterId;
            clusterCount++;
        }
    }
    
    // Create correctly sized array
    bytes32[] memory eligibleClusters = new bytes32[](clusterCount);
    for (uint256 k = 0; k < clusterCount; k++) {
        eligibleClusters[k] = tempClusters[k];
    }
    
    return eligibleClusters;
}

// Helper function to get node addresses for a cluster
function getClusterNodeAddresses(bytes32 clusterId) internal view returns (address[] memory) {
    uint256 nodeCount = 0;
    address[] memory tempAddresses = new address[](10); // Assuming max 10 nodes per cluster
    
    for (address nodeAddr = address(0); nodeAddr < address(type(uint160).max); nodeAddr = address(uint160(nodeAddr) + 1)) {
        if (clusterMembers[clusterId][nodeAddr]) {
            tempAddresses[nodeCount] = nodeAddr;
            nodeCount++;
            
            if (nodeCount >= 10) break; // Prevent potential infinite loop
        }
    }
    
    // Create correctly sized array
    address[] memory clusterNodeAddresses = new address[](nodeCount);
    for (uint256 i = 0; i < nodeCount; i++) {
        clusterNodeAddresses[i] = tempAddresses[i];
    }
    
    return clusterNodeAddresses;
}
    
function calculateOptimalPath(
    bytes32[] memory eligibleClusters,
    uint256 /* txSize */
) internal view returns (bytes32[] memory, uint256) {
    if (eligibleClusters.length == 0) {
        return (new bytes32[](0), 0);
    }
    
    bytes32[] memory optimalPath = new bytes32[](1);
    uint256 highestScore = 0;
    
    for (uint256 i = 0; i < eligibleClusters.length; i++) {
        NodeTypes.Cluster storage cluster = clusters[eligibleClusters[i]];
        
        // Calculate weighted score based on node count and processed transactions
        uint256 nodeCountScore = (cluster.nodeCount * CAPACITY_WEIGHT) / 100;
        uint256 performanceScore = (cluster.processedTxCount * PERFORMANCE_WEIGHT) / 100;
        uint256 totalScore = nodeCountScore + performanceScore;
        
        if (totalScore > highestScore) {
            highestScore = totalScore;
            optimalPath[0] = eligibleClusters[i];
        }
    }
    
    return (optimalPath, highestScore);
}
}
/**
 * @title PasschainValidator
 * @dev Manages PoA validation process for transactions
 */
contract PasschainValidator is BeeRoutingAlgorithm, ReentrancyGuard {
    struct ValidationProcess {
        bytes32 txId;
        bytes32 clusterId;
        uint256 validationsRequired;
        uint256 validationsReceived;
        uint256 validationsPositive;
        bool isComplete;
        mapping(address => bool) hasValidated;
    }
    
    mapping(bytes32 => ValidationProcess) public validationProcesses;
    
    event ValidationStarted(bytes32 indexed txId, bytes32 indexed clusterId);
    event ValidationSubmitted(bytes32 indexed txId, address validator, bool result);
    event ValidationComplete(bytes32 indexed txId, bool isValid);
    
    function initiateValidation(
        bytes32 txId,
        bytes memory txData
    ) external nonReentrant {
        require(!validationProcesses[txId].isComplete, "Already validated");
        
        // Find optimal cluster route
        NodeTypes.NodeRole[] memory roles = new NodeTypes.NodeRole[](3);
        roles[0] = NodeTypes.NodeRole.DESIGNATOR;
        roles[1] = NodeTypes.NodeRole.VALIDATOR;
        roles[2] = NodeTypes.NodeRole.EXECUTOR;
        
        bytes32[] memory routePath = this.findOptimalRoute(
            txId,
            txData.length,
            roles
        );
        require(routePath.length > 0, "No valid route found");
        
        // Setup validation process
        ValidationProcess storage process = validationProcesses[txId];
        process.txId = txId;
        process.clusterId = routePath[0];
        process.validationsRequired = calculateRequiredValidations(routePath[0]);
        
        emit ValidationStarted(txId, routePath[0]);
    }
    
    function submitValidation(
        bytes32 txId,
        bool isValid
    ) external nonReentrant {
        ValidationProcess storage process = validationProcesses[txId];
        require(!process.isComplete, "Validation complete");
        require(!process.hasValidated[msg.sender], "Already validated");
        require(
            clusterMembers[process.clusterId][msg.sender],
            "Not cluster member"
        );
        
        process.hasValidated[msg.sender] = true;
        process.validationsReceived++;
        if (isValid) {
            process.validationsPositive++;
        }
        
        emit ValidationSubmitted(txId, msg.sender, isValid);
        
        // Check if validation is complete
        if (process.validationsReceived >= process.validationsRequired) {
            bool validationResult = (
                process.validationsPositive * 100 / process.validationsReceived
            ) >= VALIDATION_THRESHOLD;
            
            process.isComplete = true;
            emit ValidationComplete(txId, validationResult);
        }
    }
    
    function calculateRequiredValidations(
        bytes32 clusterId
    ) internal view returns (uint256) {
        NodeTypes.Cluster storage cluster = clusters[clusterId];
        return (cluster.nodeCount * 2) / 3; // Require 2/3 majority
    }
}

/**
 * @title ClusterCommunication
 * @dev Handles secure communication within and between clusters
 */
contract ClusterCommunication is PasschainValidator {
    struct SecureMessage {
        bytes32 messageId;
        bytes encryptedContent;
        bytes mac;
        address sender;
        address recipient;
        uint256 timestamp;
    }
    
    mapping(bytes32 => SecureMessage) public messages;
    
    event MessageSent(bytes32 indexed messageId, address indexed recipient);
    
    function sendClusterMessage(
        bytes32 clusterId,
        address recipient,
        bytes memory content
    ) external nonReentrant {
        require(
            clusterMembers[clusterId][msg.sender],
            "Not in cluster"
        );
        
        bytes32 messageId = keccak256(abi.encodePacked(
            clusterId,
            msg.sender,
            recipient,
            block.timestamp
        ));
        
        // AES-GCM encryption would be implemented here
        bytes memory encrypted = content; // Placeholder
        
        // HMAC generation
        bytes memory mac = abi.encodePacked(
            keccak256(abi.encodePacked(content, clusters[clusterId].sessionKey))
        );
        
        messages[messageId] = SecureMessage({
            messageId: messageId,
            encryptedContent: encrypted,
            mac: mac,
            sender: msg.sender,
            recipient: recipient,
            timestamp: block.timestamp
        });
        
        emit MessageSent(messageId, recipient);
    }
}