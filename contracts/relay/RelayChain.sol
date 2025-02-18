// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";

/**
 * @title MerkleTreeLib
 * @dev Library for Merkle tree operations
 */
library MerkleTreeLib {
    struct MerkleTree {
        bytes32 root;
        mapping(bytes32 => bool) verified;
    }

    function verifyProof(
        bytes32 root,
        bytes32 leaf,
        bytes32[] memory proof
    ) internal pure returns (bool) {
        bytes32 computedHash = leaf;
        
        for (uint256 i = 0; i < proof.length; i++) {
            computedHash = hashPair(computedHash, proof[i]);
        }
        
        return computedHash == root;
    }
    
    function hashPair(bytes32 a, bytes32 b) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(a, b));
    }
}

/**
 * @title RelayTypes
 * @dev Core data structures for relay chain operations
 */
library RelayTypes {
    struct Transaction {
        bytes32 id;
        address sourceBridge;
        address targetBridge;
        bytes32 originalTxHash;
        bytes metadata;
        bytes32 merkleRoot;
        uint256 timestamp;
        bool isValidated;
        bool isStateUpdated;
        bool isRelayed;
    }

    struct StateUpdate {
        bytes32 txId;
        bytes32 merkleRoot;
        bytes32[] affectedChains;
        mapping(bytes32 => bool) notificationStatus;
        uint256 updateTime;
        bool isComplete;
    }

    struct ValidationState {
        uint256 totalStake;
        uint256 approvalStake;
        uint256 startTime;
        bool isComplete;
        mapping(address => uint256) validatorStakes;
    }
}

/**
 * @title ProofOfStakeValidator
 * @dev Manages PoS validation for relay chain
 */
contract ProofOfStakeValidator is AccessControl {
    using RelayTypes for RelayTypes.Transaction;
    using RelayTypes for RelayTypes.ValidationState;
    using MerkleTreeLib for MerkleTreeLib.MerkleTree;

    bytes32 public constant VALIDATOR_ROLE = keccak256("VALIDATOR_ROLE");
    
    mapping(bytes32 => RelayTypes.ValidationState) public validationStates;
    mapping(address => uint256) public validatorStakes;
    
    uint256 public constant MIN_STAKE = 1000 ether;
    uint256 public constant VALIDATION_THRESHOLD = 70; // 70% stake approval needed
    
    event ValidationSubmitted(bytes32 indexed txId, address validator, uint256 stake);
    event ValidationComplete(bytes32 indexed txId, bool isValid);
    
    constructor() {
        _setupRole(DEFAULT_ADMIN_ROLE, msg.sender);
    }
    
    function submitValidation(
        bytes32 txId,
        bool approval
    ) external {
        require(hasRole(VALIDATOR_ROLE, msg.sender), "Not a validator");
        
        RelayTypes.ValidationState storage validationState = validationStates[txId];
        require(!validationState.isComplete, "Validation complete");
        
        uint256 stake = validatorStakes[msg.sender];
        require(stake >= MIN_STAKE, "Insufficient stake");
        
        if (approval) {
            validationState.approvalStake += stake;
        }
        validationState.totalStake += stake;
        validationState.validatorStakes[msg.sender] = stake;
        
        emit ValidationSubmitted(txId, msg.sender, stake);
        
        // Check if validation threshold is met
        if (validationState.totalStake > 0) {
            uint256 approvalPercentage = (validationState.approvalStake * 100) / 
                                        validationState.totalStake;
            
            if (approvalPercentage >= VALIDATION_THRESHOLD) {
                validationState.isComplete = true;
                emit ValidationComplete(txId, true);
            }
        }
    }
}

/**
 * @title StateManager
 * @dev Handles state updates and Merkle tree management
 */
contract StateManager is ProofOfStakeValidator {
    using MerkleTreeLib for MerkleTreeLib.MerkleTree;
    
    mapping(bytes32 => RelayTypes.StateUpdate) public stateUpdates;
    mapping(bytes32 => bytes32) public chainStates;
    
    event StateUpdateInitiated(bytes32 indexed txId, bytes32 merkleRoot);
    event ChainNotified(bytes32 indexed chainId, bytes32 indexed txId);
    event StateUpdateComplete(bytes32 indexed txId);
    
    function updateState(
        bytes32 txId,
        bytes32[] memory affectedChains,
        bytes memory txData
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        RelayTypes.StateUpdate storage update = stateUpdates[txId];
        require(!update.isComplete, "State update complete");
        
        bytes32 merkleRoot = createMerkleRoot(txData);
        update.merkleRoot = merkleRoot;
        update.affectedChains = affectedChains;
        update.updateTime = block.timestamp;
        
        emit StateUpdateInitiated(txId, merkleRoot);
        
        for (uint256 i = 0; i < affectedChains.length; i++) {
            notifyChain(affectedChains[i], txId, merkleRoot);
        }
    }
    
    function notifyChain(
        bytes32 chainId,
        bytes32 txId,
        bytes32 merkleRoot
    ) internal {
        chainStates[chainId] = merkleRoot;
        RelayTypes.StateUpdate storage update = stateUpdates[txId];
        update.notificationStatus[chainId] = true;
        
        emit ChainNotified(chainId, txId);
    }
    
    function createMerkleRoot(
        bytes memory data
    ) internal pure returns (bytes32) {
        return keccak256(data);
    }
}

/**
 * @title TransactionRelay
 * @dev Manages final transaction relay to receiving blockchain
 */
contract TransactionRelay is StateManager, ReentrancyGuard {
    struct RelayedTransaction {
        bytes32 txId;
        bytes originalMetadata;
        bytes enrichedData;
        uint256 relayTime;
        bool isComplete;
    }
    
    mapping(bytes32 => RelayedTransaction) public relayedTransactions;
    
    event TransactionRelayed(
        bytes32 indexed txId,
        address indexed targetBridge,
        bytes32 merkleRoot
    );
    
    function relayTransaction(
        bytes32 txId,
        address targetBridge,
        bytes memory originalMetadata,
        bytes memory enrichedData
    ) external nonReentrant onlyRole(DEFAULT_ADMIN_ROLE) {
        require(
            validationStates[txId].isComplete,
            "Validation incomplete"
        );
        require(
            stateUpdates[txId].isComplete,
            "State update incomplete"
        );
        
        RelayedTransaction storage relayedTx = relayedTransactions[txId];
        require(!relayedTx.isComplete, "Already relayed");
        
        relayedTx.txId = txId;
        relayedTx.originalMetadata = originalMetadata;
        relayedTx.enrichedData = enrichedData;
        relayedTx.relayTime = block.timestamp;
        relayedTx.isComplete = true;
        
        emit TransactionRelayed(
            txId,
            targetBridge,
            stateUpdates[txId].merkleRoot
        );
    }
}

/**
 * @title ReceivingBlockchainInterface
 * @dev Interface for receiving blockchain to process relayed transactions
 */
contract ReceivingBlockchainInterface is ReentrancyGuard {
    struct ReceivedTransaction {
        bytes32 txId;
        bytes32 merkleRoot;
        bytes metadata;
        bytes enrichedData;
        uint256 receiveTime;
        bool isProcessed;
    }
    
    mapping(bytes32 => ReceivedTransaction) public receivedTransactions;
    
    event TransactionReceived(bytes32 indexed txId, bytes32 merkleRoot);
    event TransactionProcessed(bytes32 indexed txId);
    
    function receiveTransaction(
        bytes32 txId,
        bytes32 merkleRoot,
        bytes memory metadata,
        bytes memory enrichedData
    ) external nonReentrant {
        ReceivedTransaction storage receivedTx = receivedTransactions[txId];
        require(!receivedTx.isProcessed, "Already processed");
        
        receivedTx.txId = txId;
        receivedTx.merkleRoot = merkleRoot;
        receivedTx.metadata = metadata;
        receivedTx.enrichedData = enrichedData;
        receivedTx.receiveTime = block.timestamp;
        
        emit TransactionReceived(txId, merkleRoot);
        
        processTransaction(txId);
    }
    
    function processTransaction(bytes32 txId) internal {
        ReceivedTransaction storage receivedTx = receivedTransactions[txId];
        receivedTx.isProcessed = true;
        
        emit TransactionProcessed(txId);
    }
}