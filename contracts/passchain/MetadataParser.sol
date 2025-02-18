// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Counters.sol";
import "../blockchain/ChaCha20Poly1305.sol";

/**
 * @title MetadataParser
 * @dev Enhanced version with actual encryption and detailed protocol conversion
 */
contract MetadataParser is ReentrancyGuard {
    using Counters for Counters.Counter;
    
    ChaCha20Poly1305 public immutable encryptionHandler;
    
    struct ProtocolInstruction {
        bytes32 originalProtocol;
        bytes32 convertedProtocol;
        mapping(bytes32 => bytes32) opcodeMapping;
    }

    struct ParsedMetadata {
        bytes32 transactionId;
        address sender;
        address recipient;
        uint256 timestamp;
        bytes32 sourceProtocol;
        bytes32 targetProtocol;
        bytes encryptedPayload;
        bytes authTag;
        bool isSpeculative;
        TransactionStatus status;
    }

    struct IRNode {
        bytes32 nodeType;
        bytes value;
        bytes32[] childrenIds;
        mapping(bytes32 => bytes32) keyValuePairs;
    }

    enum TransactionStatus {
        Pending,
        Speculative,
        Confirmed,
        Failed
    }

    mapping(bytes32 => ParsedMetadata) public parsedTransactions;
    mapping(bytes32 => ProtocolInstruction) public protocolInstructions;
    mapping(bytes32 => IRNode) public irNodes;
    Counters.Counter private _transactionIdCounter;

    event MetadataParsed(bytes32 indexed transactionId, bool isSpeculative);
    event ProtocolConverted(bytes32 indexed originalProtocol, bytes32 indexed convertedProtocol);
    event TransactionStatusUpdated(bytes32 indexed transactionId, TransactionStatus status);

    constructor() {
        encryptionHandler = new ChaCha20Poly1305();
    }

    function generateEncryptionParams(
        bytes32 transactionId
    ) internal pure returns (bytes32 key, bytes12 nonce) {
        key = keccak256(abi.encodePacked("KEY", transactionId));
        nonce = bytes12(uint96(uint256(keccak256(abi.encodePacked("NONCE", transactionId)))));
        return (key, nonce);
    }

    function prepareTransactionMetadata(
        address _recipient,
        bytes32 _sourceProtocol,
        bytes32 _targetProtocol,
        bytes calldata _payload,
        bool _isSpeculative
    ) external nonReentrant returns (bytes32) {
        bytes32 transactionId = generateTransactionId();
        (bytes32 key, bytes12 nonce) = generateEncryptionParams(transactionId);
        
        ParsedMetadata storage metadata = parsedTransactions[transactionId];
        metadata.transactionId = transactionId;
        metadata.sender = msg.sender;
        metadata.recipient = _recipient;
        metadata.timestamp = block.timestamp;
        metadata.sourceProtocol = _sourceProtocol;
        metadata.targetProtocol = _targetProtocol;
        metadata.isSpeculative = _isSpeculative;
        metadata.status = TransactionStatus.Pending;

        (bytes memory encryptedPayload, bytes memory authTag) = encryptPayload(_payload, key, nonce);
        metadata.encryptedPayload = encryptedPayload;
        metadata.authTag = authTag;

        emit MetadataParsed(transactionId, _isSpeculative);
        return transactionId;
    }

    function parseToIRNode(
        bytes32 _nodeId,
        bytes32 _nodeType,
        bytes calldata _value
    ) external returns (bytes32) {
        IRNode storage node = irNodes[_nodeId];
        node.nodeType = _nodeType;
        node.value = _value;
        return _nodeId;
    }

    function addKeyValueToNode(
        bytes32 _nodeId,
        bytes32 _key,
        bytes32 _value
    ) external {
        IRNode storage node = irNodes[_nodeId];
        node.keyValuePairs[_key] = _value;
    }

    function convertProtocolInstructions(
        bytes32 _originalProtocol,
        bytes32 _targetProtocol,
        bytes32[] calldata _opcodes,
        bytes32[] calldata _convertedOpcodes
    ) external {
        require(_opcodes.length == _convertedOpcodes.length, "Array length mismatch");
        
        ProtocolInstruction storage instruction = protocolInstructions[_originalProtocol];
        instruction.originalProtocol = _originalProtocol;
        instruction.convertedProtocol = _targetProtocol;

        for (uint i = 0; i < _opcodes.length; i++) {
            instruction.opcodeMapping[_opcodes[i]] = _convertedOpcodes[i];
        }

        emit ProtocolConverted(_originalProtocol, _targetProtocol);
    }

    function updateTransactionStatus(
        bytes32 _transactionId,
        TransactionStatus _status
    ) external {
        require(parsedTransactions[_transactionId].sender != address(0), "Transaction not found");
        parsedTransactions[_transactionId].status = _status;
        emit TransactionStatusUpdated(_transactionId, _status);
    }

    function generateTransactionId() private returns (bytes32) {
        _transactionIdCounter.increment();
        return keccak256(abi.encodePacked(block.timestamp, msg.sender, _transactionIdCounter.current()));
    }

    function encryptPayload(
        bytes calldata _payload,
        bytes32 _key,
        bytes12 _nonce
    ) public view returns (bytes memory encryptedPayload, bytes memory authTag) {
        return encryptionHandler.encrypt(_payload, _key, _nonce);
    }
}