// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Counters.sol";

/**
 * @title ChaCha20Poly1305
 * @dev Implementation of ChaCha20-Poly1305 AEAD
 */
contract ChaCha20Poly1305 {
    uint256 constant CHACHA20_ROUNDS = 20;
    
    struct ChaChaState {
        uint32[16] state;  // 4x4 state matrix
    }

    struct Poly1305State {
        uint256 r;  // Key part for Poly1305
        uint256 s;  // Accumulator
    }

    /**
     * @dev Initialize ChaCha20 state
     */
    function initializeState(
        bytes32 key,
        bytes12 nonce,
        uint32 counter
    ) internal pure returns (ChaChaState memory) {
        ChaChaState memory state;
        
        // Constants for ChaCha20
        state.state[0] = 0x61707865; // "expa"
        state.state[1] = 0x3320646e; // "nd 3"
        state.state[2] = 0x79622d32; // "2-by"
        state.state[3] = 0x6b206574; // "te k"

        // Key setup (8 words)
        for (uint i = 0; i < 8; i++) {
            state.state[4 + i] = uint32(uint256(key) >> (i * 32));
        }

        // Counter
        state.state[12] = counter;

        // Nonce (3 words)
        bytes memory nonceBytes = abi.encodePacked(nonce);
        for (uint i = 0; i < 3; i++) {
            state.state[13 + i] = uint32(uint8(nonceBytes[i * 4])) |
                                 (uint32(uint8(nonceBytes[i * 4 + 1])) << 8) |
                                 (uint32(uint8(nonceBytes[i * 4 + 2])) << 16) |
                                 (uint32(uint8(nonceBytes[i * 4 + 3])) << 24);
        }

        return state;
    }

    /**
     * @dev ChaCha20 Quarter Round
     */
    function quarterRound(
        uint32 a,
        uint32 b,
        uint32 c,
        uint32 d
    ) internal pure returns (uint32, uint32, uint32, uint32) {
        a = a + b;
        d = rotl32(d ^ a, 16);
        
        c = c + d;
        b = rotl32(b ^ c, 12);
        
        a = a + b;
        d = rotl32(d ^ a, 8);
        
        c = c + d;
        b = rotl32(b ^ c, 7);
        
        return (a, b, c, d);
    }

    /**
     * @dev 32-bit rotation
     */
    function rotl32(uint32 x, uint32 n) internal pure returns (uint32) {
        return (x << n) | (x >> (32 - n));
    }

    /**
     * @dev ChaCha20 block function
     */
    function chacha20Block(ChaChaState memory state) internal pure returns (bytes memory) {
        uint32[16] memory working;
        for (uint i = 0; i < 16; i++) {
            working[i] = state.state[i];
        }

        // 10 double-rounds
        for (uint i = 0; i < CHACHA20_ROUNDS; i += 2) {
            // Column rounds
            (working[0], working[4], working[8], working[12]) = 
                quarterRound(working[0], working[4], working[8], working[12]);
            (working[1], working[5], working[9], working[13]) = 
                quarterRound(working[1], working[5], working[9], working[13]);
            (working[2], working[6], working[10], working[14]) = 
                quarterRound(working[2], working[6], working[10], working[14]);
            (working[3], working[7], working[11], working[15]) = 
                quarterRound(working[3], working[7], working[11], working[15]);

            // Diagonal rounds
            (working[0], working[5], working[10], working[15]) = 
                quarterRound(working[0], working[5], working[10], working[15]);
            (working[1], working[6], working[11], working[12]) = 
                quarterRound(working[1], working[6], working[11], working[12]);
            (working[2], working[7], working[8], working[13]) = 
                quarterRound(working[2], working[7], working[8], working[13]);
            (working[3], working[4], working[9], working[14]) = 
                quarterRound(working[3], working[4], working[9], working[14]);
        }

        // Add working state to input state
        bytes memory output = new bytes(64);
        for (uint i = 0; i < 16; i++) {
            uint32 word = working[i] + state.state[i];
            for (uint j = 0; j < 4; j++) {
                output[i * 4 + j] = bytes1(uint8(word >> (j * 8)));
            }
        }

        return output;
    }

    /**
     * @dev Poly1305 MAC implementation
     */
    function poly1305Mac(
        bytes memory message,
        bytes32 key
    ) internal pure returns (bytes memory) {
        Poly1305State memory state;
        
        // Initialize r and s from key
        state.r = uint256(key) & 0x0ffffffc0ffffffc0ffffffc0fffffff;
        state.s = 0;

        // Process message blocks
        uint256 blockSize = 16;
        for (uint i = 0; i < message.length; i += blockSize) {
            uint256 blockLen = message.length - i;
            if (blockLen > blockSize) {
                blockLen = blockSize;
            }

            uint256 n = 0;
            for (uint j = 0; j < blockLen; j++) {
                n |= uint256(uint8(message[i + j])) << (j * 8);
            }

            if (blockLen < blockSize) {
                n |= uint256(1) << (blockLen * 8);
            }

            state.s = addmod(state.s, n, 1 << 130);
            state.s = mulmod(state.s, state.r, 1 << 130);
        }

        // Convert final state to bytes
        bytes memory mac = new bytes(16);
        uint256 finalState = state.s;
        for (uint i = 0; i < 16; i++) {
            mac[i] = bytes1(uint8(finalState >> (i * 8)));
        }
        
        return mac;
    }

    /**
     * @dev Encrypt data using ChaCha20-Poly1305
     */
    function encrypt(
        bytes memory plaintext,
        bytes32 key,
        bytes12 nonce
    ) external pure returns (bytes memory ciphertext, bytes memory tag) {
        // Generate keystream
        ChaChaState memory state = initializeState(key, nonce, 0);
        bytes memory keystream = chacha20Block(state);
        
        // Encrypt plaintext
        ciphertext = new bytes(plaintext.length);
        for (uint i = 0; i < plaintext.length; i++) {
            ciphertext[i] = bytes1(uint8(plaintext[i]) ^ uint8(keystream[i % 64]));
        }
        
        // Generate MAC
        tag = poly1305Mac(ciphertext, key);
        
        return (ciphertext, tag);
    }
}