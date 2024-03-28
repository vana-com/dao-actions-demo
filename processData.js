#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

// Read the input string from a file
const inputPath = path.join(__dirname, 'encoded.txt');
const encodedData = fs.readFileSync(inputPath, 'utf8').trim();

const decodedData = Buffer.from(encodedData, 'base64').toString('ascii');

console.log(`Encoded: ${encodedData}`);
console.log(`Decoded: ${decodedData}`);

// Optional: Write the results to a file
const outputPath = path.join(__dirname, 'decoded.txt');
const outputContent = `Encoded: ${encodedData}\nDecoded: ${decodedData}`;
fs.writeFileSync(outputPath, outputContent);
