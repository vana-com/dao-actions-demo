#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

try {
    // Read the data from a file
    const inputPath = path.join(__dirname, 'data.json');
    const data = JSON.parse(fs.readFileSync(inputPath, 'utf8').trim());

    console.log(`Found ${data?.users?.length} user records.`)

    const analytics = {
        totalRecords: data.users.length
    }

    // Write analytics.txt
    const outputPath = path.join(__dirname, 'analytics.json');
    const outputContent = JSON.stringify(analytics, null, 2);

    fs.writeFileSync(outputPath, outputContent);

    console.log(`Saved analytics.json`)
} catch (e) {
    console.error('Failed to read data.json', e)
    process.exit(1)
}
