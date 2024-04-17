#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const os = require('os');

const dataDir = path.join(__dirname, 'data');
const progressFile = 'progress.json';
const SAMPLE_SIZE = 1000;  // Reservoir sample size for vote data

let upvotesSample = [];
let downvotesSample = [];

const loadProgress = () => {
    if (fs.existsSync(progressFile)) {
        const progressData = fs.readFileSync(progressFile, 'utf8');
        return JSON.parse(progressData);
    }
    return { processedFiles: [], metrics: initializeMetrics() };
};

const saveProgress = (progress) => {
    fs.writeFileSync(progressFile, JSON.stringify(progress, null, 2));
};

const extractDataFromCSV = csvData => {
    let records = [], fields = [], currentField = '', inQuotes = false, i = 0;
    csvData = csvData.replace(/\r\n?/g, '\n');

    while (i < csvData.length) {
        const char = csvData[i], nextChar = csvData[i + 1];
        if (char === '"') {
            if (!inQuotes && (i === 0 || ",\n".includes(csvData[i - 1]))) inQuotes = true;
            else if (inQuotes && nextChar === '"') currentField += char, i++;
            else inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            fields.push(currentField);
            currentField = '';
        } else if (char === '\n' && !inQuotes) {
            fields.push(currentField);
            records.push(fields);
            fields = [];
            currentField = '';
        } else currentField += char;
        i++;
    }

    if (currentField || fields.length) {
        fields.push(currentField);
        records.push(fields);
    }

    return { headers: records[0], data: records.slice(1) };
};

const initializeMetrics = () => ({
    totalPosts: 0,
    totalComments: 0,
    postLengthStats: { count: 0, mean: 0, M2: 0 },
    commentLengthStats: { count: 0, mean: 0, M2: 0 },
    startDate: Infinity,
    endDate: -Infinity
});

const welfordUpdate = (stats, newValue) => {
    const delta = newValue - stats.mean;
    stats.mean += delta / ++stats.count;
    const delta2 = newValue - stats.mean;
    stats.M2 += delta * delta2;
};

const updateDateRange = (metrics, dateString) => {
    if (!dateString) return;
    const dateValue = new Date(dateString).getTime();
    if (dateValue < metrics.startDate) metrics.startDate = dateValue;
    if (dateValue > metrics.endDate) metrics.endDate = dateValue;
};

const reservoirSample = (sampleArray, newValue) => {
    const currentSize = sampleArray.length;
    if (currentSize < SAMPLE_SIZE) {
        sampleArray.push(newValue);
    } else {
        const replaceIndex = Math.floor(Math.random() * (currentSize + 1));
        if (replaceIndex < SAMPLE_SIZE) {
            sampleArray[replaceIndex] = newValue;
        }
    }
};

const processFile = (file, metrics) => {
    const csvData = fs.readFileSync(file, 'utf8');
    const { headers, data } = extractDataFromCSV(csvData);

    data.forEach(row => {
        const dateIndex = headers.indexOf('date');
        const bodyIndex = headers.indexOf('body');
        const directionIndex = headers.indexOf('direction');
        const date = row[dateIndex];
        const bodyLength = bodyIndex !== -1 && row[bodyIndex] ? row[bodyIndex].trim().length : 0;

        if (date) updateDateRange(metrics, date);

        if (path.basename(file) === 'posts.csv' || path.basename(file) === 'comments.csv') {
            if (path.basename(file) === 'posts.csv') {
                metrics.totalPosts++;
                welfordUpdate(metrics.postLengthStats, bodyLength);
            } else {
                metrics.totalComments++;
                welfordUpdate(metrics.commentLengthStats, bodyLength);
            }
        } else if (file.includes('post_votes.csv') || file.includes('comment_votes.csv')) {
            const voteDirection = row[directionIndex];
            const voteCount = voteDirection === 'up' ? 1 : (voteDirection === 'down' ? -1 : 0);
            if (voteDirection === 'up') {
                reservoirSample(upvotesSample, voteCount);
            } else if (voteDirection === 'down') {
                reservoirSample(downvotesSample, voteCount);
            }
        }
    });
};

const generateAnalytics = (metrics) => {
    upvotesSample.sort((a, b) => a - b);
    downvotesSample.sort((a, b) => a - b);
    const medianUpvotes = upvotesSample[Math.floor(upvotesSample.length / 2)];
    const medianDownvotes = downvotesSample[Math.floor(downvotesSample.length / 2)];

    return {
        totalPosts: metrics.totalPosts,
        totalComments: metrics.totalComments,
        postLengthMean: metrics.postLengthStats.mean,
        postLengthStdDev: Math.sqrt(metrics.postLengthStats.M2 / metrics.postLengthStats.count),
        commentLengthMean: metrics.commentLengthStats.mean,
        commentLengthStdDev: Math.sqrt(metrics.commentLengthStats.M2 / metrics.commentLengthStats.count),
        startDate: new Date(metrics.startDate).toISOString(),
        endDate: new Date(metrics.endDate).toISOString(),
        medianUpvotesPerPostOrComment: medianUpvotes,
        medianDownvotesPerPostOrComment: medianDownvotes
    };
};

const processZipFiles = () => {
    const progress = loadProgress();
    const zipFiles = fs.readdirSync(dataDir).filter(file => file.endsWith('.zip'));

    zipFiles.forEach(zipFile => {
        if (!progress.processedFiles.includes(zipFile)) {
            const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'reddit-data-'));
            execSync(`unzip -q "${path.join(dataDir, zipFile)}" -d "${tempDir}"`);
            const files = fs.readdirSync(tempDir);

            files.forEach(file => {
                const filePath = path.join(tempDir, file);
                processFile(filePath, progress.metrics);
            });

            // Clean up the temporary directory after processing
            fs.rmdirSync(tempDir, { recursive: true });
            progress.processedFiles.push(zipFile);
            saveProgress(progress);
        }
    });

    return progress.metrics;
};

const main = () => {
    const metrics = processZipFiles();
    const analytics = generateAnalytics(metrics);
    console.log(JSON.stringify(analytics, null, 2));
    fs.writeFileSync('analytics.json', JSON.stringify(analytics, null, 2));
};

main();
