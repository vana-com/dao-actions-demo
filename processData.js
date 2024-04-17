#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const os = require('os');

const dataDir = path.join(__dirname, 'data');
const progressFile = 'progress.json';
const RESERVOIR_SAMPLE_SIZE = 1000;

const activitySamples = [];

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
    if (currentSize < RESERVOIR_SAMPLE_SIZE) {
        sampleArray.push(newValue);
    } else {
        const replaceIndex = Math.floor(Math.random() * (currentSize + 1));
        if (replaceIndex < RESERVOIR_SAMPLE_SIZE) {
            sampleArray[replaceIndex] = newValue;
        }
    }
};

const sampleUserEngagement = (totalActivities, activeDays) => {
    if (activeDays.size > 0) {
        const averageActivities = totalActivities / activeDays.size;
        reservoirSample(activitySamples, averageActivities); // Sample this user's average activities per active day
    }
};

const processFile = (file, metrics) => {
    const csvData = fs.readFileSync(file, 'utf8');
    const { headers, data } = extractDataFromCSV(csvData);

    let totalActivities = 0;
    let activeDays = new Set();

    data.forEach(row => {
        const dateIndex = headers.indexOf('date');
        const bodyIndex = headers.indexOf('body');
        const directionIndex = headers.indexOf('direction');
        const date = row[dateIndex];
        const bodyLength = bodyIndex !== -1 && row[bodyIndex] ? row[bodyIndex].trim().length : 0;

        if (date) {
            activeDays.add(date);
            updateDateRange(metrics, date);
        }

        if (path.basename(file) === 'posts.csv' || path.basename(file) === 'comments.csv') {
            if (path.basename(file) === 'posts.csv') {
                metrics.totalPosts++;
                welfordUpdate(metrics.postLengthStats, bodyLength);
            } else {
                metrics.totalComments++;
                welfordUpdate(metrics.commentLengthStats, bodyLength);
            }
        }

        if (path.basename(file) === 'posts.csv' || path.basename(file) === 'comments.csv' || file.includes('post_votes.csv') || file.includes('comment_votes.csv')) {
            totalActivities++;
        }

        if (file.includes('posts.csv') || file.includes('comments.csv') || file.includes('votes.csv')) {
            sampleUserEngagement(totalActivities, activeDays);
        }
    });
};

const generateAnalytics = (metrics) => {
    activitySamples.sort((a, b) => a - b);
    const medianActivityPerDay = activitySamples[Math.floor(activitySamples.length / 2)];

    return {
        totalPosts: metrics.totalPosts,
        totalComments: metrics.totalComments,
        postLengthMean: metrics.postLengthStats.mean,
        postLengthStdDev: Math.sqrt(metrics.postLengthStats.M2 / metrics.postLengthStats.count),
        commentLengthMean: metrics.commentLengthStats.mean,
        commentLengthStdDev: Math.sqrt(metrics.commentLengthStats.M2 / metrics.commentLengthStats.count),
        startDate: new Date(metrics.startDate).toISOString(),
        endDate: new Date(metrics.endDate).toISOString(),
        medianActivityPerActiveDay: medianActivityPerDay
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
