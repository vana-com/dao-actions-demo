#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const os = require('os');

const dataDir = path.join(__dirname, 'data');

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

const calculateStatistics = (data) => {
    if (data.length === 0) {
        return {
            median: 0,
            mean: 0,
            stdDev: 0,
            coefficientOfVariation: 0
        };
    }

    const sortedData = data.sort((a, b) => a - b);
    const n = data.length;
    const median = n % 2 === 0 ? (sortedData[n / 2 - 1] + sortedData[n / 2]) / 2 : sortedData[(n - 1) / 2];
    const mean = data.reduce((a, b) => a + b, 0) / n;
    const stdDev = Math.sqrt(data.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / n);
    const coefficientOfVariation = mean !== 0 ? stdDev / mean : 0;

    return {
        median: parseFloat(median.toFixed(2)),
        mean: parseFloat(mean.toFixed(2)),
        stdDev: parseFloat(stdDev.toFixed(2)),
        coefficientOfVariation: parseFloat(coefficientOfVariation.toFixed(2))
    };
};

const processZipFile = (zipFile, metrics) => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'reddit-data-'));
    execSync(`unzip -q "${zipFile}" -d "${tempDir}"`);

    const files = fs.readdirSync(tempDir);
    metrics.totalUsers++;

    files.forEach(file => {
        const csvData = fs.readFileSync(path.join(tempDir, file), 'utf8');
        let { headers, data } = extractDataFromCSV(csvData);

        if (file === 'posts.csv') {
            metrics.totalPosts += data.length;
            metrics.postsWithNonEmptyText += data.filter(row => row[headers.indexOf('body')].trim() !== '').length;
            metrics.postDates = [...metrics.postDates, ...data.map(row => new Date(row[headers.indexOf('date')]))];
            metrics.postLengths = [...metrics.postLengths, ...data.map(row => row[headers.indexOf('body')].trim().length)];
        } else if (file === 'comments.csv') {
            metrics.totalComments += data.length;
            metrics.commentsWithNonEmptyText += data.filter(row => row[headers.indexOf('body')].trim() !== '').length;
            metrics.commentDates = [...metrics.commentDates, ...data.map(row => new Date(row[headers.indexOf('date')]))];
            metrics.commentLengths = [...metrics.commentLengths, ...data.map(row => row[headers.indexOf('body')].trim().length)];
        } else if (file === 'post_votes.csv') {
            metrics.postVotes += data.length;
        } else if (file === 'comment_votes.csv') {
            metrics.commentVotes += data.length;
        } else if (file === 'subscribed_subreddits.csv') {
            metrics.subredditCounts.push(data.length);
        }
    });

    fs.rmdirSync(tempDir, { recursive: true });
};

const analyzeTemporalData = (dates) => {
    const activityByDay = {};
    const activityByMonth = {};
    const activityByYear = {};

    dates.forEach(date => {
        const day = date.toISOString().split('T')[0];
        const month = date.toISOString().slice(0, 7);
        const year = date.getFullYear();
        activityByDay[day] = (activityByDay[day] || 0) + 1;
        activityByMonth[month] = (activityByMonth[month] || 0) + 1;
        activityByYear[year] = (activityByYear[year] || 0) + 1;
    });

    const dailyActivity = Object.values(activityByDay);
    const monthlyActivity = Object.values(activityByMonth);

    return {
        dailyCoefficientOfVariation: calculateStatistics(dailyActivity).coefficientOfVariation,
        monthlyCoefficientOfVariation: calculateStatistics(monthlyActivity).coefficientOfVariation,
        yearlyActivity: activityByYear
    };
};

const generateAnalytics = () => {
    const zipFiles = fs.readdirSync(dataDir).filter(file => file.endsWith('.zip'));
    const metrics = {
        totalUsers: 0,
        totalPosts: 0,
        totalComments: 0,
        postsWithNonEmptyText: 0,
        commentsWithNonEmptyText: 0,
        subredditCounts: [],
        postDates: [],
        commentDates: [],
        postLengths: [],
        commentLengths: [],
        postVotes: 0,
        commentVotes: 0,
    };

    zipFiles.forEach(file => processZipFile(path.join(dataDir, file), metrics));

    const postActivity = analyzeTemporalData(metrics.postDates);
    const commentActivity = analyzeTemporalData(metrics.commentDates);

    const yearlyActivity = {};
    Object.entries(postActivity.yearlyActivity).forEach(([year, count]) => {
        yearlyActivity[year] = (yearlyActivity[year] || 0) + count;
    });
    Object.entries(commentActivity.yearlyActivity).forEach(([year, count]) => {
        yearlyActivity[year] = (yearlyActivity[year] || 0) + count;
    });

    const totalActivity = Object.values(yearlyActivity).reduce((a, b) => a + b, 0);
    const activityDistribution = {};
    Object.entries(yearlyActivity).forEach(([year, count]) => {
        activityDistribution[year] = parseFloat((count / totalActivity).toFixed(4));
    });

    return {
        totalUsers: metrics.totalUsers,
        totalPosts: metrics.totalPosts,
        totalComments: metrics.totalComments,
        totalVotes: metrics.postVotes + metrics.commentVotes,
        startDate: metrics.postDates.length > 0 ? metrics.postDates.sort((a, b) => a - b)[0].toISOString().split('T')[0] : null,
        endDate: metrics.commentDates.length > 0 ? metrics.commentDates.sort((a, b) => b - a)[0].toISOString().split('T')[0] : null,
        medianPostsPerUser: calculateStatistics(metrics.postLengths).median,
        medianCommentsPerUser: calculateStatistics(metrics.commentLengths).median,
        medianVotesPerUser: calculateStatistics([...metrics.postLengths, ...metrics.commentLengths]).median,
        uniqueSubreddits: new Set(metrics.subredditCounts).size,
        medianSubredditsPerUser: calculateStatistics(metrics.subredditCounts).median,
        subredditCoefficientOfVariation: calculateStatistics(metrics.subredditCounts).coefficientOfVariation,
        medianVotesPerPost: calculateStatistics(metrics.postLengths).median,
        medianVotesPerComment: calculateStatistics(metrics.commentLengths).median,
        dailyPostCoefficientOfVariation: postActivity.dailyCoefficientOfVariation,
        monthlyPostCoefficientOfVariation: postActivity.monthlyCoefficientOfVariation,
        dailyCommentCoefficientOfVariation: commentActivity.dailyCoefficientOfVariation,
        monthlyCommentCoefficientOfVariation: commentActivity.monthlyCoefficientOfVariation,
        activityDistributionByYear: activityDistribution,
        postsWithNonEmptyTextPercentage: parseFloat(((metrics.postsWithNonEmptyText / metrics.totalPosts) * 100).toFixed(2)),
        commentsWithNonEmptyTextPercentage: parseFloat(((metrics.commentsWithNonEmptyText / metrics.totalComments) * 100).toFixed(2)),
        medianPostLength: calculateStatistics(metrics.postLengths).median,
        medianCommentLength: calculateStatistics(metrics.commentLengths).median
    };
};

const main = () => {
    const analytics = generateAnalytics();
    console.log(JSON.stringify(analytics, null, 2));
    fs.writeFileSync('analytics.json', JSON.stringify(analytics, null, 2));
};

main();
