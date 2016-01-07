var fs      = require('fs'),
    path    = require('path'),
    rimraf  = require('rimraf');

// ID,NAME,VERSION,ATTEMPT_ID,ATTEMPT_COUNT,RESULT_ID,SCORE,TOTAL
var ASSESSMENT_PATTERN = /^(.+),(.+),(.+),(.+),(.+),(.+),(.+),(.+)/;
// ID,RESPONSE_ID,RESPONSE_TYPE,RESPONSE_VALUE,ASSESSMENT_ID,ATTEMPT_ID
var ASSESSMENT_ITEM_PATTERN = /^(.+),(.+),(.+),(.+),(.+),(.+)/;

var assessments = null,
    items = null,

    targetReady = false;

if (process.argv.length < 4) {
    console.log('Usage: node etl.js <assessments-csv> <assessment-items-csv>');
    return;
}
// Reading Assessments
fs.readFile(path.normalize(process.argv[2]), 'utf-8', function(err, data) {
    if (err) {
        console.log('Failed to read file.');
        return;
    }

    assessments = data;
    start();
});

// Reading Assessement Items
fs.readFile(path.normalize(process.argv[3]), 'utf-8', function(err, data) {
    if (err) {
        console.log('Failed to read file.');
        return;
    }
    items = data.trim()
        .split('\n')
        .filter(function(line, i) { return i > 0; }) // remove column names
        .map(formatAssessmentItemsElements);

    start();
});

// Cleaning target
rimraf('target', function(err) {
    fs.mkdir('target', function(err) {
        targetReady = true;
        start();
    });
});

// Start ETL
function start() {
    if (!(assessments && items && targetReady)) {
        return;
    }

    var outputFilesData = assessments.trim()
        .split('\n')
        .filter(function(line, i) { return i > 0; }) // remove column names
        .map(formatAssessmentElements) // [ [{events},..], [{events},..], ... ]

    // Writting files
    outputFilesData.forEach(function(events, i) {
        var filename = './target/events-' + i + '-' + Date.now() + '.json';
        debugger;
        fs.writeFile(filename, JSON.stringify(events), function(err) {
            if (err) {
                console.log('Couldn\'t create ' + filename);
            } else {
                console.log('Created ' + filename);
            }
        });
    });
}

function formatAssessmentItemsElements(line) {
    // Pulling values from assessment item row
    var res = line.trim().match(ASSESSMENT_ITEM_PATTERN);
    if (!res) return null;

    // Details JSON structure for AssessmentItemEvent
    return createEvent('ASSESSMENT_ITEM_EVENT', {
        action: 'COMPLETED',
        object: {
            id: res[1]
        },
        generated: {
            id: res[2],
            type: res[3],
            value: res[4]
        },
        isPartOf: {
            // attempt id
            id: res[6]
        }
    });
}

function formatAssessmentElements(line) {
    // Pulling values from assessment row
    var res = line.trim().match(ASSESSMENT_PATTERN);
    if (!res) return null;

    var eventsChain = [];

    eventsChain.push(createEvent('ASSESSMENT_EVENT', {
        action: 'STARTED',
        object: {
            id: res[1],
            name: res[2],
            version: res[3]
        },
        generated: {
            id: res[4],
            count: res[5]
        }
    }));

    items
        .filter(function(assessmentItemEvent) { // Left joining by attempt id
            return assessmentItemEvent.values.isPartOf.id == res[4];
        }).forEach(function(assessmentItemEvent) { // Adding to the chain
            eventsChain.push(assessmentItemEvent);
        });

    eventsChain.push(createEvent('ASSESSMENT_EVENT', {
        action: 'SUBMITTED',
        object: {
            id: res[1],
            name: res[2],
            version: res[3]
        },
        generated: {
            id: res[4],
            count: res[5]
        }
    }));

    eventsChain.push(createEvent('OUTCOME_EVENT', {
        action: 'GRADED',
        target: {
            id: res[1]
        },
        object: {
            id: res[4]
        },
        generated: {
            id: res[6],
            normalScore: res[7],
            totalScore: res[8]
        }
    }));

    return eventsChain;
}

function createEvent(type, details) {
    var typeMap = {
        ASSESSMENT_EVENT: 'AssessmentEvent',
        ASSESSMENT_ITEM_EVENT: 'AssessmentItemEvent',
        OUTCOME_EVENT: 'OutcomeEvent'
    };

    if (typeMap[type]) {
        return {
            type: typeMap[type],
            values: details
        };
    }

}