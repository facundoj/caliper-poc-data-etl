var fs      = require('fs'),
    path    = require('path'),
    rimraf  = require('rimraf');

// identity_id,login_identity_id,school_id,assessment_id,assessment_version,
// 1,          2,                3,        4,            5,
// attempt_id,assmtitem_id,assmtitem_version,assessment_type_id,response_type,question_time,
// 6,         7,           8,                9,                 10,           11,
// score_posible,score_earned,masterobjectives,masterobjectivesid,objectivenumber
// 12,           13,          14,              15,                16
var ASSESSMENT_ITEM_PATTERN = /^([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),(.*),([0-9\.]*),([0-9\.]*)/;
var assessments = null,
    itemsGroupedByAttempt = new Map(),
    targetReady = false;

if (process.argv.length < 3) {
    console.log('Usage: node etl.js <assessment-items-csv>');
    return;
}

// Reading Assessement Items
fs.readFile(path.normalize(process.argv[2]), 'utf-8', function(err, data) {
    if (err) {
        console.log('Failed to read file.');
        return;
    }

    console.log('[ITEMS] Starting process.. (' + new Date() + ')');
    data.trim()
        .split('\n')
        .filter(function(line, i) { return i > 0; }) // remove column names
        .forEach(formatAssessmentItemsElements);

    console.log('[ITEMS] Done! (' + new Date() + ')');

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
    if (!(itemsGroupedByAttempt.size > 0 && targetReady)) {
        console.log("no");
        return;
    }

    console.log('[ATTEMPTS] Starting attempts..(' + new Date() + ')');

    var i = 0;
    itemsGroupedByAttempt.forEach(function (value, attempt) {
        i++;
        formatAssessmentElementsAndPrint(value, i);
    });

    console.log('[ATTEMPTS] Done! (' + new Date() + ')');
}

function formatAssessmentItemsElements(line) {
    // identity_id,login_identity_id,school_id,assessment_id,assessment_version,
    // 1,          2,                3,        4,            5,
    // attempt_id,assmtitem_id,assmtitem_version,assessment_type_id,response_type,question_time,
    // 6,         7,           8,                9,                 10,           11,
    // score_posible,score_earned,masterobjectives,masterobjectivesid,objectivenumber
    // 12,           13,          14,              15,                16
    // Pulling values from assessment item row
    var res = line.trim().match(ASSESSMENT_ITEM_PATTERN);
    if (!res) return null;

    // Supports n Learning Objects splitted by ";"
    var learningObjectives = [];
    res[15].trim().split(';').forEach(function(id) {
        learningObjectives.push({
            id: id
        });
    });

    var outcomeEvent = createEvent('OUTCOME_EVENT', {
        action: 'GRADED',
        actor: {
            id: res[1]
        },
        target: {
            id: res[7],
            learningObjectives
        },
        object: {
            id: res[6],
            count: 1
        },
        generated: {
            normalScore: res[13],
            totalScore: res[12]
        },
        assessment: {
            id: res[4],
            version: res[5]
        }
    });

    var attemptKey = res[6];
    if (!itemsGroupedByAttempt.has(attemptKey)) {
        itemsGroupedByAttempt.set(attemptKey, []);
    }

    itemsGroupedByAttempt.get(attemptKey).push(outcomeEvent);
}

function formatAssessmentElementsAndPrint(line, i) {
    var filename = path.normalize('./target/events-' + i + '-' + Date.now() + '.json');

    fs.writeFile(filename, JSON.stringify(line), function(err) {
        if (err) {
            console.log('Couldn\'t create ' + filename);
        } else {
            console.log('Created ' + filename);
        }
    });
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
